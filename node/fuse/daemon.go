// +build darwin freebsd netbsd openbsd linux

/*
 * passthrough.go
 *
 * Copyright 2017-2018 Bill Zissimopoulos
 */
/*
 * This file is part of Cgofuse.
 *
 * It is licensed under the MIT license. The full license text can be found
 * in the License.txt file at the root of this project.
 */

package fuse

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"log"
	"sync"
	"math/rand"
	"strings"
//	"io/ioutil"

	"firestore/core/file"
	"github.com/billziss-gh/cgofuse/examples/shared"
	"github.com/billziss-gh/cgofuse/fuse"
	"github.com/pkg/xattr"
	"github.com/spf13/viper"
	"golang.org/x/sys/unix"
)

// Directory name constants
const NODE_DIR = "storage_nodes"
const DATA_USER_DIR = "data users"
const METADATA_DIR = "metadata"
const PROTOCOL_DIR = "protcol"
const SUBJECTS_DIR = "subjects"
const STUDIES_DIR = "studies"

// A magical prefix used by xattr
const XATTR_PREFIX = "user."

// A magical prefix used


func trace(vals ...interface{}) func(vals ...interface{}) {
	uid, gid, _ := fuse.Getcontext()
	return shared.Trace(1, fmt.Sprintf("[uid=%v,gid=%v]", uid, gid), vals...)
}

func errno(err error) int {
	if nil != err {
		return -int(err.(syscall.Errno))
	} else {
		return 0
	}
}

var (
	_host *fuse.FileSystemHost
)

type Ptfs struct {
	fuse.FileSystemBase
	root string
	nodePermission string
	subjectPermission string
	startDir string
	mountDir string
	nodeID string
	xattrMux sync.Mutex
	xattrFlag bool

	subjectPermissions map[string]string // subjectID -> permission
}

func NewFuseFS(nodeID string) *Ptfs {
	startDir := creataLocalMountPointPath(nodeID)
	mountDir := GetDestinationMountPoint(nodeID)
	createDirectory(startDir)
	createDirectory(mountDir)

	syscall.Umask(0)
	ptfs := Ptfs{
		startDir: startDir,
		mountDir: mountDir,
		nodeID: nodeID,
		xattrFlag: false,
	}

	// Initialze the map of subjects that have a specific permission to their files
	subjectPermissions := make(map[string]string, 0)
	ptfs.subjectPermissions = subjectPermissions

	// Create the file tree the node is to maintain
	ptfs.createStudyDirectoryTree()

	// Set the initial state of the system to have a many-to-many
	// relation between subjects and nodes
	if viper.GetBool("set_files") {
		ptfs.createSubjectDirectoryTree()
	}

	// Allow all subjects' files to be accessed
	for idx, _ := range ptfs.subjectPermissions {
		ptfs.subjectPermissions[idx] = file.FILE_ALLOWED
	}

	// Customized parameters. Need to purify them as well. Magic...
	opts := []string{"", "-o", "sync", startDir, mountDir}
	ptfs.root, _ = filepath.Abs(opts[len(opts) - 2])
	opts = append(opts[:len(opts) - 2], opts[len(opts) - 1])

	// A warm start should verify the files and their attributes by asking the 
	// rest of the network about the state of the permissions somewhere around here...
	// restore_permission_state()
	_host = fuse.NewFileSystemHost(&ptfs)
	go func() {
		log.Printf("Mounting dir on %s\n", mountDir)
		ok := _host.Mount("", opts[1:])
		if ok != true {
			panic(errors.New("Could not mount Fuse system"))
		}
	}()
	return &ptfs
}

func (fs *Ptfs) Root() string {
	return fs.root
}

func (self *Ptfs) Init() {
	defer trace()()
	e := syscall.Chdir(self.root)
	if e == nil {
		fmt.Printf("Chaning dir\n")
		self.root = "./"
	}
}

// TODO: finish it l8r
func (self *Ptfs) Listxattr(path string, fill func(name string) bool) (errc int) {
	fmt.Printf("Listxattr() -> path: %v\nname: %v\n", path, fill)

	defer trace(path, fill)(&errc)
	buf := make([]byte, 100)
	errc, err := syscall.Listxattr(path, buf)
	if err != nil {
		panic(err)
	}
	return errc
}

// TODO: figure out why errc equals 7!
func (self *Ptfs) Getxattr(path string, attr string) (errc int, res []byte) {	
	defer trace(path, attr)(&errc, &res)
	res = make([]byte, 100)
	path = filepath.Join(self.root, path)
	errc, _ = syscall.Getxattr(path, attr, res)
	

	fmt.Printf("Getxattr() -> path: %v\nattr: %v\nres: %s\nerrc: %v\n", path, attr, res, errc)

	// return errc!!!
	return 0, res
}

func (self *Ptfs) Setxattr(path string, attr string, data []byte, flags int) (errc int) {
	if !self.canSetXattr() {
		fmt.Printf("Setxattr\n")
		return int(unix.EPERM)
	}

	defer trace(path, attr, data, flags)(&errc)
	path = filepath.Join(self.root, path)
	errc = errno(syscall.Setxattr(path, attr, data, flags))

	fmt.Printf("Setxattr() -> path: %v\nattr: %v\ndata: %s\nflags: %v\nerrcode: %v\n", path, attr, data, flags, errc)
	return
}

func (self *Ptfs) Removexattr(path string, attr string) (errc int) {
	if !self.canSetXattr() {
		fmt.Printf("Removexattr = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}

	defer trace(path, attr)(&errc)
	path = filepath.Join(self.root, path)
	errc = errno(syscall.Removexattr(path, attr))
	return errc
}

func (self *Ptfs) Statfs(path string, stat *fuse.Statfs_t) (errc int) {
	defer trace(path)(&errc, stat)
	if !self.isAllowedAccess(path) {
		fmt.Printf("Statfs = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	path = filepath.Join(self.root, path)
	stgo := syscall.Statfs_t{}
	errc = errno(syscall_Statfs(path, &stgo))
	copyFusestatfsFromGostatfs(stat, &stgo)
	return
}

func (self *Ptfs) Mknod(path string, mode uint32, dev uint64) (errc int) {
	defer trace(path, mode, dev)(&errc)
	fmt.Printf("Mknod()...\n") 
	if !self.isAllowedAccess(path) {
		fmt.Printf("Mknod = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	defer setuidgid()()
	path = filepath.Join(self.root, path)
	return errno(syscall.Mknod(path, mode, int(dev)))
}

func (self *Ptfs) Mkdir(path string, mode uint32) (errc int) {
	defer trace(path, mode)(&errc)
	fmt.Printf("Mkdir()...\n")
	if !self.isAllowedAccess(path) {
		fmt.Printf("Mkdir = %d\n", int(unix.EPERM))
		return -int(unix.EPERM)
	}
	defer setuidgid()()
	path = filepath.Join(self.root, path)
	return errno(syscall.Mkdir(path, mode))
}

func (self *Ptfs) Unlink(path string) (errc int) {
	defer trace(path)(&errc)
	fmt.Println("Unlink")
	if !self.isAllowedAccess(path) {
		fmt.Printf("Unlink = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	path = filepath.Join(self.root, path)
	return errno(syscall.Unlink(path))
}

func (self *Ptfs) Rmdir(path string) (errc int) {
	defer trace(path)(&errc)
	fmt.Println("Rmdir")
	if !self.isAllowedAccess(path) {
		fmt.Printf("Rmdir = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	path = filepath.Join(self.root, path)
	return errno(syscall.Rmdir(path))
}

func (self *Ptfs) Link(oldpath string, newpath string) (errc int) {
	defer trace(oldpath, newpath)(&errc)
	fmt.Println("Link")
	if !self.isAllowedAccess(oldpath) {
		fmt.Printf("Link = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	defer setuidgid()()
	oldpath = filepath.Join(self.root, oldpath)
	newpath = filepath.Join(self.root, newpath)
	return errno(syscall.Link(oldpath, newpath))
}

func (self *Ptfs) Symlink(target string, newpath string) (errc int) {
	defer trace(target, newpath)(&errc)
	fmt.Println("Symlink")
	if !self.isAllowedAccess(target) {
		fmt.Printf("Symlink = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	defer setuidgid()()
	newpath = filepath.Join(self.root, newpath)
	return errno(syscall.Symlink(target, newpath))
}

func (self *Ptfs) Readlink(path string) (errc int, target string) {
	defer trace(path)(&errc, &target)
	fmt.Println("Readlink")
	if !self.isAllowedAccess(path) {
		fmt.Printf("Readlink = %d\n", int(unix.EPERM))
		return int(unix.EPERM), ""
	}
	path = filepath.Join(self.root, path)
	buff := [1024]byte{}
	n, e := syscall.Readlink(path, buff[:])
	if nil != e {
		return errno(e), ""
	}
	return 0, string(buff[:n])
}

func (self *Ptfs) Rename(oldpath string, newpath string) (errc int) {
	defer trace(oldpath, newpath)(&errc)
	fmt.Println("Rename")
	if !self.isAllowedAccess(oldpath) {
		fmt.Printf("Rename = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	defer setuidgid()()
	oldpath = filepath.Join(self.root, oldpath)
	newpath = filepath.Join(self.root, newpath)
	return errno(syscall.Rename(oldpath, newpath))
}

func (self *Ptfs) Chmod(path string, mode uint32) (errc int) {
	defer trace(path, mode)(&errc)
	fmt.Println("Chmod")
	if !self.isAllowedAccess(path) {
		fmt.Printf("Chmod = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	path = filepath.Join(self.root, path)
	return errno(syscall.Chmod(path, mode))
}

func (self *Ptfs) Chown(path string, uid uint32, gid uint32) (errc int) {
	defer trace(path, uid, gid)(&errc)
	fmt.Println("Chown")
	if !self.isAllowedAccess(path) {
		fmt.Printf("Chown = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	path = filepath.Join(self.root, path)
	return errno(syscall.Lchown(path, int(uid), int(gid)))
}

func (self *Ptfs) Utimens(path string, tmsp1 []fuse.Timespec) (errc int) {
	defer trace(path, tmsp1)(&errc)
	fmt.Println("Utimens")
	if !self.isAllowedAccess(path) {
		fmt.Printf("Utimens = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	path = filepath.Join(self.root, path)
	tmsp := [2]syscall.Timespec{}
	tmsp[0].Sec, tmsp[0].Nsec = tmsp1[0].Sec, tmsp1[0].Nsec
	tmsp[1].Sec, tmsp[1].Nsec = tmsp1[1].Sec, tmsp1[1].Nsec
	return errno(syscall.UtimesNano(path, tmsp[:]))
}

func (self *Ptfs) Create(path string, flags int, mode uint32) (errc int, fh uint64) {
	defer trace(path, flags, mode)(&errc, &fh)
	defer setuidgid()()
	fmt.Println("Create")
	if !self.isAllowedAccess(path) {
		fmt.Printf("Create = %d\n", int(unix.EPERM))
		return int(unix.EPERM), 0
	}
	return self.open(path, flags, mode)
}

func (self *Ptfs) Open(path string, flags int) (errc int, fh uint64) {
	defer trace(path, flags)(&errc, &fh)
	fmt.Println("Open")
	if !self.isAllowedAccess(path) {
		fmt.Printf("Open = %d\n", int(unix.EPERM))
		return int(unix.EPERM), 0
	}
	return self.open(path, flags, 0)
}

func (self *Ptfs) open(path string, flags int, mode uint32) (errc int, fh uint64) {
	fmt.Println("open")
	if !self.isAllowedAccess(path) {
		fmt.Printf("open = %d\n", int(unix.EPERM))
		return int(unix.EPERM), ^uint64(0)
	}
	path = filepath.Join(self.root, path)
	f, e := syscall.Open(path, flags, mode)
	if nil != e {
		return errno(e), ^uint64(0)
	}
	return 0, uint64(f)
}

func (self *Ptfs) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	defer trace(path, fh)(&errc, stat)
	fmt.Println("Getattr")
	if !self.isAllowedAccess(path) {
		fmt.Printf("Getattr = %d\n", -int(unix.EPERM))
		return -int(unix.EPERM)
	}
	stgo := syscall.Stat_t{}
	if ^uint64(0) == fh {
		path = filepath.Join(self.root, path)
		errc = errno(syscall.Lstat(path, &stgo))
	} else {
		errc = errno(syscall.Fstat(int(fh), &stgo))
	}
	copyFusestatFromGostat(stat, &stgo)
	return
}

func (self *Ptfs) Truncate(path string, size int64, fh uint64) (errc int) {
	defer trace(path, size, fh)(&errc)
	fmt.Println("Truncate")
	if !self.isAllowedAccess(path) {
		fmt.Printf("Truncate = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	if ^uint64(0) == fh {
		path = filepath.Join(self.root, path)
		errc = errno(syscall.Truncate(path, size))
	} else {
		errc = errno(syscall.Ftruncate(int(fh), size))
	}
	return
}

func (self *Ptfs) Read(path string, buff []byte, ofst int64, fh uint64) (n int) {
	defer trace(path, buff, ofst, fh)(&n)
	fmt.Printf("Read\n")
	if !self.isAllowedAccess(path) {
		fmt.Printf("Read = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	n, e := syscall.Pread(int(fh), buff, ofst)
	if nil != e {
		return errno(e)
	}
	return n
}

func (self *Ptfs) Write(path string, buff []byte, ofst int64, fh uint64) (n int) {
	defer trace(path, buff, ofst, fh)(&n)
	fmt.Println("Write")
	if !self.isAllowedAccess(path) {
		fmt.Printf("Write = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	n, e := syscall.Pwrite(int(fh), buff, ofst)
	if nil != e {
		return errno(e)
	}
	return n
}

func (self *Ptfs) Release(path string, fh uint64) (errc int) {
	fmt.Println("Release")
	if !self.isAllowedAccess(path) {
		fmt.Printf("Release = %d\n", -int(unix.EPERM))
		return -int(unix.EPERM)
	}
	defer trace(path, fh)(&errc)
	return errno(syscall.Close(int(fh)))
}

func (self *Ptfs) Fsync(path string, datasync bool, fh uint64) (errc int) {
	defer trace(path, datasync, fh)(&errc)
	fmt.Println("Fsync")
	if !self.isAllowedAccess(path) {
		fmt.Printf("Fsync = %d\n", -int(unix.EPERM))
		return -int(unix.EPERM)
	}
	return errno(syscall.Fsync(int(fh)))
}

func (self *Ptfs) Opendir(path string) (errc int, fh uint64) {
	defer trace(path)(&errc, &fh)
	fmt.Println("Opendir")
	if !self.isAllowedAccess(path) {
		fmt.Printf("Opendir = %d\n", -int(unix.EPERM))
		return -int(unix.EPERM), ^uint64(0)
	}
	path = filepath.Join(self.root, path)
	f, e := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
	if nil != e {
		return errno(e), ^uint64(0)
	}
	return 0, uint64(f)
}

func (self *Ptfs) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, ofst int64,	fh uint64) (errc int) {
	defer trace(path, fill, ofst, fh)(&errc)
	fmt.Println("Readdir")
	if !self.isAllowedAccess(path) {
		fmt.Printf("Readdir = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	path = filepath.Join(self.root, path)
	file, e := os.Open(path)
	if nil != e {
		return errno(e)
	}
	defer file.Close()
	nams, e := file.Readdirnames(0)
	if nil != e {
		return errno(e)
	}
	nams = append([]string{".", ".."}, nams...)
	for _, name := range nams {
		if !fill(name, nil, 0) {
			break
		}
	}
	return 0
}

func (self *Ptfs) Releasedir(path string, fh uint64) (errc int) {
	defer trace(path, fh)(&errc)
	fmt.Println("Releasedir")
	if !self.isAllowedAccess(path) {
		fmt.Printf("Releasedir = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	return errno(syscall.Close(int(fh)))
}

func (self *Ptfs) Shutdown() {
	fmt.Printf("Shutdown() not implemented!\n")
	/*	if self.Unmount() != true {
			fmt.Errorf("Unmount failed\n")
		}
		self.Destroy()*/
}

// Returns the path for the local entry point for the FUSE file system
func creataLocalMountPointPath(nodeName string) string {
	return fmt.Sprintf("%s/%s/%s", viper.GetString("fuse_mount"), NODE_DIR, nodeName)
}

// Returns the location to which changes in the node's local storage directory
// are reflected
func GetDestinationMountPoint(nodeName string) string {
	return fmt.Sprintf("/tmp/%s/%s", NODE_DIR, nodeName)
}

// Creates a directory tree using dirPath
func createDirectory(dirPath string) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			panic(err)
		}
	}
}

// Sets the permission globally for the entire node to which this is attatched 
// NOTE: this is only temporarily; the entire system relies on a statically defined
// file tree
func (self *Ptfs) SetNodePermission(permission string) error {
	/*self.nodePermission = permission
	if _, err := os.Stat(self.mountDir); err != nil {
		if os.IsNotExist(err) {
			log.Printf("Directory %s does not exist!\n", self.mountDir)
		} else {
			log.Fatal("Some unknown error while checking if directory exists\n")
		}
	}
	*/
	// Set permission on current working directory
	/*self.xattrMux.Lock()
	self.xattrFlag = true
	fmt.Printf("self.mountDir: %s/\n", self.startDir)
	if err := xattr.Set(self.startDir, XATTR_PREFIX + "PERMISSION", []byte(permission)); err != nil {
		return err
	}
	self.xattrFlag = false
	self.xattrMux.Unlock()*/
	return nil
}

// Use only /storage_nodes/node_i/studies/study_i/subjects/subject_x
func (self *Ptfs) SetSubjectNodePermission(subject, permission string) {
	self.subjectPermissions[subject] = permission
	//self.subjectPermissions["subject_1"] = file.FILE_DISALLOWED
}

func (self *Ptfs) setXattrFlag(path, permission string) error {
	// might use lock here...
	/*self.xattrMux.Lock()
	self.xattrFlag = true
	fmt.Printf("Set %s on %s\n", permission, path)*/
	/*if err := xattr.Set(path, XATTR_PREFIX + "PERMISSION", []byte(permission)); err != nil {
		fmt.Println(err.Error())
		return err
	}*/
	/*self.xattrFlag = false
	self.xattrMux.Unlock()*/
	return nil
}

func (self *Ptfs) isAllowedAccess(path string) bool {
	self.xattrMux.Lock()
	defer self.xattrMux.Unlock()
	path = filepath.Join(self.mountDir, path)
	//fmt.Printf("Path to inspect: %s\n", path)
	//fmt.Printf("Address of lock: %p\n", &self.xattrMux)
	//fmt.Printf("Address of self: %p\n", &self)

	// First check if we access a higher-level part of the tree (subjects-side). Always allow
	if !strings.Contains(path, "subjects") {
		//fmt.Printf("Accessing higher levels in the tree\n")
		return true
	}

	// Check if we access a forbidden part of the subject-side of the tree
	for s, perm := range self.subjectPermissions {
		lowestDir := self.mountDir + "/" + SUBJECTS_DIR + "/" +  s
		//fmt.Printf("lowestDir: %s\n", lowestDir)
		ok, err := filepath.Match(lowestDir, path)
		if err != nil {
			panic(err)
		}

		if ok && perm == file.FILE_DISALLOWED {
		//	fmt.Printf("Match betweel %s and %s\n", lowestDir, path)
			return false
		}
	}

/*
	for s, perm := range self.subjectPermissions {
	//	fmt.Printf("Subject: %s\n", s)
		if strings.Contains(path, "subjects") && !strings.Contains(path, s) {
	//		fmt.Printf("Accessing directory with subjects in\n")
			return true 
		} else if strings.Contains(path, s) && perm == file.FILE_ALLOWED {
	//		fmt.Printf("Accessing a directory that might be closed off\n")
			return true
		} else if strings.Contains(path, s) && perm == file.FILE_DISALLOWED {
	//		fmt.Printf("Denied access to %s\n", path)
			return false
		}
	}

	if !strings.Contains(path, "subjects") {
	//	fmt.Printf("Accessing any other directory\n")
		return true
	}

	//fmt.Printf("Denying access to path\n")*/
	return true
}

func (self *Ptfs) canSetXattr() bool {
	return self.xattrFlag	
}

func (self *Ptfs) GetLocalMountPoint() string {
	return self.startDir
}

func (self *Ptfs) createStudyDirectoryTree() {
	for i := 1; i <= viper.GetInt("num_studies"); i++ {
		dirPath := fmt.Sprintf("%s/%s/study_%d", self.startDir, STUDIES_DIR, i)
		createDirectory(dirPath + "/" + DATA_USER_DIR)
		createDirectory(dirPath + "/" + METADATA_DIR)
		createDirectory(dirPath + "/" + PROTOCOL_DIR)
		createDirectory(dirPath + "/" + SUBJECTS_DIR)
		
		for j := 1; j <= viper.GetInt("num_subjects"); j++ {
			subjectDirPath := fmt.Sprintf("%s/%s/subject_%d", dirPath, SUBJECTS_DIR, j)
			createDirectory(subjectDirPath)
			
			// Set default file permission on study directory
			if err := xattr.Set(subjectDirPath, XATTR_PREFIX + "PERMISSION", []byte(file.FILE_ALLOWED)); err != nil {
				log.Fatal(err)
			}
		}
	}	
}

func (self *Ptfs) createSubjectDirectoryTree() {
	for i := 1; i <= viper.GetInt("num_subjects"); i++ {
		subjectDirPath := fmt.Sprintf("%s/%s/subject_%d", self.startDir, SUBJECTS_DIR, i)
		subjectID := fmt.Sprintf("subject_%d", i)
		createDirectory(subjectDirPath)
		self.subjectPermissions[subjectID] = file.FILE_ALLOWED

		for j := 1; j <= viper.GetInt("num_studies"); j++ {
			studyDirPath := fmt.Sprintf("%s/%s/study_%d", subjectDirPath, STUDIES_DIR, j)
			studyID := fmt.Sprintf("study_%d", j)
			createDirectory(studyDirPath)

			// Create the files assoicated with a subject and a study with random noise
			self.createStudyFiles(subjectID, studyID)
			self.linkStudyFiles(subjectID, studyID)
		}
	}
}

// Link files from 'subjects' directory to 'studies' directory
func (self *Ptfs) linkStudyFiles(subject, study string) {
	filePathDir := fmt.Sprintf("%s/%s/%s/%s/%s", self.startDir, SUBJECTS_DIR, subject, STUDIES_DIR, study)
	targetPathDir := fmt.Sprintf("%s/%s/%s/%s/%s", self.startDir, STUDIES_DIR, study, SUBJECTS_DIR, subject)

	for i := 1; i <= viper.GetInt("files_per_study"); i++ {
		filePath := fmt.Sprintf("%s/file_%d", filePathDir, i)
		targetPath := fmt.Sprintf("%s/file_%d", targetPathDir, i)
		fmt.Printf("s = %s\n", filePathDir)
		if err := os.Symlink(filePath, targetPath); err != nil {
			panic(err)
		}
	}
}

// Insert files into 'subjects' directory
func (self *Ptfs) createStudyFiles(subject, study string) {
	dirPath := fmt.Sprintf("%s/%s/%s/%s/%s", self.startDir, SUBJECTS_DIR, subject, STUDIES_DIR, study)

	for i := 1; i <= viper.GetInt("files_per_study"); i++ {
		filePath := fmt.Sprintf("%s/file_%d", dirPath, i)
		studyFile, err := os.Create(filePath)

		if err != nil {
			panic(err)
		}
	
		fileSize := viper.GetInt("file_size")
		fileContents := make([]byte, fileSize)
		_, err = rand.Read(fileContents)
		if err != nil { 
			panic(err)
		}
			
		n, err := studyFile.Write(fileContents)
		if err != nil {
			fmt.Errorf("Should write %d bytes -- wrote %d instead\n", fileSize, n)
			panic(err)
		}
		err = studyFile.Close()
		if err != nil {
			panic(err)
		}
	}
}

//home/thomas/go_workspace/src/firestore/storage_nodes/node_1/subjects/subject_1/study_1/file_1:

/*func (self * Ptfs) createStudyFile(filePath string) {
	// Fill file with noise
	file, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}

	fileSize := viper.GetInt("file_size")
	fileContents := make([]byte, fileSize)
	_, err = rand.Read(fileContents)
	if err != nil { 
	panic(err)
	}
		
	n, err := file.Write(fileContents)
	if err != nil {
		fmt.Errorf("Should write %d bytes -- wrote %d instead\n", fileSize, n)
		panic(err)
	}
	err = file.Close()
	if err != nil {
		panic(err)
	}
}*/