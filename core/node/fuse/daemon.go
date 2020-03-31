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
_	"time"
	"os"
	"path/filepath"
	"syscall"
	"log"
	"sync"
	"math/rand"
	"strings"
//	"io/ioutil"

//	"firestore/core/file"
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

var (
	errSubjectExists 			= errors.New("Subject already exists")
	errSetSubjectPermission 	= errors.New("Could not set permission related to a non-existing subject")
)

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
	isMounted bool
)

type Ptfs struct {
	fuse.FileSystemBase
	root string
	startDir string
	mountDir string
	nodeID string
	xattrMux sync.Mutex
	xattrFlag bool

	// Node permission lock and permission string
	nodeLock sync.Mutex
	nodePermission string

	// Collection of subject-centric data structures
	// TODO: more bullet-proof identification of subjects..?
	subjects map[string]interface{}			// subectID -> 
	subjectPermissions map[string]string // subjectID -> permission

	// Used to wait until FUSE in mounted
	ch chan bool
}

func NewFuseFS(nodeID string) (*Ptfs, error) {
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
		subjects: make(map[string]interface{}, 0),
		subjectPermissions: make(map[string]string, 0),
		ch: make(chan bool, 0),
	}

	// Customized parameters. Need to purify them as well. Magic...
	opts := []string{"", "-o", "nonempty", startDir, mountDir}
	ptfs.root, _ = filepath.Abs(opts[len(opts) - 2])
	opts = append(opts[:len(opts) - 2], opts[len(opts) - 1])
	_host = fuse.NewFileSystemHost(&ptfs)

	go func() {
		log.Printf("Mounting dir on %s\n", mountDir)
		ok := _host.Mount("", opts[1:])
		if ok != true {
			log.Fatal(errors.New("Could not mount Fuse system"))
		}
		fmt.Printf("Unmounted\n")
	}()

	// Wait for init to complete
	select {
		case <- ptfs.ch: 
			break
	}

	ptfs.initiate_daemon()
	fmt.Printf("Subjects: %v\n", ptfs.subjects)
	fmt.Printf("Subject permissions: %v\n", ptfs.subjectPermissions)
	return &ptfs, nil
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
	self.ch <- true
}

// Searches the fuse mounting point recursively three levels down to restore
// the state of the daemon. As a feature later in time, we might ask the rest of the network 
// about the correct state of this node
func (self *Ptfs) initiate_daemon() {
	// We start by creating the mounting point. Each change in the mount point shall we reflected into
	// the start directory
	fmt.Printf("P: %s\n", self.mountDir + "/" + STUDIES_DIR)
	createDirectory(self.mountDir + "/" + STUDIES_DIR)
	createDirectory(self.mountDir + "/" + SUBJECTS_DIR)
		
	if viper.GetBool("set_files") {
		fmt.Printf("Creating tree...\n")
		self.createStudyDirectoryTree()
		self.createSubjectDirectoryTree()
	}
}


// Returns the path for the local entry point for the FUSE file system
func creataLocalMountPointPath(nodeName string) string {
	fmt.Printf("mount: %s\n", viper.GetString("fuse_mount"))
	return fmt.Sprintf("%s/%s/%s", viper.GetString("fuse_mount"), NODE_DIR, nodeName)
}

// Returns the location to which changes in the node's local storage directory
// are reflected
func GetDestinationMountPoint(nodeName string) string {
	return fmt.Sprintf("/tmp/%s/%s", NODE_DIR, nodeName)
}

// Creates a directory tree using dirPath
func createDirectory(dirPath string) {
	fmt.Printf("Dir path: %s\n", dirPath)
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
	// Set permission on mount point both in-memory and in xattr
	self.xattrMux.Lock()
	self.nodePermission = permission
	log.Printf("Setting new permission for node %s: %s\n", self.nodeID, self.nodePermission)
	/*if err := xattr.Set(self.startDir, XATTR_PREFIX + "PERMISSION", []byte(permission)); err != nil {
		return err
	}*/
	self.xattrMux.Unlock()
	return nil
}

func (self *Ptfs) SetSubjectPermission(subject, permission string) error {	
	if self.SubjectExists(subject) {
		self.subjectPermissions[subject] = permission
		return nil
	}
	return errSetSubjectPermission
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
	path = filepath.Join(self.root, path)
	//fmt.Printf("Path to inspect: %s\n", path)

	// Node-centric access
	if self.nodePermission == "disallow" {
		return false
	}

	// Check if we access a forbidden part of the tree, with respect to the subjects
	for s, perm := range self.subjectPermissions {
//		fmt.Printf("Perm: %s\ts = %s\n", perm, s)
		ok := strings.Contains(path, s)
		if ok && perm == "disallow" {
			return false
		}
	}
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
		dirPath := fmt.Sprintf("%s/%s/study_%d", self.mountDir, STUDIES_DIR, i)
		fmt.Printf("DIRPATH: %s\n", dirPath)
		createDirectory(dirPath + "/" + DATA_USER_DIR)
		createDirectory(dirPath + "/" + METADATA_DIR)
		createDirectory(dirPath + "/" + PROTOCOL_DIR)
		createDirectory(dirPath + "/" + SUBJECTS_DIR)
		
		for j := 1; j <= viper.GetInt("num_subjects"); j++ {
			subjectDirPath := fmt.Sprintf("%s/%s/subject_%d", dirPath, SUBJECTS_DIR, j)
			createDirectory(subjectDirPath)
			
			// Set default file permission on study directory
			if err := xattr.Set(subjectDirPath, XATTR_PREFIX + "PERMISSION", []byte("allow")); err != nil {
				log.Fatal(err)
			}
		}
	}
}

// Move to another file!
func (self *Ptfs) createSubjectDirectoryTree() {
	for i := 1; i <= viper.GetInt("num_subjects"); i++ {
		subjectDirPath := fmt.Sprintf("%s/%s/subject_%d", self.mountDir, SUBJECTS_DIR, i)
		subjectID := fmt.Sprintf("subject_%d", i)
		createDirectory(subjectDirPath)

		if err := self.addSubject(subjectID); err != nil {
			panic(err)
		}
		
		if err := self.SetSubjectPermission(subjectID, "allow"); err != nil {
			panic(err)
		}

		for j := 1; j <= viper.GetInt("num_studies"); j++ {
			studyDirPath := fmt.Sprintf("%s/%s/study_%d", subjectDirPath, STUDIES_DIR, j)
			studyID := fmt.Sprintf("study_%d", j)
			createDirectory(studyDirPath)

			// Create the files assoicated with a subject and a study with random noise
			self.createStudyFiles(self.mountDir, subjectID, studyID)
			self.linkStudyFiles(self.mountDir, subjectID, studyID)
		}
	}
}

// Link files from 'subjects' directory to 'studies' directory
func (self *Ptfs) linkStudyFiles(parentDir, subject, study string) {
	filePathDir := fmt.Sprintf("%s/%s/%s/%s/%s", parentDir, SUBJECTS_DIR, subject, STUDIES_DIR, study)
	targetPathDir := fmt.Sprintf("%s/%s/%s/%s/%s", parentDir, STUDIES_DIR, study, SUBJECTS_DIR, subject)

	fmt.Printf("target path: %s\n", targetPathDir)

	for i := 1; i <= viper.GetInt("files_per_study"); i++ {
		filePath := fmt.Sprintf("%s/file_%d", filePathDir, i)
		targetPath := fmt.Sprintf("%s/file_%d", targetPathDir, i)
		if err := os.Symlink(filePath, targetPath); err != nil {
			panic(err)
		}
	}
}

// Insert files into 'subjects' directory. FIX ME
func (self *Ptfs) createStudyFiles(parentTree, subject, study string) {
	dirPath := fmt.Sprintf("%s/%s/%s/%s/%s", parentTree, SUBJECTS_DIR, subject, STUDIES_DIR, study)
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
			_  = err
		}
		err = studyFile.Close()
		if err != nil {
			panic(err)
		}
	}
}

func (self *Ptfs) CreateSubject(subjectID string) error {
	if err := self.addSubject(subjectID); err != nil {
		return err
	}
	dirPath := fmt.Sprintf("%s/%s/%s", self.mountDir, SUBJECTS_DIR, subjectID)
	createDirectory(dirPath)
	return nil
}

func (self *Ptfs) addSubject(subjectID string) error {
	if self.SubjectExists(subjectID) {
		return errSubjectExists
	}
	//fmt.Printf("Adding %s to the map\n", subjectID)
	self.subjects[subjectID] = ""
	return nil
}

func (self *Ptfs) SubjectExists(subjectID string) bool {
	if _, ok := self.subjects[subjectID]; ok {
		//fmt.Printf("%s exists in map\n", subjectID)
		return true
	}
	//fmt.Printf("%s does not exist in map\n", subjectID)
	return false
}

func (self *Ptfs) RemoveSubject(subjectID string) error {
	fmt.Printf("RemoveSubject not implemented -- nothing to do...\n")
	return nil
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
	/*if !self.canSetXattr() {
		fmt.Printf("Setxattr\n")
		return int(unix.EPERM)
	}*/

	defer trace(path, attr, data, flags)(&errc)
	path = filepath.Join(self.root, path)
	errc = errno(syscall.Setxattr(path, attr, data, flags))
	return
}

func (self *Ptfs) Removexattr(path string, attr string) (errc int) {
	if !self.canSetXattr() {
		fmt.Printf("Removexattr = %d\n", int(unix.EPERM))
		return -int(fuse.EACCES)
	}

	defer trace(path, attr)(&errc)
	path = filepath.Join(self.root, path)
	errc = errno(syscall.Removexattr(path, attr))
	return errc
}

func (self *Ptfs) Statfs(path string, stat *fuse.Statfs_t) (errc int) {
	defer trace(path)(&errc, stat)
	path = filepath.Join(self.root, path)
	stgo := syscall.Statfs_t{}
	errc = errno(syscall_Statfs(path, &stgo))
	copyFusestatfsFromGostatfs(stat, &stgo)
	return
}

func (self *Ptfs) Mknod(path string, mode uint32, dev uint64) (errc int) {
	defer trace(path, mode, dev)(&errc)
	fmt.Printf("Mknod()...\n")
	defer setuidgid()()
	path = filepath.Join(self.root, path)
	if !self.isAllowedAccess(path) {
		fmt.Printf("Mknod = %d\n", int(unix.EPERM))
		return -int(fuse.EACCES)
	}	
	return errno(syscall.Mknod(path, mode, int(dev)))
}

func (self *Ptfs) Mkdir(path string, mode uint32) (errc int) {
	defer trace(path, mode)(&errc)
	fmt.Printf("Mkdir()...\n")
	defer setuidgid()()
	path = filepath.Join(self.root, path)
	if !self.isAllowedAccess(path) {
		fmt.Printf("Mkdir = %d\n", int(unix.EPERM))
		return -int(fuse.EACCES)
	}
	return errno(syscall.Mkdir(path, mode))
}

func (self *Ptfs) Unlink(path string) (errc int) {
	defer trace(path)(&errc)
	fmt.Println("Unlink")
	path = filepath.Join(self.root, path)
	if !self.isAllowedAccess(path) {
		fmt.Printf("Unlink = %d\n", int(unix.EPERM))
		return -int(fuse.EACCES)
	}
	return errno(syscall.Unlink(path))
}

func (self *Ptfs) Rmdir(path string) (errc int) {
	defer trace(path)(&errc)
	fmt.Println("Rmdir")
	path = filepath.Join(self.root, path)
	if !self.isAllowedAccess(path) {
		fmt.Printf("rmdir = %d\n", int(unix.EPERM))
		return -int(fuse.EACCES)
	}
	return errno(syscall.Rmdir(path))
}

func (self *Ptfs) Link(oldpath string, newpath string) (errc int) {
	defer trace(oldpath, newpath)(&errc)
	fmt.Println("Link")
	defer setuidgid()()
	oldpath = filepath.Join(self.root, oldpath)
	newpath = filepath.Join(self.root, newpath)
	if !self.isAllowedAccess(newpath) {
		fmt.Printf("Link = %d\n", int(unix.EPERM))
		return -int(fuse.EACCES)
	}
	return errno(syscall.Link(oldpath, newpath))
}

func (self *Ptfs) Symlink(target string, newpath string) (errc int) {
	defer trace(target, newpath)(&errc)
	fmt.Println("Symlink")
	defer setuidgid()()
	newpath = filepath.Join(self.root, newpath)
	if !self.isAllowedAccess(newpath) {
		fmt.Printf("Link = %d\n", int(unix.EPERM))
		return -int(fuse.EACCES)
	}
	return errno(syscall.Symlink(target, newpath))
}

func (self *Ptfs) Readlink(path string) (errc int, target string) {
	defer trace(path)(&errc, &target)
	fmt.Println("Readlink")
	path = filepath.Join(self.root, path)
	if !self.isAllowedAccess(path) {
		fmt.Printf("Readlink = %d\n", int(unix.EPERM))
		return -int(fuse.EACCES), ""
	}
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
	defer setuidgid()()
	oldpath = filepath.Join(self.root, oldpath)
	newpath = filepath.Join(self.root, newpath)
	if !self.isAllowedAccess(oldpath) {
		fmt.Printf("Readlink = %d\n", int(unix.EPERM))
		return -int(fuse.EACCES)
	}
	return errno(syscall.Rename(oldpath, newpath))
}

func (self *Ptfs) Chmod(path string, mode uint32) (errc int) {
	defer trace(path, mode)(&errc)
	fmt.Println("Chmod")
	path = filepath.Join(self.root, path)
	if !self.isAllowedAccess(path) {
		fmt.Printf("Chmod = %d\n", int(unix.EPERM))
		return -int(fuse.EACCES)
	}
	return errno(syscall.Chmod(path, mode))
}

func (self *Ptfs) Chown(path string, uid uint32, gid uint32) (errc int) {
	defer trace(path, uid, gid)(&errc)
	fmt.Println("Chown")
	path = filepath.Join(self.root, path)
	if !self.isAllowedAccess(path) {
		fmt.Printf("Chown = %d\n", int(unix.EPERM))
		return -int(fuse.EACCES)
	}
	return errno(syscall.Lchown(path, int(uid), int(gid)))
}

func (self *Ptfs) Utimens(path string, tmsp1 []fuse.Timespec) (errc int) {
	defer trace(path, tmsp1)(&errc)
	fmt.Println("Utimens")
	path = filepath.Join(self.root, path)
	if !self.isAllowedAccess(path) {
		fmt.Printf("Utimens = %d\n", int(unix.EPERM))
		return -int(fuse.EACCES)
	}
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
		return -int(fuse.EACCES), 0
	}
	return self.open(path, flags, mode)
}

func (self *Ptfs) Open(path string, flags int) (errc int, fh uint64) {
	defer trace(path, flags)(&errc, &fh)
	fmt.Println("Open")
	if !self.isAllowedAccess(path) {
		fmt.Printf("Open = %d\n", int(unix.EPERM))
		return -int(fuse.EACCES), 0
	}
	return self.open(path, flags, 0)
}

func (self *Ptfs) open(path string, flags int, mode uint32) (errc int, fh uint64) {
	fmt.Println("open")
	path = filepath.Join(self.root, path)
	if !self.isAllowedAccess(path) {
		fmt.Printf("open = %d\n", int(unix.EPERM))
		return -int(fuse.EACCES), 0
	}
	f, e := syscall.Open(path, flags, mode)
	if nil != e {
		return errno(e), ^uint64(0)
	}
	return 0, uint64(f)
}

func (self *Ptfs) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	defer trace(path, fh)(&errc, stat)
	fmt.Println("Getattr")
	/*if !self.isAllowedAccess(path) {
		fmt.Printf("Getattr = %d\n", int(unix.EPERM))
		return -int(fuse.EACCES)
	}*/
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
		return -int(fuse.EACCES)
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
		return -int(fuse.EACCES)
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
		return -int(fuse.EACCES)
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
		fmt.Printf("Release = %d\n", int(unix.EPERM))
		return -int(fuse.EACCES)
	}
	defer trace(path, fh)(&errc)
	return errno(syscall.Close(int(fh)))
}

func (self *Ptfs) Fsync(path string, datasync bool, fh uint64) (errc int) {
	defer trace(path, datasync, fh)(&errc)
	fmt.Println("Fsync")
	return errno(syscall.Fsync(int(fh)))
}

func (self *Ptfs) Opendir(path string) (errc int, fh uint64) {
	defer trace(path)(&errc, &fh)
	fmt.Println("Opendir")
	path = filepath.Join(self.root, path)
	if !self.isAllowedAccess(path) {
		fmt.Printf("Opendir = %d\n", int(unix.EPERM))
		return -int(fuse.EACCES), 0
	}
	f, e := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
	if nil != e {
		return errno(e), ^uint64(0)
	}
	return 0, uint64(f)
}

func (self *Ptfs) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, ofst int64,	fh uint64) (errc int) {
	defer trace(path, fill, ofst, fh)(&errc)
	fmt.Println("Readdir")
	path = filepath.Join(self.root, path)
	if !self.isAllowedAccess(path) {
		fmt.Printf("Readdir = %d\n", int(unix.EPERM))
		return -int(fuse.EACCES)
	}
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
		return -int(fuse.EACCES)
	}
	return errno(syscall.Close(int(fh)))
}

func Shutdown() {
	fmt.Printf("Unmounting file system\n")
	/*if _host.Unmount() != true {
		fmt.Errorf("Unmount failed\n")
	}*/
}

