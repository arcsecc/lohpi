// +build darwin freebsd netbsd openbsd linux

/*
 * passthrough.go
 *
 * Copyright 2017-2018 Bill Zissimopoulos
 
/*
 * This file is part of Cgofuse.
 *
 * It is licensed under the MIT license. The full license text can be found
 * in the License.txt file at the root of this project.*/
 
 package fuse
/*
import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	pb "github.com/tomcat-bit/lohpi/protobuf" 
	"github.com/billziss-gh/cgofuse/examples/shared"
	"github.com/billziss-gh/cgofuse/fuse"
	"github.com/pkg/xattr"
)

// Directory name constants
const NODE_DIR = "storage_nodes"
const DATA_USER_DIR = "data users"
const METADATA_DIR = "metadata"
const PROTOCOL_DIR = "protcol"
const SUBJECTS_DIR = "subjects"
const STUDIES_DIR = "studies"
const METADATA_FILE = "metadata.json"

// Used by the policy manager to set the action requested
const ACCESS = "read"

// A magical prefix used by xattr
const XATTR_PREFIX = "user."

var (
	errSetSubjectPermission = errors.New("Could not set permission related to a non-existing subject")
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

type Config struct {
	MountDirectory string
}

var (
	_host     *fuse.FileSystemHost
	isMounted bool
)

// Pass-through file system
type Ptfs struct {
	fuse.FileSystemBase
	root      string
	StartDir  string
	MountDir  string
	nodeID    string
	xattrMux  sync.Mutex
	xattrFlag bool

	// Internal maps that keeps information about subjects and objects
	objectHeadersMap  		map[string][]*pb.ObjectHeader      // subectID -> object they are a part of
	objectHeadersMapLock 	sync.RWMutex

	// Subject mapped to objects 
	subjectsMap   	map[string][]string
	subjectsMapLock	sync.RWMutex

	ch chan bool
}

func NewFuseFS(nodeID string, config *Config) (*Ptfs, error) {
	startDir := localMountPointPath(nodeID, config.MountDirectory)
	mountDir := destinationMountPointPath(nodeID)

	// restoreFuseState() should fill all in-memory structures with the on-disk
	// contents. This ensures a fault-tolerant model of the fuse module.
	createDirectory(startDir)
	createDirectory(mountDir)

	syscall.Umask(0)
	ptfs := Ptfs{
		StartDir:           startDir,
		MountDir:           mountDir,
		nodeID:             nodeID,
		xattrFlag:          false, // can we set xattr?
		
		objectHeadersMap:		make(map[string][]*pb.ObjectHeader),
		objectHeadersMapLock: 	sync.RWMutex{},
		
		subjectsMap:		make(map[string][]string),
		subjectsMapLock:	sync.RWMutex{},

		ch:                 make(chan bool, 0),
	}

	// Customized parameters. Need to purify them as well. Magic...
	//opts := []string{"", "-o", "nonempty", "allow_other", startDir, mountDir}
	opts := []string{"", "-o", "default_permissions", "allow_other", startDir, mountDir}
	ptfs.root, _ = filepath.Abs(opts[len(opts)-2])
	opts = append(opts[:len(opts)-2], opts[len(opts)-1])
	_host = fuse.NewFileSystemHost(&ptfs)

	go func() {
		//log.Printf("Mounting dir on %s\n", mountDir)
		ok := _host.Mount("", opts[4:])
		if ok != true {
			log.Fatal(errors.New("Could not mount Fuse system"))
		}
		//fmt.Printf("Unmounted\n")
	}()

	// Wait for init to complete
	select {
	case <-ptfs.ch:
		break
	}

	if err := ptfs.initiate_daemon(); err != nil {
		log.Fatalf(err.Error())
	}
	return &ptfs, nil
}

func (self *Ptfs) Init() {
	defer trace()()
	e := syscall.Chdir(self.root)
	if e == nil {
		self.root = "./"
	}
	self.ch <- true
}

func (self *Ptfs) ObjectHeaders() map[string][]*pb.ObjectHeader {
	self.objectHeadersMapLock.RLock()
	defer self.objectHeadersMapLock.RUnlock()
	return self.objectHeadersMap
}

func (self *Ptfs) Subjects() map[string][]string {
	self.subjectsMapLock.RLock()
	defer self.subjectsMapLock.RUnlock()
	return self.subjectsMap
}

func (self *Ptfs) InsertSubject(subject string, objectId string) {
	self.subjectsMapLock.RLock()
	defer self.subjectsMapLock.RUnlock()
	if self.subjectsMap[subject] == nil {
		self.subjectsMap[subject] = make([]string, 0)
	}

	self.subjectsMap[subject] = append(self.subjectsMap[subject], objectId)
}

func (self *Ptfs) EraseObjectsFromSubject(subjectId string) {
	self.subjectsMapLock.RLock()
	defer self.subjectsMapLock.RUnlock()
	self.subjectsMap[subjectId] = make([]string, 0)
}

func (self *Ptfs) SubjectExists(subjectId string) bool {
	self.subjectsMapLock.RLock()
	defer self.subjectsMapLock.RUnlock()
	_, ok := self.subjectsMap[subjectId]
	return ok
}

func (self *Ptfs) ObjectHeaderExists(objectId string) bool {
	self.objectHeadersMapLock.Lock()
	defer self.objectHeadersMapLock.Unlock()
	_, ok := self.objectHeadersMap[objectId] 
	return ok
}

func (self *Ptfs) SubjectExistsInObject(subjectId, objectId string) bool {
	allHeaders := self.ObjectHeaders()
	headers := allHeaders[objectId]

	if headers == nil {
		return false
	}

	for _, header := range headers {
		_ = header
/*		for _, s := range header.GetMetadata().GetSubjects() {
			if subjectId == s {
				return true
			}
		}
	}
	
	return false
}

func (self *Ptfs) SetExtendedAttribute(path, name string, value []byte) error {
	//return xattr.SetWithFlags(path, XATTR_PREFIX+"test", value, xattr.XATTR_CREATE)
	return xattr.Set(path, XATTR_PREFIX+name, value)
}

func (self *Ptfs) ListExtendedAttributes(path string) ([]string, error) {
	return []string{}, nil
}

func (self *Ptfs) GetExtendedAttribute(path, name string) ([]byte, error) {
	return xattr.Get(path, XATTR_PREFIX+name)
}

func (self *Ptfs) RemoveExtendedAttribute(path, name string) ([]string, error) {
	return []string{}, nil
}

/*func (self *Ptfs) SetExtendedAttributeWithFlags(path, name string, data []byte, flags int) error {
	return nil
}

// Searches the fuse mounting point recursively three levels down to restore
// the state of the daemon. As a feature later in time, we might ask the rest of the network
// about the correct state of this node
func (self *Ptfs) initiate_daemon() error {
	// We start by creating the mounting point. Each change in the mount point shall we reflected into
	// the start directory
	if err := createDirectory(self.MountDir + "/" + STUDIES_DIR); err != nil {
		return err
	}

	if err := createDirectory(self.MountDir + "/" + SUBJECTS_DIR); err != nil {
		return err
	}

	return nil
}

// Returns the path for the local entry point for the FUSE file system
func localMountPointPath(nodename, dirname string) string {
	return fmt.Sprintf("%s/%s/%s", dirname, NODE_DIR, nodename)
}

// Returns the location to which changes in the node's local storage directory
// are reflected
func destinationMountPointPath(nodeName string) string {
	return fmt.Sprintf("/tmp/%s/%s", NODE_DIR, nodeName)
}

func (self *Ptfs) setXattrFlag(path, permission string) error {
	// might use lock here...
	/*if err := xattr.Set(path, XATTR_PREFIX + "PERMISSION", []byte(permission)); err != nil {
		fmt.Println(err.Error())
		return err
	}
	/*self.xattrFlag = false
	self.xattrMux.Unlock()
	return nil
}

func (self *Ptfs) isAllowedAccess(path string) bool {
	path = filepath.Join(self.root, path)
	//fmt.Printf("Path to inspect: %s\n", path)

	/*
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
	return self.StartDir
}

func (self *Ptfs) RemoveSubject(subjectID string) error {
	//fmt.Printf("RemoveSubject not implemented -- nothing to do...\n")
	return nil
}

// TODO: finish it l8r
func (self *Ptfs) Listxattr(path string, fill func(name string) bool) (errc int) {
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
	//fmt.Printf("Getxattr() -> path: %v\nattr: %v\nres: %s\nerrc: %v\n", path, attr, res, errc)

	// return errc!!!
	return errc, res
}

func (self *Ptfs) Setxattr(path string, attr string, data []byte, flags int) (errc int) {
	defer trace(path, attr, data, flags)(&errc)
	path = filepath.Join(self.root, path)
	errc = errno(syscall.Setxattr(path, attr, data, flags))
	return
}

func (self *Ptfs) Removexattr(path string, attr string) (errc int) {
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
	defer setuidgid()()
	path = filepath.Join(self.root, path)
	return errno(syscall.Mknod(path, mode, int(dev)))
}

func (self *Ptfs) Mkdir(path string, mode uint32) (errc int) {
	defer trace(path, mode)(&errc)
	defer setuidgid()()
	path = filepath.Join(self.root, path)
	return errno(syscall.Mkdir(path, mode))
}

func (self *Ptfs) Unlink(path string) (errc int) {
	defer trace(path)(&errc)
	path = filepath.Join(self.root, path)
	return errno(syscall.Unlink(path))
}

func (self *Ptfs) Rmdir(path string) (errc int) {
	defer trace(path)(&errc)
	path = filepath.Join(self.root, path)
	return errno(syscall.Rmdir(path))
}

func (self *Ptfs) Link(oldpath string, newpath string) (errc int) {
	defer trace(oldpath, newpath)(&errc)
	defer setuidgid()()
	oldpath = filepath.Join(self.root, oldpath)
	newpath = filepath.Join(self.root, newpath)
	return errno(syscall.Link(oldpath, newpath))
}

func (self *Ptfs) Symlink(target string, newpath string) (errc int) {
	defer trace(target, newpath)(&errc)
	defer setuidgid()()
	newpath = filepath.Join(self.root, newpath)
	return errno(syscall.Symlink(target, newpath))
}

func (self *Ptfs) Readlink(path string) (errc int, target string) {
	defer trace(path)(&errc, &target)
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
	defer setuidgid()()
	oldpath = filepath.Join(self.root, oldpath)
	newpath = filepath.Join(self.root, newpath)
	return errno(syscall.Rename(oldpath, newpath))
}

func (self *Ptfs) Chmod(path string, mode uint32) (errc int) {
	defer trace(path, mode)(&errc)
	path = filepath.Join(self.root, path)
	return errno(syscall.Chmod(path, mode))
}

func (self *Ptfs) Chown(path string, uid uint32, gid uint32) (errc int) {
	defer trace(path, uid, gid)(&errc)
	path = filepath.Join(self.root, path)
	return errno(syscall.Lchown(path, int(uid), int(gid)))
}

func (self *Ptfs) Utimens(path string, tmsp1 []fuse.Timespec) (errc int) {
	defer trace(path, tmsp1)(&errc)
	path = filepath.Join(self.root, path)
	tmsp := [2]syscall.Timespec{}
	tmsp[0].Sec, tmsp[0].Nsec = tmsp1[0].Sec, tmsp1[0].Nsec
	tmsp[1].Sec, tmsp[1].Nsec = tmsp1[1].Sec, tmsp1[1].Nsec
	return errno(syscall.UtimesNano(path, tmsp[:]))
}

func (self *Ptfs) Create(path string, flags int, mode uint32) (errc int, fh uint64) {
	defer trace(path, flags, mode)(&errc, &fh)
	defer setuidgid()()
	return self.open(path, flags, mode)
}

func (self *Ptfs) Open(path string, flags int) (errc int, fh uint64) {
	defer trace(path, flags)(&errc, &fh)
	return self.open(path, flags, 0)
}

func (self *Ptfs) open(path string, flags int, mode uint32) (errc int, fh uint64) {
	path = filepath.Join(self.root, path)
	f, e := syscall.Open(path, flags, mode)
	if nil != e {
		return errno(e), ^uint64(0)
	}
	return 0, uint64(f)
}

func (self *Ptfs) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	defer trace(path, fh)(&errc, stat)
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
	n, e := syscall.Pread(int(fh), buff, ofst)
	if nil != e {
		return errno(e)
	}
	return n
}

func (self *Ptfs) Write(path string, buff []byte, ofst int64, fh uint64) (n int) {
	defer trace(path, buff, ofst, fh)(&n)
	n, e := syscall.Pwrite(int(fh), buff, ofst)
	if nil != e {
		return errno(e)
	}
	return n
}

func (self *Ptfs) Release(path string, fh uint64) (errc int) {
	defer trace(path, fh)(&errc)
	return errno(syscall.Close(int(fh)))
}

func (self *Ptfs) Fsync(path string, datasync bool, fh uint64) (errc int) {
	defer trace(path, datasync, fh)(&errc)
	return errno(syscall.Fsync(int(fh)))
}

func (self *Ptfs) Opendir(path string) (errc int, fh uint64) {
	defer trace(path)(&errc, &fh)
	path = filepath.Join(self.root, path)
	f, e := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
	if nil != e {
		return errno(e), ^uint64(0)
	}
	return 0, uint64(f)
}

func (self *Ptfs) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, ofst int64, fh uint64) (errc int) {
	defer trace(path, fill, ofst, fh)(&errc)
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
	return errno(syscall.Close(int(fh)))
}

func Shutdown() {
	fmt.Printf("Unmounting file system\n")
	/*if _host.Unmount() != true {
		fmt.Errorf("Unmount failed\n")
	}
}

// Creates a directory tree from the root to 'dirPath' if it does not exist
func createDirectory(dirPath string) error {
	if _, err := os.Stat(dirPath); err != nil {
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			return err
		}
	}
	return nil
}*/