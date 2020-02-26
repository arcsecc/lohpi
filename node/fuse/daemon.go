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

	"firestore/core/file"
	"github.com/billziss-gh/cgofuse/examples/shared"
	"github.com/billziss-gh/cgofuse/fuse"
	"github.com/pkg/xattr"
	"golang.org/x/sys/unix"
)

// The parent directory of the fuse mount point
const NODE_DIR = "storage_nodes"

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
}

func NewFuseFS(nodeID string) *Ptfs {
	startDir :=getLocalMountPoint(nodeID)
	mountDir := getDestinationMountPoint(nodeID)
	createDirectory(startDir)
	createDirectory(mountDir)
	syscall.Umask(0)
	ptfs := Ptfs{
		startDir: startDir,
		mountDir: mountDir,
		nodeID: nodeID,
		xattrFlag: false,
	}

	// Customized parameters. Need to purify them as well. Magic...
	opts := []string{"", "-o", "nonempty", startDir, mountDir}
	ptfs.root, _ = filepath.Abs(opts[len(opts) - 2])
	opts = append(opts[:len(opts) - 2], opts[len(opts) - 1])

	// A warm start should verify the files and their attributes by asking the 
	// rest of the network about the state of the permissions somewhere around here...
	// restore_permission_state()

	_host = fuse.NewFileSystemHost(&ptfs)
	go func() {
		ok := _host.Mount("", opts[1:])
		if ok != true {
			panic(errors.New("Could not mount Fuse system"))
		}
		log.Printf("Mounted dir on %s\n", mountDir)
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
	errc, err := syscall.Getxattr(path, attr, res)
	if err != nil {
		panic(err)
	}
 
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
	if !self.isAllowedAccess() {
		fmt.Printf("Statfs = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	defer trace(path)(&errc, stat)
	path = filepath.Join(self.root, path)
	stgo := syscall.Statfs_t{}
	errc = errno(syscall_Statfs(path, &stgo))
	copyFusestatfsFromGostatfs(stat, &stgo)
	return
}

func (self *Ptfs) Mknod(path string, mode uint32, dev uint64) (errc int) {
	if !self.isAllowedAccess() {
		fmt.Printf("Mknod = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	defer trace(path, mode, dev)(&errc)
	defer setuidgid()()
	path = filepath.Join(self.root, path)
	return errno(syscall.Mknod(path, mode, int(dev)))
}

func (self *Ptfs) Mkdir(path string, mode uint32) (errc int) {
	if !self.isAllowedAccess() {
		fmt.Printf("Mkdir = %d\n", int(unix.EPERM))
		return -int(unix.EPERM)
	}
	defer trace(path, mode)(&errc)
	defer setuidgid()()
	path = filepath.Join(self.root, path)
	return errno(syscall.Mkdir(path, mode))
}

func (self *Ptfs) Unlink(path string) (errc int) {
	if !self.isAllowedAccess() {
		fmt.Printf("Unlink = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	defer trace(path)(&errc)
	path = filepath.Join(self.root, path)
	return errno(syscall.Unlink(path))
}

func (self *Ptfs) Rmdir(path string) (errc int) {
	if !self.isAllowedAccess() {
		fmt.Printf("Rmdir = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	defer trace(path)(&errc)
	path = filepath.Join(self.root, path)
	return errno(syscall.Rmdir(path))
}

func (self *Ptfs) Link(oldpath string, newpath string) (errc int) {
	if !self.isAllowedAccess() {
		fmt.Printf("Link = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	defer trace(oldpath, newpath)(&errc)
	defer setuidgid()()
	oldpath = filepath.Join(self.root, oldpath)
	newpath = filepath.Join(self.root, newpath)
	return errno(syscall.Link(oldpath, newpath))
}

func (self *Ptfs) Symlink(target string, newpath string) (errc int) {
	if !self.isAllowedAccess() {
		fmt.Printf("Symlink = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	defer trace(target, newpath)(&errc)
	defer setuidgid()()
	newpath = filepath.Join(self.root, newpath)
	return errno(syscall.Symlink(target, newpath))
}

func (self *Ptfs) Readlink(path string) (errc int, target string) {
	if !self.isAllowedAccess() {
		fmt.Printf("Readlink = %d\n", int(unix.EPERM))
		return int(unix.EPERM), ""
	}
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
	if !self.isAllowedAccess() {
		fmt.Printf("Rename = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	defer trace(oldpath, newpath)(&errc)
	defer setuidgid()()
	oldpath = filepath.Join(self.root, oldpath)
	newpath = filepath.Join(self.root, newpath)
	return errno(syscall.Rename(oldpath, newpath))
}

func (self *Ptfs) Chmod(path string, mode uint32) (errc int) {
	if !self.isAllowedAccess() {
		fmt.Printf("Chmod = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	defer trace(path, mode)(&errc)
	path = filepath.Join(self.root, path)
	return errno(syscall.Chmod(path, mode))
}

func (self *Ptfs) Chown(path string, uid uint32, gid uint32) (errc int) {
	if !self.isAllowedAccess() {
		fmt.Printf("Chown = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
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
	if !self.isAllowedAccess() {
		fmt.Printf("Create = %d\n", int(unix.EPERM))
		return int(unix.EPERM), 0
	}
	defer trace(path, flags, mode)(&errc, &fh)
	defer setuidgid()()
	return self.open(path, flags, mode)
}

func (self *Ptfs) Open(path string, flags int) (errc int, fh uint64) {
	if !self.isAllowedAccess() {
		fmt.Printf("Open = %d\n", int(unix.EPERM))
		return int(unix.EPERM), 0
	}
	defer trace(path, flags)(&errc, &fh)
	return self.open(path, flags, 0)
}

func (self *Ptfs) open(path string, flags int, mode uint32) (errc int, fh uint64) {
	if !self.isAllowedAccess() {
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
	if !self.isAllowedAccess() {
		fmt.Printf("Truncate = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
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
	if !self.isAllowedAccess() {
		fmt.Printf("Read = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
	defer trace(path, buff, ofst, fh)(&n)
	n, e := syscall.Pread(int(fh), buff, ofst)
	if nil != e {
		return errno(e)
	}
	return n
}

func (self *Ptfs) Write(path string, buff []byte, ofst int64, fh uint64) (n int) {
	if !self.isAllowedAccess() {
		fmt.Printf("Write = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
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
	if !self.isAllowedAccess() {
		fmt.Printf("Opendir = %d\n", -int(unix.EPERM))
		return -int(unix.EPERM), ^uint64(0)
	}
	defer trace(path)(&errc, &fh)
	path = filepath.Join(self.root, path)
	f, e := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
	if nil != e {
		return errno(e), ^uint64(0)
	}
	return 0, uint64(f)
}

func (self *Ptfs) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, ofst int64,	fh uint64) (errc int) {
	if !self.isAllowedAccess() {
		fmt.Printf("Readdir = %d\n", int(unix.EPERM))
		return int(unix.EPERM)
	}
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

func (self *Ptfs) Shutdown() {
	fmt.Printf("Shutdown() not implemented!\n")
	/*	if self.Unmount() != true {
			fmt.Errorf("Unmount failed\n")
		}
		self.Destroy()*/
}

// Returns the path for the local entry point for the FUSE file system
func getLocalMountPoint(nodeName string) string {
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s/%s/%s", cwd, NODE_DIR, nodeName)
}

// Returns the location to which changes in the node's local storage directory
// are reflected
func getDestinationMountPoint(nodeName string) string {
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
	self.nodePermission = permission
	fmt.Printf("mountDir = %s\n", self.mountDir)
	fmt.Printf("startDir = %s\n", self.startDir)

	if _, err := os.Stat(self.mountDir); err != nil {
		if os.IsNotExist(err) {
			log.Printf("Directory %s does not exist!\n", self.mountDir)
		} else {
			log.Fatal("Some unknown error while checking if directory exists\n")
		}
	}
	
	// Set permission on current working directory
	self.xattrMux.Lock()
	self.xattrFlag = true
	if err := xattr.Set(self.mountDir, XATTR_PREFIX + "PERMISSION", []byte(permission)); err != nil {
		return err
	}
	self.xattrFlag = false
	self.xattrMux.Unlock()
	return nil
}

func (self *Ptfs) isAllowedAccess() bool {
	if self.nodePermission == file.FILE_ALLOWED {
		return true
	}
	return false
}

func (self *Ptfs) canSetXattr() bool {
	return self.xattrFlag	
}