package fuse

import (
	"fmt"
	"os"
	"os/signal"
	"log"
	"context"
	"sync"
	"syscall"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/jacobsa/fuse/fuseops"
	cLog "github.com/inconshreveable/log15"
)

var ctx context.Context

type Filesystem struct {
	filesystem *FS
	fsServer *fuse.MountedFileSystem
}

//////////////////////////////////////////////
// PUBLIC FUNCTIONS 
//////////////////////////////////////////////
func FSMount(mountpoint string, fileMaxSize int) (*Filesystem, error) {
	fs, err := NewFileSystem(ctx, mountpoint)
	if err != nil {
		return nil, err
	}

	fsname := fmt.Sprintf("node_fs_%s", mountpoint)
	cfg := &fuse.MountConfig{
		FSName:      fsname,
		ReadOnly:    false,
		ErrorLogger: log.New(os.Stderr, "ERROR: ", log.LstdFlags),
	}

	fsMounted, err := fuse.Mount(
		mountpoint,
		fuseutil.NewFileSystemServer(fs),
		cfg,
	)

	if err != nil {
		return nil, err
	}

	cLog.Info("FUSE mounted", "dir", mountpoint)
	self := &Filesystem {
		filesystem: fs,
		fsServer: fsMounted,
	}

	go fsMounted.Join(ctx)
	return self, nil
}

/*func (fs *Filesystem) CreateDirectory(absPath string) error {
	if absPath[0] != '/' {
		return errors.New("Path needs to be absolute")
	}	
}*/

func (fs *Filesystem) MkDir(path string) {
	// relative to the mounting point
	
	
	//k := fs.filesystem
	//_ = k.ReadDir(ctx, &op)
}

func (fs *Filesystem) Shutdown() {
	cLog.Info("FUSE file system is unmounting...")
	err := fuse.Unmount(fs.fsServer.Dir())
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(3)
	}
}

//////////////////////////////////////////////
// PRIVATE FUNCTIONS 
//////////////////////////////////////////////
func init() {
	fmt.Printf("Initializing FUSE...\n")
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(context.Background())

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT)

	go func() {
		once := &sync.Once{}

		for range c {
			once.Do(func() {
				fmt.Println("Interrupt received, cleaning up mounted filesystem")
				cancel()
			})
		}
	}()
}
