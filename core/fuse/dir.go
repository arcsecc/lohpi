package fuse

import (
	"crypto/sha1"
	"fmt"
	"path"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

// seedForPath returns a new seed for an item of typ at path.
func seedForPath(parentSeed int64, tpe string, path string) (seed int64) {
	s := fmt.Sprintf("seed-%16x/%s/%s", seed, tpe, path)
	hash := sha1.Sum([]byte(s))
	for i := 0; i < 8; i++ {
		shift := uint(i) * 8
		seed |= int64(hash[i]) << shift
	}

	return seed
}

// Dir is a directory containing fake data.
type Dir struct {
	path    string

	entries []fuseutil.Dirent
	// inodes  map[fuseops.InodeID]string
}

// NewDir initializes a directory.
func NewDir(fs *FS, dir string) *Dir {
	d := Dir{
		path:    dir,
		entries: make([]fuseutil.Dirent, 0),
		// inodes:  make(map[fuseops.InodeID]string),
	}

	return &d
}

func (d Dir) String() string {
	return fmt.Sprintf("<Dir object: %v >", d.path)
}

// inode returns the inode for a given file name.
func (d Dir) inode(name string) fuseops.InodeID {
	return inodePath(path.Join(d.path, name))
}

// ReadDir returns the entries of this directory.
func (d Dir) ReadDir(dst []byte, offset int) (n int) {
	for _, entry := range d.entries[offset:] {
		written := fuseutil.WriteDirent(dst[n:], entry)
		if written == 0 {
			break
		}

		n += written
	}

	return n
}
