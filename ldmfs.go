// Package ldmfs implements the code of live-data-move fs.
// It is a fuse filesystem that moves data between directories while you're using it.
//
// This is a proof of concept. If you use this code in production, please send me videos of your chat with your manager after it ate all your data.
package ldmfs

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"log"
	"path/filepath"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// New returns a new pass-through fs.FS.
func New(srcPath, dstPath string) fs.FS {
	return &filesystem{
		srcPath: srcPath,
		dstPath: dstPath,
		fileToIntervals: map[string]*Intervals{},
		fileToSize: map[string]int64{},
	}
}

type filesystem struct {
	srcPath, dstPath string
	fileToIntervals map[string]*Intervals
		fileToSize  map[string]int64
}

func (f *filesystem) Root() (fs.Node, error) {
	return &dir{node{f, ""}}, nil
}

type node struct {
	fs   *filesystem
	path string
}

func (n node) srcPath() string {
	return filepath.Join(n.fs.srcPath, n.path)
}

func (n node) dstPath() string {
	return filepath.Join(n.fs.dstPath, n.path)
}

func (n *node) stat() (os.FileInfo, error) {
	stat, err := os.Stat(n.srcPath())
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		stat, err = os.Stat(n.dstPath())
	}
	return stat, err
}

func (n *node) Attr(ctx context.Context, attr *fuse.Attr) (retErr error) {
	stat, err := n.stat()
	if err != nil {
		return err
	}
	attr.Size = uint64(stat.Size())
	attr.Mode = stat.Mode()
	attr.Mtime = stat.ModTime()
	if st, ok := stat.Sys().(*syscall.Stat_t); ok {
		attr.Blocks = uint64(st.Blocks)
		attr.Atime = time.Unix(st.Atim.Unix())
		attr.Ctime = time.Unix(st.Ctim.Unix())
		attr.Nlink = uint32(st.Nlink)
		attr.Uid = st.Uid
		attr.Gid = st.Gid
		attr.Rdev = uint32(st.Rdev)
		attr.BlockSize = uint32(st.Blksize)
	}
	return nil
}

func (n node) doToPath(f func(path string) error) error {
	srcExists, _, err := exists(n.srcPath())
	if err != nil {
		return err
	}
	if srcExists {
		if err := f(n.dstPath()); err != nil && !os.IsNotExist(err) {
			return err
		}
		return f(n.srcPath())
	}
	return f(n.dstPath())
}

func (n *node) Setattr(ctx context.Context, request *fuse.SetattrRequest, response *fuse.SetattrResponse) (retErr error) {
	if request.Valid.Uid() || request.Valid.Gid() {
		uid, gid := -1, -1
		if request.Valid.Uid() {
			uid = int(request.Uid)
		}
		if request.Valid.Gid() {
			gid = int(request.Gid)
		}
		if err := n.doToPath(func(path string) error {
			return os.Chown(path, uid, gid)
		}); err != nil {
			return err
		}
	}
	if request.Valid.Mode() {
		if err := n.doToPath(func(path string) error {
			return os.Chmod(path, request.Mode)
		}); err != nil {
			return err
		}
	}
	if request.Valid.Size() {
		if err := n.doToPath(func(path string) error {
			return os.Truncate(path, int64(request.Size))
		}); err != nil {
			return err
		}
	}
	if request.Valid.Mtime() || request.Valid.Atime() {
		var mtime, atime time.Time
		if !request.Valid.Mtime() || !request.Valid.Atime() {
			stat, err := n.stat()
			if err != nil {
				return err
			}
			mtime = stat.ModTime()
			if st, ok := stat.Sys().(*syscall.Stat_t); ok {
				atime = time.Unix(st.Atim.Unix())
			} else {
				atime = time.Now()
			}
		}
		if request.Valid.Mtime() {
			mtime = request.Mtime
		}
		if request.Valid.Atime() {
			atime = request.Atime
		}
		if err := n.doToPath(func(path string) error {
			return os.Chtimes(path, mtime, atime)
		}); err != nil {
			return err
		}
	}
	return nil
}

type dir struct {
	node
}

func (d *dir) Create(ctx context.Context, request *fuse.CreateRequest, response *fuse.CreateResponse) (_ fs.Node, _ fs.Handle, retErr error) {
	file := &file{node{d.fs, filepath.Join(d.path, request.Name)}}
	handle, err := file.openFile(int(request.Flags), request.Mode)
	return file, handle, err
}

func (d *dir) Lookup(ctx context.Context, name string) (_ fs.Node, retErr error) {
	n := node{d.fs, filepath.Join(d.path, name)}
	stat, err := n.stat()
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fuse.ENOENT
		}
		return nil, err
	}
	if stat.IsDir() {
		return &dir{n}, nil
	}
	return &file{n}, nil
}

func (n node) createPathUpto() error {
	if n.path == "" || n.path == "." {
		return nil
	}
	p := node{n.fs, filepath.Dir(n.path)}
	if err := p.createPathUpto(); err != nil {
		return err
	}
	stat, err := os.Stat(p.srcPath())
	if err != nil {
		return err
	}
	err = os.Mkdir(p.dstPath(), stat.Mode())
	if os.IsExist(err) {
		return nil
	}
	return err
}

func (d *dir) Mkdir(ctx context.Context, request *fuse.MkdirRequest) (_ fs.Node, retErr error) {
	n := node{d.fs, filepath.Join(d.path, request.Name)}
	if err := n.createPathUpto(); err != nil {
		return nil, err
	}
	if err := os.Mkdir(n.dstPath(), request.Mode); err != nil {
		return nil, err
	}
	if err := os.Mkdir(n.srcPath(), request.Mode); err != nil {
		return nil, err
	}
	return &dir{n}, nil
}

func (d *dir) ReadDirAll(ctx context.Context) (_ []fuse.Dirent, retErr error) {
	stats, err := ioutil.ReadDir(d.srcPath())
	if err != nil {
		return nil, err
	}
	dstStats, err := ioutil.ReadDir(d.dstPath())
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	stats = append(stats, dstStats...)
	dirents := make([]fuse.Dirent, 0, len(stats))
	seen := make(map[string]bool, len(stats))
	for _, stat := range stats {
		b := filepath.Base(stat.Name())
		if seen[b] {
			continue
		}
		seen[b] = true
		t := fuse.DT_Unknown
		switch stat.Mode() & os.ModeType {
		case os.ModeDir:
			t = fuse.DT_Dir
		case os.ModeSymlink:
			t = fuse.DT_Link
		case os.ModeNamedPipe:
			t = fuse.DT_FIFO
		case os.ModeSocket:
			t = fuse.DT_Socket
		case os.ModeDevice:
			if stat.Mode()&os.ModeCharDevice == 0 {
				t = fuse.DT_Block
			} else {
				t = fuse.DT_Char
			}
		case 0:
			t = fuse.DT_File
		}
		dirents = append(dirents, fuse.Dirent{
			Name: b,
			Type: t,
		})
	}
	return dirents, nil
}

func (d *dir) Remove(ctx context.Context, request *fuse.RemoveRequest) error {
	n := node{d.fs, filepath.Join(d.path, request.Name)}
	return n.doToPath(func(path string) error {
		return os.Remove(path)
	})
}

type file struct {
	node
}

func (f *file) Open(ctx context.Context, request *fuse.OpenRequest, response *fuse.OpenResponse) (_ fs.Handle, retErr error) {
	return f.openFile(int(request.Flags), 0666)
}

func exists(path string) (bool, os.FileInfo, error) {
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil, nil
		}
		return false, nil, err
	}
	return true, st, nil
}

func (f *file) openFile(flags int, mode os.FileMode) (fs.Handle, error) {
	srcExists, stat, err := exists(f.srcPath())
	if err != nil {
		return nil, err
	}
	dstExists, _, err := exists(f.dstPath())
	if err != nil {
		return nil, err
	}
	if !dstExists {
		if err := f.createPathUpto(); err != nil {
			return nil, err
		}
	}
	if !srcExists {
		dstFile, err := os.OpenFile(f.dstPath(), flags, mode)
		if err != nil {
			return nil, err
		}
		return &handle{f.node, nil, dstFile, &Intervals{}}, nil
	}
	dstFlags := flags & ^(os.O_SYNC | os.O_RDONLY)
	if dstFlags&os.O_WRONLY == 0 {
		dstFlags |= os.O_RDWR
	}
	if !dstExists {
		dstFlags |= os.O_CREATE
	}
	dstFile, err := os.OpenFile(f.dstPath(), dstFlags, mode)
	if err != nil {
		return nil, err
	}
	// TODO: copy all properties (chown, chgrp, chmod, size)
	srcFile, err := os.OpenFile(f.srcPath(), flags, mode)
	if err != nil {
		return nil, err
	}
	ivs := f.fs.fileToIntervals[f.path]
	if ivs == nil {
		ivs = &Intervals{}
		f.fs.fileToIntervals[f.path] = ivs
	}
	f.fs.fileToSize[f.path] = stat.Size()
	return &handle{f.node, srcFile, dstFile, ivs}, nil
}

type handle struct {
	node
	srcFile, dstFile *os.File
	intervals        *Intervals
}

func (h *handle) Read(ctx context.Context, request *fuse.ReadRequest, response *fuse.ReadResponse) (retErr error) {
	f := h.dstFile
	if h.srcFile != nil {
		f = h.srcFile
	}
	buffer := make([]byte, request.Size)
	n, err := f.ReadAt(buffer, request.Offset)
	response.Data = buffer[:n]
	if err != nil && err != io.EOF {
		return err
	}
	if h.srcFile != nil && !h.intervals.Has(request.Offset, request.Offset+int64(n)) {
		n, err := h.dstFile.WriteAt(response.Data, request.Offset)
		if err != nil && err != io.EOF {
			// ignore
			log.Printf("Failed to update %s: %v", h.dstFile.Name(), err)
		} else {
			h.intervals.Add(request.Offset, request.Offset+int64(n))
			log.Printf("Updated %s [%d-%d]", h.dstFile.Name(), request.Offset, request.Offset+int64(n))
			h.checkComplete()
		}
	}
	return nil
}

func (h *handle) Write(ctx context.Context, request *fuse.WriteRequest, response *fuse.WriteResponse) (retErr error) {
	f := h.dstFile
	if h.srcFile != nil {
		f = h.srcFile
		h.intervals.Del(request.Offset, request.Offset+int64(len(request.Data)))
	}
	n, err := f.WriteAt(request.Data, request.Offset)
	response.Size = n
	if err != nil {
		return err
	}
	if h.srcFile != nil {
		n, err := h.dstFile.WriteAt(request.Data, request.Offset)
		if err != nil {
			// ignore
		} else {
			h.intervals.Add(request.Offset, request.Offset+int64(n))
			h.checkComplete()
		}
	}
	return nil
}

func (h *handle) checkComplete() {
	if !h.intervals.Has(0, h.fs.fileToSize[h.path]) {
		return
	}
	if err := os.Remove(h.srcPath()); err != nil {
		log.Printf("Warning: Failed to mark copy of %q complete: %v", h.srcPath(), err)
		return
	}
	 h.srcFile.Close()
	 h.srcFile = nil
	 h.intervals = nil
	 delete(h.fs.fileToSize, h.path)
	 delete(h.fs.fileToIntervals, h.path)
	 log.Printf("Completed copy of %q", h.path)
}

func (h *handle) Fsync(ctx context.Context, request *fuse.FsyncRequest) error {
	// TODO
	return nil
}

func (h *handle) Release(ctx context.Context, request *fuse.ReleaseRequest) error {
	if h.srcFile != nil {
		if err := h.srcFile.Close(); err != nil {
			return err
		}
	}
	return h.dstFile.Close()
}
