/*
Package main mounts an ldmfs on a directory.
*/
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	ldmfs "github.com/Jille/live-data-move-fs"
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n  %s <src> <dst> <mountpoint>\n", os.Args[0], os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() != 3 {
		usage()
		os.Exit(2)
	}
	if err := do(os.Args[1], os.Args[2], os.Args[3]); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	os.Exit(0)
}

func do(srcPath, dstPath, mountpoint string) (retErr error) {
	if err := os.MkdirAll(mountpoint, 0777); err != nil {
		return err
	}
	fuse.Debug = func(msg interface{}) { log.Println(msg) }
	conn, err := fuse.Mount(
		mountpoint,
		fuse.FSName("ldmfs"),
		fuse.Subtype("ldmfs"),
		fuse.VolumeName("ldmfs"),
		fuse.LocalVolume(),
		fuse.MaxReadahead(1<<32-1),
	)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil && retErr == nil {
			retErr = err
		}
	}()
	log.Printf("Going to serve %s", mountpoint)
	if err := fs.Serve(conn, ldmfs.New(srcPath, dstPath)); err != nil {
		return err
	}
	<-conn.Ready
	return conn.MountError
}
