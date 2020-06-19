# Live data move FS

This is a **proof of concept**. If you use this code in production, please send me videos of your chat with your manager after it ate all your data.

This is a FUSE filesystem that moves your data from one directory to another while you're using it. This can be used to migrate to another disk without incurring downtime for the full duration of the copy.

Usage

```shell
$ go run cmd/ldmfs/main.go /path/to/src /path/to/dst /path/to/mnt
$ find /path/to/mnt -type f -exec cat {} +
```

Everything read from the filesystem is read from the source, and written to the destination directory. Once all bytes of a file have been copied, the original is deleted (thus marking the completion of the copy, and making the destination the source of truth).

This project should be extended with some background threads that slowly read data and copy it to the destination.
