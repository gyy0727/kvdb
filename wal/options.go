package wal

import "os"

type Options struct {
	DirPath        string //*路径
	SegmentSize    int64  //*段大小
	SegmentFileExt string //*文件后缀
	Sync           bool   //*是否同步
	BytesPerSync   uint32 //*每多少字节同步
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

var DefaultOptions = Options{
	DirPath:        os.TempDir(), //*TempDir返回一个用于保管临时文件的默认目录。
	SegmentSize:    GB,           //*1G
	SegmentFileExt: ".SEG",       //*后缀
	Sync:           false,        //*是否已经刷新数据到磁盘
	BytesPerSync:   0,            //*每多少字节刷新一次
}
