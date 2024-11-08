package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// *块类型
type ChunkType = byte

// *段ID
type SegmentID = uint32

const (
	ChunkTypeFull ChunkType = iota
	ChunkTypeFirst
	ChunkTypeMiddle
	ChunkTypeLast
)

var (
	ErrClosed     = errors.New("the segment file is closed")
	ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")
)

const (
	chunkHeaderSize = 7
	blockSize       = 32 * KB
	fileModePerm    = 0644
	maxLen          = binary.MaxVarintLen32*3 + binary.MaxVarintLen64
)

// *表示一个段文件，管理文件的写入操作，包括块的编号和大小
type segment struct {
	id                 SegmentID //*段ID
	fd                 *os.File  //*对应的文件
	currentBlockNumber uint32    //*当前正在写入的块id
	currentBlockSize   uint32    //*当前的块已经写入到的位置
	closed             bool      //*是否已经关闭
	header             []byte
	startupBlock       *startupBlock
	isStartupTraversal bool
}

// *用于遍历段文件中的数据，提供逐块读取的功能
type segmentReader struct {
	segment     *segment
	blockNumber uint32
	chunkOffset int64
}

// *用于启动时遍历的块
type startupBlock struct {
	block       []byte
	blockNumber int64
}

// *表示数据块在段文件中的位置，用于精确读取数据
type ChunkPosition struct {
	SegmentId   SegmentID
	BlockNumber uint32
	ChunkOffset int64
	ChunkSize   uint32
}

// *对象池
var blockPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, blockSize)
	},
}

// *向对象池取对象
func getBuffer() []byte {
	return blockPool.Get().([]byte)
}

// *向对象池存对象
func putBuffer(buf []byte) {
	blockPool.Put(buf)
}

func openSegementFile(dirPath, extName string, id uint32) (*segment, error) {
	fd, err := os.OpenFile(SegmentFileName(dirPath, extName, id), os.O_CREATE|os.O_RDWR|os.O_APPEND, fileModePerm)
	if err != nil {
		return nil, err
	}
	//*将指针移到文件末尾并返回当前位置
	offset, err := fd.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("seek to the end of segment file  %d%s failed: %v", id, extName, err)
	}

	return &segment{
		id:                 id,
		fd:                 fd,
		header:             make([]byte, chunkHeaderSize),
		currentBlockNumber: uint32(offset / blockSize),
		currentBlockSize:   uint32(offset % blockSize),
		startupBlock: &startupBlock{
			block:       make([]byte, blockSize),
			blockNumber: -1,
		},
		isStartupTraversal: false,
	}, nil

}

func (seg *segment) NewReader() *segmentReader {
	return &segmentReader{
		segment:     seg,
		blockNumber: 0,
		chunkOffset: 0,
	}
}

// *同步文件的缓冲区内容到磁盘,确保数据写入到磁盘
func (seg *segment) Sync() error {
	if seg.closed {
		return nil
	}
	return seg.fd.Sync()
}

// *删除段文件
func (seg *segment) Remove() error {
	if !seg.closed {
		seg.closed = true
		if err := seg.fd.Close(); err != nil {
			return err
		}
	}
	return os.Remove(seg.fd.Name())
}

// *关闭段文件,停止读写
func (seg *segment) Close() error {
	if seg.closed {
		return nil
	}
	seg.closed = true
	return seg.fd.Close()
}

// *返回当前段文件已经写入的数据大小
func (seg *segment) Size() int64 {
	size := int64(seg.currentBlockNumber) * int64(blockSize)
	return size + int64(seg.currentBlockSize)
}
