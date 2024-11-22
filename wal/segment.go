package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/gyy0727/kvdb/bytebufferpool"
)

// *块类型
type ChunkType = byte

// *段ID
type SegmentID = uint32

// *块的类型
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
	chunkHeaderSize = 7                                               //*块的头部大小,每一个记录中装有信息的头部
	blockSize       = 32 * KB                                         //*块的大小
	fileModePerm    = 0644                                            //*文件权限
	maxLen          = binary.MaxVarintLen32*3 + binary.MaxVarintLen64 //*最大大小,25
)

// *表示一个段文件，管理文件的写入操作，包括块的编号和大小
type segment struct {
	id                 SegmentID     //*段ID
	fd                 *os.File      //*对应的文件
	currentBlockNumber uint32        //*当前正在写入的块id
	currentBlockSize   uint32        //*当前的块已经写入到的位置
	closed             bool          //*是否已经关闭
	header             []byte        //*头部数据
	startupBlock       *startupBlock //*启动块，用于初始化时的遍历
	isStartupTraversal bool          //*是否正在进行启动遍历
}

// *用于遍历段文件中的数据，提供逐块读取的功能
type segmentReader struct {
	segment     *segment //*段文件
	blockNumber uint32   //*块编号
	chunkOffset int64    //*块偏移量
}

// *用于启动时遍历的块
type startupBlock struct {
	block       []byte //*块数据
	blockNumber int64  //*块编号
}

// *表示数据块在段文件中的位置，用于精确读取数据
type ChunkPosition struct {
	SegmentId   SegmentID //*所属的段id
	BlockNumber uint32    //*块id
	ChunkOffset int64     //*块偏移量
	ChunkSize   uint32    //*块大小
}

// *对象池,返回一个block,即单条记录
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

// *打开段文件
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

// *新建段文件头部
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

// *将数据写入bufferpool
func (seg *segment) writeToBuffer(data []byte, chunkBuffer *bytebufferpool.ByteBuffer) (*ChunkPosition, error) {
	//*记录初始位置
	startBufferLen := chunkBuffer.Len()
	//*填充数据的大小
	padding := uint32(0)
	if seg.closed {
		return nil, ErrClosed
	}

	//*如果当前块的大小不足以容纳,填充字节,然后写入到下一个块
	if seg.currentBlockSize+chunkHeaderSize >= blockSize {
		if seg.currentBlockSize < blockSize {
			p := make([]byte, blockSize-seg.currentBlockSize)
			chunkBuffer.B = append(chunkBuffer.B, p...)
			padding += blockSize - seg.currentBlockSize
			seg.currentBlockNumber += 1
			seg.currentBlockSize = 0
		}
	}

	//*更改定位
	position := &ChunkPosition{
		SegmentId:   seg.id,
		BlockNumber: seg.currentBlockNumber,
		ChunkOffset: int64(seg.currentBlockSize),
	}

	//*要写入的数据大小
	dataSize := uint32(len(data))

	//*足够容纳,写入当前块
	if seg.currentBlockSize+dataSize+chunkHeaderSize <= blockSize {
		seg.appendChunkBuffer(chunkBuffer, data, ChunkTypeFull)
		position.ChunkSize = dataSize + chunkHeaderSize
	} else {
		var (
			leftSize             = dataSize             //*剩余要写入数据的大小
			blockCount    uint32 = 0                    //*块的个数
			currBlockSize        = seg.currentBlockSize //*当前已写入的大小
		)
		//*循环写入数据
		for leftSize > 0 {
			//*当前块可以写入的大小
			chunkSize := blockSize - currBlockSize - chunkHeaderSize
			if chunkSize > leftSize {
				//*当前可以容纳剩余未写入的数据
				chunkSize = leftSize
			}
			//*单次写入要写入到的位置
			var end = dataSize - leftSize + chunkSize
			if end > dataSize {
				end = dataSize
			}

			//*当前正在写入块的类型
			var chunkType ChunkType
			switch leftSize {
			case dataSize:
				chunkType = ChunkTypeFirst
			case chunkSize:
				chunkType = ChunkTypeLast
			default:
				chunkType = ChunkTypeMiddle
			}

			seg.appendChunkBuffer(chunkBuffer, data[dataSize-leftSize:end], chunkType)
			leftSize -= chunkSize
			blockCount += 1
			currBlockSize = (currBlockSize + chunkSize + chunkHeaderSize) % blockSize
		}
		position.ChunkSize = blockCount*chunkHeaderSize + dataSize
	}

	endBufferLen := chunkBuffer.Len()
	if position.ChunkSize+padding != uint32(endBufferLen-startBufferLen) {
		return nil, fmt.Errorf("wrong!!! the chunk size %d is not equal to the buffer len %d",
			position.ChunkSize+padding, endBufferLen-startBufferLen)
	}

	seg.currentBlockSize += position.ChunkSize
	if seg.currentBlockSize >= blockSize {
		seg.currentBlockNumber += seg.currentBlockSize / blockSize
		seg.currentBlockSize = seg.currentBlockSize % blockSize
	}

	return position, nil
}

func (seg *segment) writeAll(data [][]byte) (positions []*ChunkPosition, err error) {
	if seg.closed {
		return nil, ErrClosed
	}
	//*if something wrong ,restore the segement status
	originBlockNumber := seg.currentBlockNumber
	originBlockSize := seg.currentBlockSize
	//*取出一个[]byte的对象
	chunkBuffer := bytebufferpool.Get()
	//*清空里面的数据
	chunkBuffer.Reset()
	//*如果出错,用于重置segement的状态
	defer func() {
		if err != nil {
			seg.currentBlockNumber = originBlockNumber
			seg.currentBlockSize = originBlockSize

		}
		bytebufferpool.Put(chunkBuffer)
	}()

	var pos *ChunkPosition
	positions = make([]*ChunkPosition, len(data))
	positions = make([]*ChunkPosition, 0, len(data))
	for i := 0; i < len(positions); i++ {
		pos, err = seg.writeToBuffer(data[i], chunkBuffer)
		if err != nil {
			return
		}
		positions[i] = pos
	}
	if err = seg.writeChunkBuffer(chunkBuffer); err != nil {
		return
	}
	return

}

func (seg *segment) Write(data []byte) (pos *ChunkPosition, err error) {
	if seg.closed {
		return nil, ErrClosed
	}

	//*用于在出现错误时回滚
	originBlockNumber := seg.currentBlockNumber
	originBlockSize := seg.currentBlockSize

	chunkBuffer := bytebufferpool.Get()
	chunkBuffer.Reset() //*清空

	//*回滚操作
	defer func() {
		if err != nil {
			seg.currentBlockNumber = originBlockNumber
			seg.currentBlockSize = originBlockSize
		}
		bytebufferpool.Put(chunkBuffer)
	}()

	pos, err = seg.writeToBuffer(data, chunkBuffer)
	if err != nil {
		return
	}

	if err = seg.writeChunkBuffer(chunkBuffer); err != nil {
		return
	}
	return
}

// *主要作用是将data和chunkType以及校验和写入到buf中
func (seg *segment) appendChunkBuffer(buf *bytebufferpool.ByteBuffer, data []byte, chunkType ChunkType) {
	//*将一个16为无符号的整数写入切片的指定位置,小端序
	binary.LittleEndian.PutUint16(seg.header[4:6], uint16(len(data)))
	seg.header[6] = chunkType
	sum := crc32.ChecksumIEEE((seg.header[4:]))
	sum = crc32.Update(sum, crc32.IEEETable, data)
	//*将校验和放到前三个位置
	binary.LittleEndian.PutUint32(seg.header[0:4], sum)
	buf.B = append(buf.B, seg.header...)
	buf.B = append(buf.B, data...)
}

// *将准备好的chunk buffer写入段文件
func (seg *segment) writeChunkBuffer(buf *bytebufferpool.ByteBuffer) error {
	if seg.currentBlockSize > blockSize {
		return errors.New("the current block size is bigger than block size")
	}
	if _, err := seg.fd.Write(buf.Bytes()); err != nil {
		return err
	}
	seg.startupBlock.blockNumber = -1
	return nil
}

func (seg *segment) Read(blockNumber uint32, chunkOffset int64) ([]byte, error) {
	value, _, err := seg.readInternal(blockNumber, chunkOffset)
	return value, err
}

// *要读取的块number和块内偏移
func (seg *segment) readInternal(blockNumber uint32, chunkOffset int64) ([]byte, *ChunkPosition, error) {
	if seg.closed {
		return nil, nil, ErrClosed
	}

	var (
		result    []byte       //*结果
		block     []byte       //*临时块
		segSize   = seg.Size() //*段文件的大小
		nextChunk = &ChunkPosition{
			SegmentId: seg.id,
		}
	)

	if seg.isStartupTraversal {
		block = seg.startupBlock.block
	} else {
		block = getBuffer()
		if len(block) != blockSize {
			block = make([]byte, blockSize)
		}
		defer putBuffer(block)
	}
	for {

		size := int64(blockSize)                 //*块大小
		offset := int64(blockNumber) * blockSize //*文件内的偏移量
		//*如果要读取的位置+一个blocksize超过段文件的大小
		//*|  blockNumber * blockSize + chunkOffset |  == seg.Size
		if size+offset > segSize {
			//*当前size == Chunkoffset
			size = segSize - offset
		}
		//*即要读取的起始位置是段文件的结尾
		if chunkOffset >= size {
			return nil, nil, io.EOF
		}

		if seg.isStartupTraversal {

			if seg.startupBlock.blockNumber != int64(blockNumber) || size != blockSize {

				_, err := seg.fd.ReadAt(block[0:size], offset)
				if err != nil {
					return nil, nil, err
				}

				seg.startupBlock.blockNumber = int64(blockNumber)
			}
		} else {
			if _, err := seg.fd.ReadAt(block[0:size], offset); err != nil {
				return nil, nil, err
			}
		}

		header := block[chunkOffset : chunkOffset+chunkHeaderSize]

		length := binary.LittleEndian.Uint16(header[4:6])

		start := chunkOffset + chunkHeaderSize
		result = append(result, block[start:start+int64(length)]...)

		checksumEnd := chunkOffset + chunkHeaderSize + int64(length)
		checksum := crc32.ChecksumIEEE(block[chunkOffset+4 : checksumEnd])
		savedSum := binary.LittleEndian.Uint32(header[:4])
		if savedSum != checksum {
			return nil, nil, ErrInvalidCRC
		}

		chunkType := header[6]

		if chunkType == ChunkTypeFull || chunkType == ChunkTypeLast {
			nextChunk.BlockNumber = blockNumber
			nextChunk.ChunkOffset = checksumEnd

			if checksumEnd+chunkHeaderSize >= blockSize {
				nextChunk.BlockNumber += 1
				nextChunk.ChunkOffset = 0
			}
			break
		}
		blockNumber += 1
		chunkOffset = 0
	}

	return result, nextChunk, nil

}

// *返回下一个块
func (segReader *segmentReader) Next() ([]byte, *ChunkPosition, error) {
	//*段文件已经关闭
	if segReader.segment.closed {
		return nil, nil, ErrClosed
	}

	//*描述当前段文件所在的位置
	chunkPosition := &ChunkPosition{
		SegmentId:   segReader.segment.id,
		BlockNumber: segReader.blockNumber,
		ChunkOffset: segReader.chunkOffset,
	}
	//*读取数据
	value, nextChunk, err := segReader.segment.readInternal(
		segReader.blockNumber,
		segReader.chunkOffset,
	)
	if err != nil {
		return nil, nil, err
	}

	chunkPosition.ChunkSize =
		nextChunk.BlockNumber*blockSize + uint32(nextChunk.ChunkOffset) -
			(segReader.blockNumber*blockSize + uint32(segReader.chunkOffset))

	//*更新当前位置
	segReader.blockNumber = nextChunk.BlockNumber
	segReader.chunkOffset = nextChunk.ChunkOffset

	return value, chunkPosition, nil
}

// *仅返回实际使用部分
func (cp *ChunkPosition) Encode() []byte {
	return cp.encode(true)
}

// *返回完整的编码字节数组
func (cp *ChunkPosition) EncodeFixedSize() []byte {
	return cp.encode(false)
}

// *将chunkposition编码成字节数组
// *shrink 返回整个字节数组or实际使用的部分
func (cp *ChunkPosition) encode(shrink bool) []byte {
	buf := make([]byte, maxLen)

	var index = 0
	index += binary.PutUvarint(buf[index:], uint64(cp.SegmentId))
	index += binary.PutUvarint(buf[index:], uint64(cp.BlockNumber))
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkOffset))
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkSize))
	if shrink {
		return buf[:index]
	}
	return buf
}

// *从字节数组中解码出position
func DecodeChunkPosition(buf []byte) *ChunkPosition {
	if len(buf) == 0 {
		return nil
	}
	var index = 0
	segmentId, n := binary.Uvarint(buf[index:])
	index += n

	blockNumber, n := binary.Uvarint(buf[index:])
	index += n

	chunkOffset, n := binary.Uvarint(buf[index:])
	index += n
	chunkSize, n := binary.Uvarint(buf[index:])
	index += n

	return &ChunkPosition{
		SegmentId:   uint32(segmentId),
		BlockNumber: uint32(blockNumber),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}
