package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	initialSgementFileID = 1
)

var (
	//* 表示要写入的数据大小超过了段文件的最大允许大小
	ErrValueTooLarge = errors.New("the data size can't larger than segment size")
	//*表示待写入数据的总大小超过了段文件的最大允许大小
	ErrPendingSizeTooLarge = errors.New("the upper bound of pendingWrites can't larger than segment size")
)

type WAL struct {
	activeSegment     *segment               //*对应的段文件activeSgement
	olderSegments     map[SegmentID]*segment //* 保存了所有的段文件
	options           Options                //* 配置选项
	mu                sync.RWMutex           //*读写锁
	bytesWrite        uint32
	renameIds         []SegmentID
	pendingWrites     [][]byte   //*待写入数据
	pendingSize       int64      //*待写入数据大小
	pendingWritesLock sync.Mutex //*预写入数据互斥锁
}

// *标识读取操作所需的信息
type Reader struct {
	segmentReaders []*segmentReader
	currentReader  int
}

func Open(options Options) (*WAL, error) {
	//*检查是否以"."开头
	if !strings.HasPrefix(options.SegmentFileExt, ".") {
		return nil, fmt.Errorf("the segment file ext must start with '.'")
	}

	wal := &WAL{
		options:       options,
		olderSegments: make(map[SegmentID]*segment),
		pendingWrites: make([][]byte, 0),
	}

	//*如果不存在那就创建文件路径
	if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
		return nil, err
	}

	//*循环打开所有段文件,entries是目录下的所有文件
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}

	//*获取所有段文件的文件id
	var segmentIDs []int
	for _, entry := range entries {
		//*如果是目录,不是文件
		if entry.IsDir() {
			continue
		}
		var id int
		//*从文件名中解析出段文件id
		_, err := fmt.Sscanf(entry.Name(), "%d"+options.SegmentFileExt, &id)
		if err != nil {
			//*解析出错
			continue
		}
		//*将解析出的id存入数组
		segmentIDs = append(segmentIDs, id)

	}

	//*没有任何段文件,初始化一个段文件
	if len(segmentIDs) == 0 {
		segment, err := openSegementFile(options.DirPath, options.SegmentFileExt, initialSgementFileID)
		if err != nil {
			return nil, err
		}
		wal.activeSegment = segment
	} else {
		//*按顺序打开段文件,用id最大的作为activeSegment
		sort.Ints(segmentIDs)
		for i, segId := range segmentIDs {
			segment, err := openSegementFile(options.DirPath, options.SegmentFileExt, SegmentID(segId))
			if err != nil {
				return nil, err
			}
			if i == len(segmentIDs)-1 {
				wal.activeSegment = segment
			} else {
				wal.olderSegments[segment.id] = segment
			}
		}
	}
	return wal, nil
}

// *根据给定的目录路径（dirPath）、文件扩展名（extName）和段ID（id）来构建一个文件名
func SegmentFileName(dirPath string, extName string, id SegmentID) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d"+extName, id))
}

// *打开一个新的段文件,然后把他设置为活跃的段文件
// *用于activeSegment 未满,但是user想使用新的段文件
func (wal *WAL) OpenNewActiveSegment() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	//*先将数据刷新到磁盘
	if err := wal.activeSegment.Sync(); err != nil {
		return err
	}

	//*创建新的activeSegment
	segment, err := openSegementFile(wal.options.DirPath, wal.options.SegmentFileExt, wal.activeSegment.id+1)
	if err != nil {
		return err
	}
	wal.olderSegments[wal.activeSegment.id] = wal.activeSegment
	wal.activeSegment = segment
	return nil
}

// *返回activeSegment的id
func (wal *WAL) ActiveSegmentID() SegmentID {
	wal.mu.RLock()
	defer wal.mu.RUnlock()
	return wal.activeSegment.id
}

// *返回wal是否为空
func (wal *WAL) IsEmpty() bool {
	wal.mu.RLock()
	defer wal.mu.RUnlock()
	return len(wal.olderSegments) == 0 && wal.activeSegment.Size() == 0
}

// *仅在WAL处于启动遍历期间时使用
func (wal *WAL) SetIsStartupTraversal(v bool) {
	for _, seg := range wal.olderSegments {
		seg.isStartupTraversal = v
	}
	wal.activeSegment.isStartupTraversal = v
}

// *获取小于segid的reader
func (wal *WAL) NewReaderWithMax(segId SegmentID) *Reader {
	wal.mu.RLock()
	defer wal.mu.Lock()

	//*所有读取实例
	var segmentReaders []*segmentReader
	for _, segment := range wal.olderSegments {
		if segId == 0 || segment.id <= segId {
			reader := segment.NewReader()
			segmentReaders = append(segmentReaders, reader)
		}
	}

	if segId == 0 || wal.activeSegment.id <= segId {
		reader := wal.activeSegment.NewReader()
		segmentReaders = append(segmentReaders, reader)

	}
	//*根据id排序reader
	sort.Slice(segmentReaders, func(i, j int) bool {
		return segmentReaders[i].segment.id < segmentReaders[j].segment.id
	})

	return &Reader{
		segmentReaders: segmentReaders,
		currentReader:  0,
	}
}

// *获取大于startPos的
func (wal *WAL) NewReaderWithStart(startPos *ChunkPosition) (*Reader, error) {
	if startPos == nil {
		return nil, errors.New("start position is nil")
	}
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	reader := wal.NewReader()
	for {

		if reader.CurrentSegmentId() < startPos.SegmentId {
			reader.SkipCurrentSegment()
			continue
		}
		currentPos := reader.CurrentChunkPosition()
		if currentPos.BlockNumber >= startPos.BlockNumber &&
			currentPos.ChunkOffset >= startPos.ChunkOffset {
			break
		}
		// call Next to find again.
		if _, _, err := reader.Next(); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}
	return reader, nil
}

func (wal *WAL) NewReader() *Reader {
	return wal.NewReaderWithMax(0)
}

// *返回下一个block
func (r *Reader) Next() ([]byte, *ChunkPosition, error) {
	//*已经到末尾
	if r.currentReader >= len(r.segmentReaders) {
		return nil, nil, io.EOF
	}
	data, position, err := r.segmentReaders[r.currentReader].Next()
	if err == io.EOF {
		r.currentReader++
		return r.Next()
	}
	return data, position, err
}

// *跳过当前的reader
func (r *Reader) SkipCurrentSegment() {
	r.currentReader++
}

// *返回当前的segmentid
func (r *Reader) CurrentSegmentId() SegmentID {
	return r.segmentReaders[r.currentReader].segment.id
}

// *当前reader的position
func (r *Reader) CurrentChunkPosition() *ChunkPosition {
	reader := r.segmentReaders[r.currentReader]
	return &ChunkPosition{
		SegmentId:   reader.segment.id,
		BlockNumber: reader.blockNumber,
		ChunkOffset: reader.chunkOffset,
	}
}

// *清理预写入缓冲区
func (wal *WAL) ClearPendingWrites() {
	wal.pendingWritesLock.Lock()
	defer wal.pendingWritesLock.Unlock()

	wal.pendingSize = 0
	wal.pendingWrites = wal.pendingWrites[:0]
}

// *将数据写入到预写入区,等待写入
func (wal *WAL) PendingWrites(data []byte) {
	wal.pendingWritesLock.Lock()
	defer wal.pendingWritesLock.Unlock()

	size := wal.maxDataWriteSize(int64(len(data)))
	wal.pendingSize += size
	wal.pendingWrites = append(wal.pendingWrites, data)
}

// *rotateActiveSegment创建一个新的段文件并替换activeSegment
func (wal *WAL) rotateActiveSegment() error {
	if err := wal.activeSegment.Sync(); err != nil {
		return err
	}
	wal.bytesWrite = 0
	segment, err := openSegementFile(wal.options.DirPath, wal.options.SegmentFileExt, wal.activeSegment.id+1)
	if err != nil {
		return err
	}
	wal.olderSegments[wal.activeSegment.id] = wal.activeSegment
	wal.activeSegment = segment
	return nil

}

//*WriteAll写入wal.pending写入wal，然后清除pending写入，
//*它不会根据wal.options同步段文件，您应该手动调用sync（）

func (wal *WAL) WriteAll() ([]*ChunkPosition, error) {

	if len(wal.pendingWrites) == 0 {
		return make([]*ChunkPosition, 0), nil
	}

	wal.mu.Lock()
	defer func() {
		wal.ClearPendingWrites()
		wal.mu.Unlock()
	}()

	//*预写入的数据要比段文件的大小大
	if wal.pendingSize > wal.options.SegmentSize {
		return nil, ErrPendingSizeTooLarge
	}

	if wal.activeSegment.Size()+wal.pendingSize > wal.options.SegmentSize {
		//*先刷新到磁盘,然后新建一个segement
		if err := wal.rotateActiveSegment(); err != nil {
			return nil, err
		}
	}
	positions, err := wal.activeSegment.writeAll(wal.pendingWrites)
	if err != nil {
		return nil, err
	}

	return positions, nil

}

// *Write将数据写入WAL
// *实际上，它将数据写入活动段文件
// *它返回数据在WAL中的位置，如果有错误，则返回错误
func (wal *WAL) Write(data []byte) (*ChunkPosition, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	//*数据过大
	if int64(len(data))+chunkHeaderSize > wal.options.SegmentSize {
		return nil, ErrValueTooLarge
	}
	//*当前的段文件能否存下
	if wal.isFull(int64(len(data))) {
		if err := wal.rotateActiveSegment(); err != nil {
			return nil, err
		}
	}
	//*写入活跃段文件
	position, err := wal.activeSegment.Write(data)
	if err != nil {
		return nil, err
	}

	//*写入了多少字节
	wal.bytesWrite += position.ChunkSize

	var needSync = wal.options.Sync
	if !needSync && wal.options.BytesPerSync > 0 {
		needSync = wal.bytesWrite >= wal.options.BytesPerSync
	}
	if needSync {
		if err := wal.activeSegment.Sync(); err != nil {
			return nil, err
		}
		wal.bytesWrite = 0
	}

	return position, nil
}

// *读取数据
func (wal *WAL) Read(pos *ChunkPosition) ([]byte, error) {

	wal.mu.RLock()
	defer wal.mu.RUnlock()
	var segment *segment
	if pos.SegmentId == wal.activeSegment.id {
		segment = wal.activeSegment
	} else {
		segment = wal.olderSegments[pos.SegmentId]
	}

	if segment == nil {
		return nil, fmt.Errorf("segment file %d%s not found", pos.SegmentId, wal.options.SegmentFileExt)
	}

	return segment.Read(pos.BlockNumber, pos.ChunkOffset)
}

// *关闭wal
func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	for _, segment := range wal.olderSegments {
		if err := segment.Close(); err != nil {
			return err
		}
		wal.renameIds = append(wal.renameIds, segment.id)
	}
	wal.olderSegments = nil

	wal.renameIds = append(wal.renameIds, wal.activeSegment.id)

	return wal.activeSegment.Close()
}

// *删除所有的segment
func (wal *WAL) Delete() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	for _, segment := range wal.olderSegments {
		if err := segment.Remove(); err != nil {
			return err
		}
	}
	wal.olderSegments = nil

	return wal.activeSegment.Remove()
}

// *同步activeSegment
func (wal *WAL) Sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.activeSegment.Sync()
}

// *重命名所有段文件的扩展名
func (wal *WAL) RenameFileExt(ext string) error {
	if !strings.HasPrefix(ext, ".") {
		return fmt.Errorf("segment file extension must start with '.'")
	}
	wal.mu.Lock()
	defer wal.mu.Unlock()

	renameFile := func(id SegmentID) error {
		oldName := SegmentFileName(wal.options.DirPath, wal.options.SegmentFileExt, id)
		newName := SegmentFileName(wal.options.DirPath, ext, id)
		return os.Rename(oldName, newName)
	}

	for _, id := range wal.renameIds {
		if err := renameFile(id); err != nil {
			return err
		}
	}

	wal.options.SegmentFileExt = ext
	return nil
}

// *wal已满
func (wal *WAL) isFull(delta int64) bool {
	return wal.activeSegment.Size()+wal.maxDataWriteSize(delta) > wal.options.SegmentSize
}

// *maxDataWriteSize计算可能的最大大小。
// *最大大小=最大填充+（num_block+1）*headerSize+数据大小
func (wal *WAL) maxDataWriteSize(size int64) int64 {
	return chunkHeaderSize + size + (size/blockSize+1)*chunkHeaderSize
}
