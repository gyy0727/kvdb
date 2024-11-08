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


type ChunkType = byte
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
