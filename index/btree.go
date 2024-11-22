package index

import (
	"bytes"
	"sync"
	"github.com/google/btree"
	"github.com/gyy0727/kvdb/wal"
)


//*存储在内存的B树
type MemoryBTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
} 

type item struct{
	key []byte
	pos *wal.ChunkPosition
}



func newBTree() *MemoryBTree {
	return &MemoryBTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}