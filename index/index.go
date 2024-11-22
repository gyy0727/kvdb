package index

import "github.com/gyy0727/kvdb/wal"

// *Indexer是一个用于索引键和位置的接口。
// *它用于在WAL中存储密钥和数据的位置。
// *打开数据库时将重建索引。
// *您可以通过实现此接口来实现自己的索引器。
type Indexer interface {
	//* 将键和位置插入索引中。
	Put(key []byte, position *wal.ChunkPosition) *wal.ChunkPosition

	//* 获取索引中键的位置。
	Get(key []byte) *wal.ChunkPosition

	//* 删除键的索引。
	Delete(key []byte) (*wal.ChunkPosition, bool)

	//* Size 表示索引中键的数量。
	Size() int

	//* Ascend 按升序遍历索引中的项，并对每一项调用处理函数。
	//* 如果处理函数返回 false，则停止遍历。
	Ascend(handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))

	//* AscendRange 在[startKey, endKey]范围内按升序遍历索引中的项，并调用 handleFn。
	//* 如果处理函数返回 false，则停止遍历。
	AscendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))

	//* AscendGreaterOrEqual 从键 >= 给定键的位置开始按升序遍历索引中的项，
	//* 并调用 handleFn。 如果处理函数返回 false，则停止遍历。
	AscendGreaterOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))

	//* Descend 按降序遍历索引中的项，并对每一项调用处理函数。
	//* 如果处理函数返回 false，则停止遍历。
	Descend(handleFn func(key []byte, pos *wal.ChunkPosition) (bool, error))

	//* DescendRange 在[startKey, endKey]范围内按降序遍历索引中的项，并调用 handleFn。
	//* 如果处理函数返回 false，则停止遍历。
	DescendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))

	//* DescendLessOrEqual 从键 <= 给定键的位置开始按降序遍历索引中的项，
	//* 并调用 handleFn。 如果处理函数返回 false，则停止遍历。
	DescendLessOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))
}


type IndexerType = byte

const (
	BTree IndexerType = iota
)

var indexType = BTree

func NewIndexer() Indexer {
	switch indexType {
	case BTree:
		return newBTree()
	default:
		panic("unexpected index type")
	}
}