package column

import (
	"github.com/scalaxy/scalaxy/fastrand"
	"reflect"
	"sync/atomic"
	"unsafe"
)

type Column struct {
	memtables       [][]int64
	memtableSize    uint64
	memtableIndexes []uint64
	shards          int
	shardCurrent    uint64
}

func Open(path string, memtableSize, shards int) *Column {
	col := &Column{}
	col.shards = shards
	col.memtableSize = uint64(memtableSize)
	col.memtableIndexes = make([]uint64, shards)
	col.memtables = make([][]int64, shards)
	for i := 0; i < shards; i++ {
		col.memtables[i] = make([]int64, memtableSize)
	}
	return col
}

func (col *Column) Write(v int64) {
	shardId := fastrand.FastRand(col.shards)
	var index uint64
	for {
		index = atomic.AddUint64(&col.memtableIndexes[shardId], 1)
		if index < col.memtableSize {
			break
		} else if index > col.memtableSize {
			continue
		} else {
			col.drop(shardId)
			atomic.StoreUint64(&col.memtableIndexes[shardId], 0)
			index = 0
			break
		}
	}
	col.memtables[shardId][index] = v
}

func (col *Column) drop(shardId int) {
}

func (col *Column) Close() {

}

const (
	BYTES_IN_INT64 = 8
)

func UnsafeCaseInt64sToBytes(val []int64) []byte {
	hdr := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&val)), Len: BYTES_IN_INT64, Cap: BYTES_IN_INT64}
	return *(*[]byte)(unsafe.Pointer(&hdr))
}

//func UnsafeCaseInt64ToBytes(val int64) []byte {
//	hdr := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&val)), Len: BYTES_IN_INT64, Cap: BYTES_IN_INT64}
//	return *(*[]byte)(unsafe.Pointer(&hdr))
//}
