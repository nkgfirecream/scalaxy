package column

import (
	"runtime"
	"sync/atomic"
)

type Column struct {
	memtable       []int64
	memtableSize   uint64
	memtableOffset uint64
}

func Open(path string, memtableSize int) *Column {
	col := &Column{}
	col.memtableSize = uint64(memtableSize)
	col.memtable = make([]int64, memtableSize)
	return col
}

func (col *Column) WriteInt64(v int64) {
	var index int
	for {
		index = atomic.AddUint64(&col.memtableOffset, 1)
		if index < col.memtableSize {
			break
		} else if index > col.memtableSize {
			runtime.Gosched()
			continue
		} else {
			col.drop()
			index = 0
			break
		}
	}
	col.memtable[index] = v
}

func (col *Column) drop() {
	atomic.StoreUint64(&col.memtableOffset, 0)
}

func (col *Column) Close() {

}

//const (
//	BYTES_IN_INT64 = 8
//)
//
//func UnsafeCaseInt64ToBytes(val int64) []byte {
//	hdr := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&val)), Len: BYTES_IN_INT64, Cap: BYTES_IN_INT64}
//	return *(*[]byte)(unsafe.Pointer(&hdr))
//}
//
//func UnsafeCaseInt64sToBytes(val []int64) []byte {
//	hdr := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&val)), Len: BYTES_IN_INT64, Cap: BYTES_IN_INT64}
//	return *(*[]byte)(unsafe.Pointer(&hdr))
//}
