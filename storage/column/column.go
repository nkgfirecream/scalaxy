package column

import (
	"bytes"
	"github.com/scalaxy/scalaxy/fastrand"
	"io"
	"log"
	"os"
	"reflect"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"
)

type Column struct {
	memtables       [][]int64
	memtableSize    uint64
	memtableIndexes []uint64
	shards          int
	shardCurrent    uint64
	diskCounter     uint64
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
	data := UnsafeCastUint64sToBytes(col.memtables[shardId])
	counter := atomic.AddUint64(&col.diskCounter, 1)
	defer func(t time.Time, counter uint64) {
		ts := time.Since(t).Seconds()
		log.Printf("shard %d dropped by %.2fs. and %.2fMB/s", counter, ts, float64(len(data))/ts/1024/1024)
	}(time.Now(), counter)
	f, err := os.OpenFile("batch_"+strconv.FormatUint(counter, 10)+".dat", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		log.Printf("store shard on disk err: %s", err)
		return
	}
	defer f.Close()

	_, err = io.Copy(f, bytes.NewBuffer(data))
	if err != nil {
		log.Printf("store shard on disk err: %s", err)
		return
	}
}

func (col *Column) Close() {

}

const (
	BYTES_IN_INT64 = 8
)

func UnsafeCastUint64sToBytes(ints []int64) []byte {
	length := len(ints) * BYTES_IN_INT64
	hdr := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&ints[0])), Len: length, Cap: length}
	return *(*[]byte)(unsafe.Pointer(&hdr))
}

//func UnsafeCaseInt64ToBytes(val int64) []byte {
//	hdr := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&val)), Len: BYTES_IN_INT64, Cap: BYTES_IN_INT64}
//	return *(*[]byte)(unsafe.Pointer(&hdr))
//}
