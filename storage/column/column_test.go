package column

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkRand(b *testing.B) {
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			r.Int()
		}
	})
}

func BenchmarkStupidAddUint64(b *testing.B) {
	var counter uint64
	for i := 0; i < b.N; i++ {
		counter++
	}
}

func BenchmarkAtomicAddUint64(b *testing.B) {
	var counter uint64
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			atomic.AddUint64(&counter, 1)
		}
	})
}

func BenchmarkAtomicLoadUint64(b *testing.B) {
	var counter uint64
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var a uint64
		for pb.Next() {
			a = atomic.LoadUint64(&counter)
		}
		a = a
	})
}

func BenchmarkAtomicStoreUint64(b *testing.B) {
	var counter uint64
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			atomic.StoreUint64(&counter, 1)
		}
	})
}

func BenchmarkMutexAddUint64(b *testing.B) {
	var counter uint64
	var mutex sync.Mutex
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mutex.Lock()
			counter++
			mutex.Unlock()
		}
	})
}

func BenchmarkColumnInsert(b *testing.B) {
	var randInts = generateInt64(500000)
	column := Open("column_int64", 100000, 1024)
	defer column.Close()
	b.SetBytes(8)
	cc := 8
	b.SetParallelism(cc)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			column.Write(randInts[counter])
			counter++
			if counter == cc {
				counter = 0
			}
		}
	})
}

func generateInt64(n int) []int64 {
	d := make([]int64, n)
	for i := 0; i < n; i++ {
		d[i] = rand.Int63()
	}
	return d
}
