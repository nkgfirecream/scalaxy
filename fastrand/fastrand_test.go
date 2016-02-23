package fastrand

import (
	"testing"
)

func BenchmarkMod(b *testing.B) {
	n1 := 1234231
	n2 := 35
	n3 := 0
	for i := 0; i < b.N; i++ {
		n3 = n1 % n2
	}
	n3 = n3
}

func BenchmarkFastRand(b *testing.B) {
	for i := 0; i < b.N; i++ {
		FastRand(64)
	}
}
