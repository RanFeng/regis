package utils

import (
	log "code/regis/lib"
	"strconv"
	"testing"
)

// BenchmarkGetRandomHexChars-12    	12053670	       101.1 ns/op
func BenchmarkGetRandomHexChars(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = GetRandomHexChars(10)
	}
}

func TestGetRandomHexChars(t *testing.T) {
	for i := 0; i < 100; i++ {
		log.Info("%v", GetRandomHexChars(10))
	}
}

var (
	mapStr = map[string]string{}
	mapInt = map[int64]string{}
)

// BenchmarkMapString-12                    4393306               324.1 ns/op
// BenchmarkMapInt-12                       5336557               242.3 ns/op
func BenchmarkMapString(b *testing.B) {
	var i int64
	for i = 0; i < int64(b.N); i++ {
		a := strconv.FormatInt(i, 10)
		mapStr[a] = a
	}
}

func BenchmarkMapInt(b *testing.B) {
	var i int64
	for i = 0; i < int64(b.N); i++ {
		a := strconv.FormatInt(i, 10)
		mapInt[i] = a
	}
}
