package utils

import (
	log "code/regis/lib"
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
