package devosender

import (
	"math/rand"
	"testing"
	"time"
)

const tag = "test.keep.free"

func init() {
	rand.Seed(time.Now().UnixNano())
}

func BenchmarkTestLazyClient_SendWTagAsync_standby(b *testing.B) {
	// Create client
	lc, err := NewLazyClientBuilder().
		ClientBuilder(
			NewClientBuilder().EntryPoint("udp://localhost:13000")).
		Build()
	if err != nil {
		panic(err)
	}
	err = lc.StandBy()
	if err != nil {
		panic(err)
	}
	defer lc.Close()

	tests := []struct {
		name    string
		msgSize int
	}{
		{
			"Msg of 256 bytes",
			256,
		},
		{
			"Msg of 512 bytes",
			512,
		},
		{
			"Msg of 1024 bytes",
			1024,
		},
		{
			"Msg of 2048 bytes",
			2048,
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				lc.SendWTagAsync(tag, getRandomMsg(tt.msgSize))
			}
			err := lc.Close()
			if err != nil {
				panic(err)
			}
		})
	}
}

func BenchmarkTestLazyClient_SendWTagAndCompressorAsync_standby(b *testing.B) {
	// Create client
	lc, err := NewLazyClientBuilder().
		ClientBuilder(
			NewClientBuilder().EntryPoint("udp://localhost:13000")).
		Build()
	if err != nil {
		panic(err)
	}
	err = lc.StandBy()
	if err != nil {
		panic(err)
	}
	defer lc.Close()

	tests := []struct {
		name       string
		msgSize    int
		compressor *Compressor
	}{
		{
			"Msg of 256 bytes",
			256,
			&Compressor{
				Algorithm: CompressorGzip,
			},
		},
		{
			"Msg of 512 bytes",
			512,
			&Compressor{
				Algorithm: CompressorGzip,
			},
		},
		{
			"Msg of 1024 bytes",
			1024,
			&Compressor{
				Algorithm: CompressorGzip,
			},
		},
		{
			"Msg of 2048 bytes",
			2048,
			&Compressor{
				Algorithm: CompressorGzip,
			},
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				lc.SendWTagAndCompressorAsync(tag, getRandomMsg(tt.msgSize), tt.compressor)
			}
			err := lc.Close()
			if err != nil {
				panic(err)
			}
		})
	}
}

func BenchmarkTestLazyClient_SendWTagAsync_udp(b *testing.B) {
	// Create client
	lc, err := NewLazyClientBuilder().
		ClientBuilder(
			NewClientBuilder().EntryPoint("udp://localhost:13000")).
		Build()
	if err != nil {
		panic(err)
	}
	defer lc.Close()

	tests := []struct {
		name    string
		msgSize int
	}{
		{
			"Msg of 256 bytes",
			256,
		},
		{
			"Msg of 512 bytes",
			512,
		},
		{
			"Msg of 1024 bytes",
			1024,
		},
		{
			"Msg of 2048 bytes",
			2048,
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				lc.SendWTagAsync(tag, getRandomMsg(tt.msgSize))
			}
			err := lc.Close()
			if err != nil {
				panic(err)
			}
		})
	}
}

func BenchmarkTestLazyClient_SendWTagAndCompressorAsync_udp(b *testing.B) {
	// Create client
	lc, err := NewLazyClientBuilder().
		ClientBuilder(
			NewClientBuilder().EntryPoint("udp://localhost:13000")).
		Build()
	if err != nil {
		panic(err)
	}
	defer lc.Close()

	tests := []struct {
		name       string
		msgSize    int
		compressor *Compressor
	}{
		{
			"Msg of 256 bytes",
			256,
			&Compressor{
				Algorithm: CompressorGzip,
			},
		},
		{
			"Msg of 512 bytes",
			512,
			&Compressor{
				Algorithm: CompressorGzip,
			},
		},
		{
			"Msg of 1024 bytes",
			1024,
			&Compressor{
				Algorithm: CompressorGzip,
			},
		},
		{
			"Msg of 2048 bytes",
			2048,
			&Compressor{
				Algorithm: CompressorGzip,
			},
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				lc.SendWTagAndCompressorAsync(tag, getRandomMsg(tt.msgSize), tt.compressor)
			}
			err := lc.Close()
			if err != nil {
				panic(err)
			}
		})
	}

	lc.Close()
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func getRandomMsg(size int) string {
	b := make([]rune, size)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
