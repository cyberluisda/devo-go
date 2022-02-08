package devosender

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cyberluisda/devo-go/devosender/compressor"
	"github.com/cyberluisda/devo-go/devosender/status"
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
		compressor *compressor.Compressor
	}{
		{
			"Msg of 256 bytes",
			256,
			&compressor.Compressor{
				Algorithm: compressor.CompressorGzip,
			},
		},
		{
			"Msg of 512 bytes",
			512,
			&compressor.Compressor{
				Algorithm: compressor.CompressorGzip,
			},
		},
		{
			"Msg of 1024 bytes",
			1024,
			&compressor.Compressor{
				Algorithm: compressor.CompressorGzip,
			},
		},
		{
			"Msg of 2048 bytes",
			2048,
			&compressor.Compressor{
				Algorithm: compressor.CompressorGzip,
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
		compressor *compressor.Compressor
	}{
		{
			"Msg of 256 bytes",
			256,
			&compressor.Compressor{
				Algorithm: compressor.CompressorGzip,
			},
		},
		{
			"Msg of 512 bytes",
			512,
			&compressor.Compressor{
				Algorithm: compressor.CompressorGzip,
			},
		},
		{
			"Msg of 1024 bytes",
			1024,
			&compressor.Compressor{
				Algorithm: compressor.CompressorGzip,
			},
		},
		{
			"Msg of 2048 bytes",
			2048,
			&compressor.Compressor{
				Algorithm: compressor.CompressorGzip,
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

func BenchmarkTestReliableClient_SendWTagAsync_standby(b *testing.B) {

	// Ensure status path does not exist
	os.RemoveAll("/tmp/devo-sender-reliable-client-benchmar")

	// Create client
	lc, err := NewReliableClientBuilder().
		StatusBuilder(
			status.NewNutsDBStatusBuilder().DbPath("/tmp/devo-sender-reliable-client-benchmar")).
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
	defer func() {
		lc.Close()
		// Clean status path
		os.RemoveAll("/tmp/devo-sender-reliable-client-benchmar")
	}()

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
		})
	}
}

func BenchmarkTestReliableClient_SendWTagAndCompressorAsync_standby(b *testing.B) {
	// Ensure status path does not exist
	os.RemoveAll("/tmp/devo-sender-reliable-client-benchmar")

	// Create client
	lc, err := NewReliableClientBuilder().
		StatusBuilder(
			status.NewNutsDBStatusBuilder().DbPath("/tmp/devo-sender-reliable-client-benchmar")).
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
	defer func() {
		lc.Close()
		// Clean status path
		os.RemoveAll("/tmp/devo-sender-reliable-client-benchmar")
	}()

	tests := []struct {
		name       string
		msgSize    int
		compressor *compressor.Compressor
	}{
		{
			"Msg of 256 bytes",
			256,
			&compressor.Compressor{
				Algorithm: compressor.CompressorGzip,
			},
		},
		{
			"Msg of 512 bytes",
			512,
			&compressor.Compressor{
				Algorithm: compressor.CompressorGzip,
			},
		},
		{
			"Msg of 1024 bytes",
			1024,
			&compressor.Compressor{
				Algorithm: compressor.CompressorGzip,
			},
		},
		{
			"Msg of 2048 bytes",
			2048,
			&compressor.Compressor{
				Algorithm: compressor.CompressorGzip,
			},
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				lc.SendWTagAndCompressorAsync(tag, getRandomMsg(tt.msgSize), tt.compressor)
			}
		})
	}
}

func BenchmarkTestReliableClient_SendWTagAsync_udp(b *testing.B) {
	// Ensure status path does not exist
	os.RemoveAll("/tmp/devo-sender-reliable-client-benchmar")

	// Create client
	lc, err := NewReliableClientBuilder().
		StatusBuilder(
			status.NewNutsDBStatusBuilder().DbPath("/tmp/devo-sender-reliable-client-benchmar")).
		ClientBuilder(
			NewClientBuilder().EntryPoint("udp://localhost:13000")).
		Build()
	if err != nil {
		panic(err)
	}
	defer func() {
		lc.Close()
		// Clean status path
		os.RemoveAll("/tmp/devo-sender-reliable-client-benchmar")
	}()

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
		})
	}
}

func BenchmarkTestReliableClient_SendWTagAndCompressorAsync_udp(b *testing.B) {
	// Ensure status path does not exist
	os.RemoveAll("/tmp/devo-sender-reliable-client-benchmar")

	// Create client
	lc, err := NewReliableClientBuilder().
		StatusBuilder(
			status.NewNutsDBStatusBuilder().DbPath("/tmp/devo-sender-reliable-client-benchmar")).
		ClientBuilder(
			NewClientBuilder().EntryPoint("udp://localhost:13000")).
		Build()
	if err != nil {
		panic(err)
	}
	defer func() {
		lc.Close()
		// Clean status path
		os.RemoveAll("/tmp/devo-sender-reliable-client-benchmar")
	}()

	tests := []struct {
		name       string
		msgSize    int
		compressor *compressor.Compressor
	}{
		{
			"Msg of 256 bytes",
			256,
			&compressor.Compressor{
				Algorithm: compressor.CompressorGzip,
			},
		},
		{
			"Msg of 512 bytes",
			512,
			&compressor.Compressor{
				Algorithm: compressor.CompressorGzip,
			},
		},
		{
			"Msg of 1024 bytes",
			1024,
			&compressor.Compressor{
				Algorithm: compressor.CompressorGzip,
			},
		},
		{
			"Msg of 2048 bytes",
			2048,
			&compressor.Compressor{
				Algorithm: compressor.CompressorGzip,
			},
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				lc.SendWTagAndCompressorAsync(tag, getRandomMsg(tt.msgSize), tt.compressor)
			}
		})
	}
}

func BenchmarkTestReliableClient_SendWTag_udp(b *testing.B) {
	// Ensure status path does not exist
	os.RemoveAll("/tmp/devo-sender-reliable-client-benchmar")

	// Create client
	lc, err := NewReliableClientBuilder().
		StatusBuilder(
			status.NewNutsDBStatusBuilder().DbPath("/tmp/devo-sender-reliable-client-benchmar")).
		ClientBuilder(
			NewClientBuilder().EntryPoint("udp://localhost:13000")).
		Build()
	if err != nil {
		panic(err)
	}
	defer func() {
		lc.Close()
		// Clean status path
		os.RemoveAll("/tmp/devo-sender-reliable-client-benchmar")
	}()

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
				lc.SendWTag(tag, getRandomMsg(tt.msgSize))
			}
		})
	}
}

func BenchmarkTestReliableClient_SendWTagAndCompressor_udp(b *testing.B) {
	// Ensure status path does not exist
	os.RemoveAll("/tmp/devo-sender-reliable-client-benchmar")

	// Create client
	lc, err := NewReliableClientBuilder().
		StatusBuilder(
			status.NewNutsDBStatusBuilder().DbPath("/tmp/devo-sender-reliable-client-benchmar")).
		ClientBuilder(
			NewClientBuilder().EntryPoint("udp://localhost:13000")).
		Build()
	if err != nil {
		panic(err)
	}
	defer func() {
		lc.Close()
		// Clean status path
		os.RemoveAll("/tmp/devo-sender-reliable-client-benchmar")
	}()

	tests := []struct {
		name       string
		msgSize    int
		compressor *compressor.Compressor
	}{
		{
			"Msg of 256 bytes",
			256,
			&compressor.Compressor{
				Algorithm: compressor.CompressorGzip,
			},
		},
		{
			"Msg of 512 bytes",
			512,
			&compressor.Compressor{
				Algorithm: compressor.CompressorGzip,
			},
		},
		{
			"Msg of 1024 bytes",
			1024,
			&compressor.Compressor{
				Algorithm: compressor.CompressorGzip,
			},
		},
		{
			"Msg of 2048 bytes",
			2048,
			&compressor.Compressor{
				Algorithm: compressor.CompressorGzip,
			},
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				lc.SendWTagAndCompressor(tag, getRandomMsg(tt.msgSize), tt.compressor)
			}
		})
	}
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func getRandomMsg(size int) string {
	b := make([]rune, size)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
