package compressor

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"io"
)

// CompressorAlgorithm define the compression algorithm used bye Compressor
type CompressorAlgorithm int

const (
	// CompressorNoComprs means compression disabled
	CompressorNoComprs CompressorAlgorithm = iota
	// CompressorGzip set GZIP compression
	CompressorGzip
	// CompressorZlib is Deprecated: This is not properly working if more than one message is send by same connection
	CompressorZlib
)

// Compressor is a simple compressor to work with relative small size of bytes (all is in memory)
type Compressor struct {
	Algorithm   CompressorAlgorithm
	MinimumSize int
}

// Compress compress bs input based on Algorithm and return the data compressed
func (mc *Compressor) Compress(bs []byte) ([]byte, error) {
	if mc == nil {
		return bs, nil
	}

	if mc.MinimumSize > 0 && len(bs) <= mc.MinimumSize {
		return bs, nil
	}

	var buf bytes.Buffer
	var zw io.WriteCloser
	switch mc.Algorithm {
	case CompressorNoComprs:
		r := make([]byte, len(bs))
		copy(r, bs)
		return r, nil
	case CompressorGzip:
		zw = gzip.NewWriter(&buf)
	case CompressorZlib:
		zw = zlib.NewWriter(&buf)
	default:
		return nil, fmt.Errorf("Algorithm %v is not supported", mc.Algorithm)
	}

	_, err := zw.Write(bs)
	if err != nil {
		return nil, fmt.Errorf("Compression: %w", err)
	}

	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("Close compression engine: %w", err)
	}

	return buf.Bytes(), nil
}

// StringAlgoritm return the string value of algorithm selected in Compressor object
func (mc *Compressor) StringAlgoritm() string {
	return StringCompressorAlgorithm(mc.Algorithm)
}

// StringCompressorAlgorithm return the string value of algorithm selected
func StringCompressorAlgorithm(a CompressorAlgorithm) string {
	switch a {
	case CompressorNoComprs:
		return "No compression"
	case CompressorGzip:
		return "GZIP"
	case CompressorZlib:
		return "ZLIB"
	default:
		return "Unknown"
	}
}

// ParseAlgorithm is the inverse func of StringCompressorAlgorithm. Error is returned
// if s value is not matching with none valid algorithm
func ParseAlgorithm(s string) (CompressorAlgorithm, error) {
	for a := CompressorNoComprs; a <= CompressorZlib; a++ {
		v := StringCompressorAlgorithm(a)
		if v == s {
			return a, nil
		}
	}

	return CompressorNoComprs, fmt.Errorf("%s is not a valid algorithm", s)
}
