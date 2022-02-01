package compressor

import (
	"reflect"
	"testing"
)

func TestStringCompressorAlgorithm(t *testing.T) {
	type args struct {
		a CompressorAlgorithm
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			"No compression",
			args{CompressorNoComprs},
			"No compression",
		},
		{
			"GZIP",
			args{CompressorGzip},
			"GZIP",
		},
		{
			"ZLIB",
			args{CompressorZlib},
			"ZLIB",
		},
		{
			"Other",
			args{234},
			"Unknown",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StringCompressorAlgorithm(tt.args.a); got != tt.want {
				t.Errorf("StringCompressorAlgorithm() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseAlgorithm(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		args    args
		want    CompressorAlgorithm
		wantErr bool
	}{
		{
			"No compression",
			args{"No compression"},
			CompressorNoComprs,
			false,
		},
		{
			"GZIP",
			args{"GZIP"},
			CompressorGzip,
			false,
		},
		{
			"ZLIB",
			args{"ZLIB"},
			CompressorZlib,
			false,
		},
		{
			"Invalid",
			args{"This is not valid"},
			CompressorNoComprs,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseAlgorithm(tt.args.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseAlgorithm() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseAlgorithm() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompressor_StringAlgoritm(t *testing.T) {
	type fields struct {
		Algorithm CompressorAlgorithm
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			"No compression",
			fields{CompressorNoComprs},
			StringCompressorAlgorithm(CompressorNoComprs),
		},
		{
			"Gzip",
			fields{CompressorGzip},
			StringCompressorAlgorithm(CompressorGzip),
		},
		{
			"Zlib",
			fields{CompressorZlib},
			StringCompressorAlgorithm(CompressorZlib),
		},
		{
			"Unknown",
			fields{123},
			StringCompressorAlgorithm(123),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &Compressor{
				Algorithm: tt.fields.Algorithm,
			}
			if got := mc.StringAlgoritm(); got != tt.want {
				t.Errorf("Compressor.StringAlgoritm() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompressor_Compress(t *testing.T) {
	type fields struct {
		Algorithm   CompressorAlgorithm
		MinimumSize int
	}
	type args struct {
		bs []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{
			"Unsupported algorithm",
			fields{Algorithm: 1234},
			args{nil},
			nil,
			true,
		},
		{
			"No compression empty imput",
			fields{Algorithm: CompressorNoComprs},
			args{nil},
			[]byte{},
			false,
		},
		{
			"No compression",
			fields{Algorithm: CompressorNoComprs},
			args{[]byte("hi")},
			[]byte("hi"),
			false,
		},
		{
			"Gzip empty imput",
			fields{Algorithm: CompressorGzip},
			args{nil},
			[]byte{31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 1, 0, 0, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0},
			false,
		},
		{
			"Gzip",
			fields{Algorithm: CompressorGzip},
			args{[]byte("hi")},
			[]byte{31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 202, 200, 4, 4, 0, 0, 255, 255, 172, 42, 147, 216, 2, 0, 0, 0},
			false,
		},
		{
			"Zlib empty imput",
			fields{Algorithm: CompressorZlib},
			args{nil},
			[]byte{120, 156, 1, 0, 0, 255, 255, 0, 0, 0, 1},
			false,
		},
		{
			"Zlib",
			fields{Algorithm: CompressorZlib},
			args{[]byte("hi")},
			[]byte{120, 156, 202, 200, 4, 4, 0, 0, 255, 255, 1, 59, 0, 210},
			false,
		},
		{
			"MinimumSize",
			fields{CompressorGzip, 5},
			args{[]byte("hi")},
			[]byte("hi"),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &Compressor{
				Algorithm:   tt.fields.Algorithm,
				MinimumSize: tt.fields.MinimumSize,
			}
			got, err := mc.Compress(tt.args.bs)
			if (err != nil) != tt.wantErr {
				t.Errorf("Compressor.Compress() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Compressor.Compress() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompressor_Compress_nil_compressor(t *testing.T) {
	var mc *Compressor
	_, err := mc.Compress([]byte{})
	if err != nil {
		t.Errorf("Compressor.Compress() nil instance error = %v", err)
	}
}
