package devosender

import (
	"reflect"
	"testing"
)

func TestLazyClient_popBuffer(t *testing.T) {
	type fields struct {
		buffer []*lazyClientRecord
	}
	tests := []struct {
		name       string
		fields     fields
		want       *lazyClientRecord
		want1      bool
		wantBuffer []*lazyClientRecord
	}{
		{
			"Empty buffer",
			fields{},
			nil,
			false,
			nil,
		},
		{
			"One element buffer",
			fields{
				[]*lazyClientRecord{
					{
						AsyncID: "async id",
						Msg:     "msg",
						Tag:     "tag",
					},
				},
			},
			&lazyClientRecord{
				AsyncID: "async id",
				Msg:     "msg",
				Tag:     "tag",
			},
			true,
			nil,
		},
		{
			"Two elements buffer",
			fields{
				[]*lazyClientRecord{
					{
						AsyncID: "async id 1",
						Msg:     "msg 1",
						Tag:     "tag 1",
					},
					{
						AsyncID: "async id 2",
						Msg:     "msg 2",
						Tag:     "tag 2",
					},
				},
			},
			&lazyClientRecord{
				AsyncID: "async id 1",
				Msg:     "msg 1",
				Tag:     "tag 1",
			},
			true,
			[]*lazyClientRecord{
				{
					AsyncID: "async id 2",
					Msg:     "msg 2",
					Tag:     "tag 2",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc := &LazyClient{
				buffer: tt.fields.buffer,
			}
			got, got1 := lc.popBuffer()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LazyClient.popBuffer() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("LazyClient.popBuffer() got1 = %v, want %v", got1, tt.want1)
			}
			if !reflect.DeepEqual(lc.buffer, tt.wantBuffer) {
				t.Errorf("LazyClient.popBuffer() remaining buffer got = %#v, want %#v", lc.buffer, tt.wantBuffer)
			}
		})
	}
}

func TestLazyClient_undoPopBuffer(t *testing.T) {
	type fields struct {
		bufferSize uint32
		buffer     []*lazyClientRecord
	}
	type args struct {
		r *lazyClientRecord
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantErr    bool
		wantBuffer []*lazyClientRecord
	}{
		{
			"Nil record",
			fields{},
			args{},
			false,
			nil,
		},
		{
			"Zero buffer size",
			fields{},
			args{
				&lazyClientRecord{
					AsyncID: "async id 1",
					Msg:     "msg 1",
					Tag:     "tag 1",
				},
			},
			true,
			nil,
		},
		{
			"Empty buffer",
			fields{
				bufferSize: 1,
			},
			args{
				&lazyClientRecord{
					AsyncID: "async id 1",
					Msg:     "msg 1",
					Tag:     "tag 1",
				},
			},
			false,
			[]*lazyClientRecord{
				{
					AsyncID: "async id 1",
					Msg:     "msg 1",
					Tag:     "tag 1",
				},
			},
		},
		{
			"Full buffer",
			fields{
				bufferSize: 1,
				buffer: []*lazyClientRecord{
					{
						AsyncID: "async id 1",
						Msg:     "msg 1",
						Tag:     "tag 1",
					},
				},
			},
			args{
				&lazyClientRecord{
					AsyncID: "async id 2",
					Msg:     "msg 2",
					Tag:     "tag 2",
				},
			},
			true,
			[]*lazyClientRecord{
				{
					AsyncID: "async id 1",
					Msg:     "msg 1",
					Tag:     "tag 1",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc := &LazyClient{
				bufferSize: tt.fields.bufferSize,
				buffer:     tt.fields.buffer,
			}
			if err := lc.undoPopBuffer(tt.args.r); (err != nil) != tt.wantErr {
				t.Errorf("LazyClient.undoPopBuffer() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(lc.buffer, tt.wantBuffer) {
				t.Errorf("LazyClient.undoPopBuffer() remaining buffer got = %#v, want %#v", lc.buffer, tt.wantBuffer)
			}
		})
	}
}

type MemoryAppLogger struct {
	Events []string
	Level  applogger.Level
}

func (mal *MemoryAppLogger) IsLevelEnabled(l applogger.Level) bool {
	return l <= mal.Level
}

func (mal *MemoryAppLogger) Log(l applogger.Level, a ...interface{}) {
	mal.Events = append(mal.Events, fmt.Sprint(l, a))
}

func (mal *MemoryAppLogger) Logf(l applogger.Level, format string, a ...interface{}) {
	mal.Events = append(mal.Events, fmt.Sprintf("%s: "+format, applogger.LevelString(l), a))
}
