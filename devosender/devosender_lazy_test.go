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
