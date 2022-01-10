package status

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xujiajun/nutsdb"
)

func TestSorteableStringTime_Swap(t *testing.T) {
	type fields struct {
		Values     []string
		Timestamps []time.Time
	}
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *SorteableStringTime
	}{
		{
			"Swap elements minor first",
			fields{
				[]string{"b", "a"},
				func() []time.Time {
					r := make([]time.Time, 2)
					var t time.Time
					r[0] = t.Add(time.Second * 2)
					r[1] = t.Add(time.Second)
					return r
				}(),
			},
			args{0, 1},
			&SorteableStringTime{
				Values: []string{"a", "b"},
				Timestamps: func() []time.Time {
					r := make([]time.Time, 2)
					var t time.Time
					r[0] = t.Add(time.Second)
					r[1] = t.Add(time.Second * 2)
					return r
				}(),
			},
		},
		{
			"Swap elements major first",
			fields{
				[]string{"b", "a"},
				func() []time.Time {
					r := make([]time.Time, 2)
					var t time.Time
					r[0] = t.Add(time.Second * 2)
					r[1] = t.Add(time.Second)
					return r
				}(),
			},
			args{1, 0},
			&SorteableStringTime{
				Values: []string{"a", "b"},
				Timestamps: func() []time.Time {
					r := make([]time.Time, 2)
					var t time.Time
					r[0] = t.Add(time.Second)
					r[1] = t.Add(time.Second * 2)
					return r
				}(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sst := &SorteableStringTime{
				Values:     tt.fields.Values,
				Timestamps: tt.fields.Timestamps,
			}
			sst.Swap(tt.args.i, tt.args.j)
			if !reflect.DeepEqual(sst, tt.want) {
				t.Errorf("SorteableStringTime.Swap() = %+v, want %+v", sst, tt.want)
			}
		})
	}
}

func Test_IsNotFoundErr(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			"Nil error",
			args{},
			false,
		},
		{
			"ErrBucketNotFound",
			args{nutsdb.ErrBucketNotFound},
			true,
		},
		{
			"ErrBucketEmpty",
			args{nutsdb.ErrBucketEmpty},
			true,
		},
		{
			"ErrNotFoundKey",
			args{nutsdb.ErrNotFoundKey},
			true,
		},
		{
			"ErrKeyNotFound",
			args{nutsdb.ErrKeyNotFound},
			true,
		},
		{
			"err bucket",
			args{errors.New("err bucket")},
			true,
		},
		{
			"key not exits",
			args{errors.New("key not exits")},
			true,
		},
		{
			"item not exits",
			args{errors.New("item not exits")},
			true,
		},
		{
			"not found bucket:FOO,key:BAR",
			args{errors.New("not found bucket:FOOfoo,key:BARbarBAR")},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsNotFoundErr(tt.args.err); got != tt.want {
				t.Errorf("IsNotFoundErr() = %v, want %v", got, tt.want)
			}
		})
	}
}
