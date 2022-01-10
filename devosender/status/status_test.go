package status

import (
	"reflect"
	"testing"
	"time"
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
