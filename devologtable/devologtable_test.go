package devologtable

import (
	"reflect"
	"testing"
	"text/template"
	"time"

	"github.com/cyberluisda/devo-go/devoquery"
	"github.com/cyberluisda/devo-go/devosender"
)

func newQE() devoquery.QueryEngine {
	qe, _ := devoquery.NewTokenEngineDefaultQuery("https://dummy.api", "dummy_token", "dummy.table")
	return qe
}

func newDS() devosender.DevoSender {
	ds, _ := devosender.NewClientBuilder().
		EntryPoint("udp://example.org:80").
		Build()

	return ds
}

func TestNewLogTableOneStringColumn(t *testing.T) {

	type args struct {
		qe     devoquery.QueryEngine
		ds     devosender.DevoSender
		table  string
		column string
	}
	tests := []struct {
		name    string
		args    args
		want    *LogTableOneStringColumn
		wantErr bool
	}{
		{
			"Empty table error",
			args{
				newQE(),
				newDS(),
				"",
				"",
			},
			nil,
			true,
		},
		{
			"Empty column error",
			args{
				newQE(),
				newDS(),
				"dummy_table",
				"",
			},
			nil,
			true,
		},
		{
			"Instantiate LogTableOneStringColumn",
			args{
				newQE(),
				newDS(),
				"dummy_table",
				"dummy_column",
			},
			&LogTableOneStringColumn{
				Table:  "dummy_table",
				Column: "dummy_column",
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewLogTableOneStringColumn(tt.args.qe, tt.args.ds, tt.args.table, tt.args.column)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewLogTableOneStringColumn() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.want != nil {
				// Elements to solve dinamically
				tt.want.BeginTable = got.maxBeginTable

				tt.want.devoSender = got.devoSender
				tt.want.queryEngine = got.queryEngine
				tt.want.maxBeginTable = got.maxBeginTable
				tt.want.saveTpl = got.saveTpl
				tt.want.queryAll = got.queryAll
				tt.want.queryGetValueTpl = got.queryGetValueTpl
				tt.want.queryLastControlPoint = got.queryLastControlPoint
				tt.want.queryFirstDataLive = got.queryFirstDataLive
				tt.want.queryGetNamesTpl = got.queryGetNamesTpl
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewLogTableOneStringColumn() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
