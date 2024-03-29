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
	qe, _ := devoquery.NewTokenEngineDefaultQuery("https://localhost:12345", "dummy_token", "dummy.table")
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

func TestLogTableOneStringColumn_SetValue(t *testing.T) {
	type fields struct {
		Table                 string
		Column                string
		BeginTable            time.Time
		devoSender            devosender.DevoSender
		queryEngine           devoquery.QueryEngine
		maxBeginTable         time.Time
		saveTpl               *template.Template
		queryAll              string
		queryGetValueTpl      *template.Template
		queryLastControlPoint string
		queryFirstDataLive    string
		queryGetNamesTpl      *template.Template
	}
	type args struct {
		name  string
		value string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"Template error",
			fields{
				Table:      "dummy_table",
				Column:     "dummy_column",
				saveTpl:    template.Must(template.New("save").Parse(oneStringColumnSaveTpl + "|{{.Value2}}")),
				devoSender: newDS(),
			},
			args{"Name", "Value"},
			true,
		},
		{
			"Send error",
			fields{
				Table:      "",
				Column:     "dummy_column",
				saveTpl:    template.Must(template.New("save").Parse(oneStringColumnSaveTpl)),
				devoSender: newDS(),
			},
			args{"Name", "Value"},
			true,
		},
		{
			"Save value",
			fields{
				Table:      "dummy_table",
				Column:     "dummy_column",
				saveTpl:    template.Must(template.New("save").Parse(oneStringColumnSaveTpl)),
				devoSender: newDS(),
			},
			args{"Name", "Value"},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ltoc := &LogTableOneStringColumn{
				Table:                 tt.fields.Table,
				Column:                tt.fields.Column,
				BeginTable:            tt.fields.BeginTable,
				devoSender:            tt.fields.devoSender,
				queryEngine:           tt.fields.queryEngine,
				maxBeginTable:         tt.fields.maxBeginTable,
				saveTpl:               tt.fields.saveTpl,
				queryAll:              tt.fields.queryAll,
				queryGetValueTpl:      tt.fields.queryGetValueTpl,
				queryLastControlPoint: tt.fields.queryLastControlPoint,
				queryFirstDataLive:    tt.fields.queryFirstDataLive,
				queryGetNamesTpl:      tt.fields.queryGetNamesTpl,
			}
			if err := ltoc.SetValue(tt.args.name, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("LogTableOneStringColumn.SetValue() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLogTableOneStringColumn_SetValueAndCheck(t *testing.T) {
	type fields struct {
		Table                 string
		Column                string
		BeginTable            time.Time
		devoSender            devosender.DevoSender
		queryEngine           devoquery.QueryEngine
		maxBeginTable         time.Time
		saveTpl               *template.Template
		queryAll              string
		queryGetValueTpl      *template.Template
		queryLastControlPoint string
		queryFirstDataLive    string
		queryGetNamesTpl      *template.Template
	}
	type args struct {
		name          string
		value         string
		checkInterval time.Duration
		maxRetries    int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"Error when call set part return error",
			fields{
				Table:      "",
				Column:     "dummy_column",
				saveTpl:    template.Must(template.New("save").Parse(oneStringColumnSaveTpl)),
				devoSender: newDS(),
			},
			args{
				"Name",
				"Value",
				time.Second * 1,
				1,
			},
			true,
		},
		{
			"Error when no retries",
			fields{
				Table:      "dummy_table",
				Column:     "dummy_column",
				saveTpl:    template.Must(template.New("save").Parse(oneStringColumnSaveTpl)),
				devoSender: newDS(),
			},
			args{
				"Name",
				"Value",
				time.Millisecond * 1,
				0,
			},
			true,
		},
		{
			"Error when Check: 1 retry",
			fields{
				Table:            "dummy_table",
				Column:           "dummy_column",
				saveTpl:          template.Must(template.New("save").Parse(oneStringColumnSaveTpl)),
				queryGetValueTpl: template.Must(template.New("queryByName").Parse(oneStringColumnQueryGetValueTpl)),
				devoSender:       newDS(),
				queryEngine:      newQE(),
			},
			args{
				"Name",
				"Value",
				time.Millisecond * 1,
				1,
			},
			true,
		},
		{
			"Error when Check: 2 retries",
			fields{
				Table:            "dummy_table",
				Column:           "dummy_column",
				saveTpl:          template.Must(template.New("save").Parse(oneStringColumnSaveTpl)),
				queryGetValueTpl: template.Must(template.New("queryByName").Parse(oneStringColumnQueryGetValueTpl)),
				devoSender:       newDS(),
				queryEngine:      newQE(),
			},
			args{
				"Name",
				"Value",
				time.Millisecond * 1,
				2,
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ltoc := &LogTableOneStringColumn{
				Table:                 tt.fields.Table,
				Column:                tt.fields.Column,
				BeginTable:            tt.fields.BeginTable,
				devoSender:            tt.fields.devoSender,
				queryEngine:           tt.fields.queryEngine,
				maxBeginTable:         tt.fields.maxBeginTable,
				saveTpl:               tt.fields.saveTpl,
				queryAll:              tt.fields.queryAll,
				queryGetValueTpl:      tt.fields.queryGetValueTpl,
				queryLastControlPoint: tt.fields.queryLastControlPoint,
				queryFirstDataLive:    tt.fields.queryFirstDataLive,
				queryGetNamesTpl:      tt.fields.queryGetNamesTpl,
			}
			if err := ltoc.SetValueAndCheck(tt.args.name, tt.args.value, tt.args.checkInterval, tt.args.maxRetries); (err != nil) != tt.wantErr {
				t.Errorf("LogTableOneStringColumn.SetValueAndCheck() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLogTableOneStringColumn_SetBatchValues(t *testing.T) {
	type fields struct {
		Table                 string
		Column                string
		BeginTable            time.Time
		devoSender            devosender.DevoSender
		queryEngine           devoquery.QueryEngine
		maxBeginTable         time.Time
		saveTpl               *template.Template
		queryAll              string
		queryGetValueTpl      *template.Template
		queryLastControlPoint string
		queryFirstDataLive    string
		queryGetNamesTpl      *template.Template
	}
	type args struct {
		values map[string]string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"Error in template",
			fields{
				Table:      "dummy_table",
				Column:     "dummy_column",
				saveTpl:    template.Must(template.New("save").Parse(oneStringColumnSaveTpl + "|{{.Value2}}")),
				devoSender: newDS(),
			},
			args{
				map[string]string{
					"Name": "Value",
				},
			},
			true,
		},
		{
			"Errors when send data",
			fields{
				Table:      "",
				Column:     "dummy_column",
				saveTpl:    template.Must(template.New("save").Parse(oneStringColumnSaveTpl)),
				devoSender: newDS(),
			},
			args{
				map[string]string{
					"Name":  "Value",
					"Name2": "Value2",
				},
			},
			true,
		},
		{
			"Send data",
			fields{
				Table:      "dummy_table",
				Column:     "dummy_column",
				saveTpl:    template.Must(template.New("save").Parse(oneStringColumnSaveTpl)),
				devoSender: newDS(),
			},
			args{
				map[string]string{
					"Name":  "Value",
					"Name2": "Value2",
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ltoc := &LogTableOneStringColumn{
				Table:                 tt.fields.Table,
				Column:                tt.fields.Column,
				BeginTable:            tt.fields.BeginTable,
				devoSender:            tt.fields.devoSender,
				queryEngine:           tt.fields.queryEngine,
				maxBeginTable:         tt.fields.maxBeginTable,
				saveTpl:               tt.fields.saveTpl,
				queryAll:              tt.fields.queryAll,
				queryGetValueTpl:      tt.fields.queryGetValueTpl,
				queryLastControlPoint: tt.fields.queryLastControlPoint,
				queryFirstDataLive:    tt.fields.queryFirstDataLive,
				queryGetNamesTpl:      tt.fields.queryGetNamesTpl,
			}
			if err := ltoc.SetBatchValues(tt.args.values); (err != nil) != tt.wantErr {
				t.Errorf("LogTableOneStringColumn.SetBatchValues() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLogTableOneStringColumn_DeleteValue(t *testing.T) {
	type fields struct {
		Table                 string
		Column                string
		BeginTable            time.Time
		devoSender            devosender.DevoSender
		queryEngine           devoquery.QueryEngine
		maxBeginTable         time.Time
		saveTpl               *template.Template
		queryAll              string
		queryGetValueTpl      *template.Template
		queryLastControlPoint string
		queryFirstDataLive    string
		queryGetNamesTpl      *template.Template
	}
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"Error in template",
			fields{
				Table:      "dummy_table",
				Column:     "dummy_column",
				saveTpl:    template.Must(template.New("save").Parse(oneStringColumnSaveTpl + "|{{.Value2}}")),
				devoSender: newDS(),
			},
			args{
				"Name",
			},
			true,
		},
		{
			"Error when send data",
			fields{
				Table:      "",
				Column:     "dummy_column",
				saveTpl:    template.Must(template.New("save").Parse(oneStringColumnSaveTpl)),
				devoSender: newDS(),
			},
			args{
				"Name",
			},
			true,
		},
		{
			"Delete item",
			fields{
				Table:      "dumm_table",
				Column:     "dummy_column",
				saveTpl:    template.Must(template.New("save").Parse(oneStringColumnSaveTpl)),
				devoSender: newDS(),
			},
			args{
				"Name",
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ltoc := &LogTableOneStringColumn{
				Table:                 tt.fields.Table,
				Column:                tt.fields.Column,
				BeginTable:            tt.fields.BeginTable,
				devoSender:            tt.fields.devoSender,
				queryEngine:           tt.fields.queryEngine,
				maxBeginTable:         tt.fields.maxBeginTable,
				saveTpl:               tt.fields.saveTpl,
				queryAll:              tt.fields.queryAll,
				queryGetValueTpl:      tt.fields.queryGetValueTpl,
				queryLastControlPoint: tt.fields.queryLastControlPoint,
				queryFirstDataLive:    tt.fields.queryFirstDataLive,
				queryGetNamesTpl:      tt.fields.queryGetNamesTpl,
			}
			if err := ltoc.DeleteValue(tt.args.name); (err != nil) != tt.wantErr {
				t.Errorf("LogTableOneStringColumn.DeleteValue() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLogTableOneStringColumn_DeleteValueAndCheck(t *testing.T) {
	type fields struct {
		Table                 string
		Column                string
		BeginTable            time.Time
		devoSender            devosender.DevoSender
		queryEngine           devoquery.QueryEngine
		maxBeginTable         time.Time
		saveTpl               *template.Template
		queryAll              string
		queryGetValueTpl      *template.Template
		queryLastControlPoint string
		queryFirstDataLive    string
		queryGetNamesTpl      *template.Template
	}
	type args struct {
		name          string
		checkInterval time.Duration
		maxRetries    int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"Error when call set part return error",
			fields{
				Table:      "",
				Column:     "dummy_column",
				saveTpl:    template.Must(template.New("save").Parse(oneStringColumnSaveTpl)),
				devoSender: newDS(),
			},
			args{
				"Name",
				time.Second * 1,
				1,
			},
			true,
		},
		{
			"Error when no retries",
			fields{
				Table:      "dummy_table",
				Column:     "dummy_column",
				saveTpl:    template.Must(template.New("save").Parse(oneStringColumnSaveTpl)),
				devoSender: newDS(),
			},
			args{
				"Name",
				time.Millisecond * 1,
				0,
			},
			true,
		},
		{
			"Error when Check: 1 retry",
			fields{
				Table:            "dummy_table",
				Column:           "dummy_column",
				saveTpl:          template.Must(template.New("save").Parse(oneStringColumnSaveTpl)),
				queryGetValueTpl: template.Must(template.New("queryByName").Parse(oneStringColumnQueryGetValueTpl)),
				devoSender:       newDS(),
				queryEngine:      newQE(),
			},
			args{
				"Name",
				time.Millisecond * 1,
				1,
			},
			true,
		},
		{
			"Error when Check: 2 retries",
			fields{
				Table:            "dummy_table",
				Column:           "dummy_column",
				saveTpl:          template.Must(template.New("save").Parse(oneStringColumnSaveTpl)),
				queryGetValueTpl: template.Must(template.New("queryByName").Parse(oneStringColumnQueryGetValueTpl)),
				devoSender:       newDS(),
				queryEngine:      newQE(),
			},
			args{
				"Name",
				time.Millisecond * 1,
				2,
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ltoc := &LogTableOneStringColumn{
				Table:                 tt.fields.Table,
				Column:                tt.fields.Column,
				BeginTable:            tt.fields.BeginTable,
				devoSender:            tt.fields.devoSender,
				queryEngine:           tt.fields.queryEngine,
				maxBeginTable:         tt.fields.maxBeginTable,
				saveTpl:               tt.fields.saveTpl,
				queryAll:              tt.fields.queryAll,
				queryGetValueTpl:      tt.fields.queryGetValueTpl,
				queryLastControlPoint: tt.fields.queryLastControlPoint,
				queryFirstDataLive:    tt.fields.queryFirstDataLive,
				queryGetNamesTpl:      tt.fields.queryGetNamesTpl,
			}
			if err := ltoc.DeleteValueAndCheck(tt.args.name, tt.args.checkInterval, tt.args.maxRetries); (err != nil) != tt.wantErr {
				t.Errorf("LogTableOneStringColumn.DeleteValueAndCheck() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLogTableOneStringColumn_GetAll(t *testing.T) {
	type fields struct {
		Table                 string
		Column                string
		BeginTable            time.Time
		devoSender            devosender.DevoSender
		queryEngine           devoquery.QueryEngine
		maxBeginTable         time.Time
		saveTpl               *template.Template
		queryAll              string
		queryGetValueTpl      *template.Template
		queryLastControlPoint string
		queryFirstDataLive    string
		queryGetNamesTpl      *template.Template
	}
	tests := []struct {
		name    string
		fields  fields
		want    map[string]string
		wantErr bool
	}{
		{
			"Error empty table",
			fields{
				queryEngine: newQE(),
			},
			nil,
			true,
		},
		{
			"Error when run query",
			fields{
				queryEngine: newQE(),
				queryAll:    "from dummy_table",
			},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ltoc := &LogTableOneStringColumn{
				Table:                 tt.fields.Table,
				Column:                tt.fields.Column,
				BeginTable:            tt.fields.BeginTable,
				devoSender:            tt.fields.devoSender,
				queryEngine:           tt.fields.queryEngine,
				maxBeginTable:         tt.fields.maxBeginTable,
				saveTpl:               tt.fields.saveTpl,
				queryAll:              tt.fields.queryAll,
				queryGetValueTpl:      tt.fields.queryGetValueTpl,
				queryLastControlPoint: tt.fields.queryLastControlPoint,
				queryFirstDataLive:    tt.fields.queryFirstDataLive,
				queryGetNamesTpl:      tt.fields.queryGetNamesTpl,
			}
			got, err := ltoc.GetAll()
			if (err != nil) != tt.wantErr {
				t.Errorf("LogTableOneStringColumn.GetAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LogTableOneStringColumn.GetAll() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLogTableOneStringColumn_GetNames(t *testing.T) {
	type fields struct {
		Table                 string
		Column                string
		BeginTable            time.Time
		devoSender            devosender.DevoSender
		queryEngine           devoquery.QueryEngine
		maxBeginTable         time.Time
		saveTpl               *template.Template
		queryAll              string
		queryGetValueTpl      *template.Template
		queryLastControlPoint string
		queryFirstDataLive    string
		queryGetNamesTpl      *template.Template
	}
	type args struct {
		devoRegexp string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []string
		wantErr bool
	}{
		{
			"Error empty regexp",
			fields{},
			args{},
			[]string{},
			true,
		},
		{
			"Error regexp length is not enough",
			fields{},
			args{"as"},
			[]string{},
			true,
		},
		{
			"Error regexp does not start with ^",
			fields{},
			args{"asd"},
			[]string{},
			true,
		},
		{
			"Error regexp does not end with $",
			fields{},
			args{"^asd"},
			[]string{},
			true,
		},
		{
			"Error in templates",
			fields{
				queryGetNamesTpl: template.Must(template.New("queryGetNames").Parse(oneStringColumnQueryGetAllNamesTpl + "|{{.Value2}}")),
			},
			args{"^asd$"},
			[]string{},
			true,
		},
		{
			"Error when make query to Devo",
			fields{
				queryGetNamesTpl: template.Must(template.New("queryGetNames").Parse(oneStringColumnQueryGetAllNamesTpl)),
				queryEngine:      newQE(),
			},
			args{"^asd$"},
			[]string{},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ltoc := &LogTableOneStringColumn{
				Table:                 tt.fields.Table,
				Column:                tt.fields.Column,
				BeginTable:            tt.fields.BeginTable,
				devoSender:            tt.fields.devoSender,
				queryEngine:           tt.fields.queryEngine,
				maxBeginTable:         tt.fields.maxBeginTable,
				saveTpl:               tt.fields.saveTpl,
				queryAll:              tt.fields.queryAll,
				queryGetValueTpl:      tt.fields.queryGetValueTpl,
				queryLastControlPoint: tt.fields.queryLastControlPoint,
				queryFirstDataLive:    tt.fields.queryFirstDataLive,
				queryGetNamesTpl:      tt.fields.queryGetNamesTpl,
			}
			got, err := ltoc.GetNames(tt.args.devoRegexp)
			if (err != nil) != tt.wantErr {
				t.Errorf("LogTableOneStringColumn.GetNames() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LogTableOneStringColumn.GetNames() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLogTableOneStringColumn_DeleteBatchValues(t *testing.T) {
	type fields struct {
		Table                 string
		Column                string
		BeginTable            time.Time
		devoSender            devosender.DevoSender
		queryEngine           devoquery.QueryEngine
		maxBeginTable         time.Time
		saveTpl               *template.Template
		queryAll              string
		queryGetValueTpl      *template.Template
		queryLastControlPoint string
		queryFirstDataLive    string
		queryGetNamesTpl      *template.Template
	}
	type args struct {
		names []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"Error in template",
			fields{
				Table:      "dummy_table",
				Column:     "dummy_column",
				saveTpl:    template.Must(template.New("save").Parse(oneStringColumnSaveTpl + "|{{.Value2}}")),
				devoSender: newDS(),
			},
			args{
				[]string{"Name1"},
			},
			true,
		},
		{
			"Errors when send data",
			fields{
				Table:      "",
				Column:     "dummy_column",
				saveTpl:    template.Must(template.New("save").Parse(oneStringColumnSaveTpl)),
				devoSender: newDS(),
			},
			args{
				[]string{"Name1", "Name2"},
			},
			true,
		},
		{
			"Send data",
			fields{
				Table:      "dummy_table",
				Column:     "dummy_column",
				saveTpl:    template.Must(template.New("save").Parse(oneStringColumnSaveTpl)),
				devoSender: newDS(),
			},
			args{
				[]string{"Name1", "Name2"},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ltoc := &LogTableOneStringColumn{
				Table:                 tt.fields.Table,
				Column:                tt.fields.Column,
				BeginTable:            tt.fields.BeginTable,
				devoSender:            tt.fields.devoSender,
				queryEngine:           tt.fields.queryEngine,
				maxBeginTable:         tt.fields.maxBeginTable,
				saveTpl:               tt.fields.saveTpl,
				queryAll:              tt.fields.queryAll,
				queryGetValueTpl:      tt.fields.queryGetValueTpl,
				queryLastControlPoint: tt.fields.queryLastControlPoint,
				queryFirstDataLive:    tt.fields.queryFirstDataLive,
				queryGetNamesTpl:      tt.fields.queryGetNamesTpl,
			}
			if err := ltoc.DeleteBatchValues(tt.args.names); (err != nil) != tt.wantErr {
				t.Errorf("LogTableOneStringColumn.DeleteBatchValues() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLogTableOneStringColumn_GetValue(t *testing.T) {
	type fields struct {
		Table                 string
		Column                string
		BeginTable            time.Time
		devoSender            devosender.DevoSender
		queryEngine           devoquery.QueryEngine
		maxBeginTable         time.Time
		saveTpl               *template.Template
		queryAll              string
		queryGetValueTpl      *template.Template
		queryLastControlPoint string
		queryFirstDataLive    string
		queryGetNamesTpl      *template.Template
	}
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *string
		wantErr bool
	}{
		{
			"Error in template",
			fields{
				Table:            "dummy_table",
				Column:           "dummy_column",
				queryGetValueTpl: template.Must(template.New("queryByName").Parse(oneStringColumnQueryGetValueTpl + "|{{.Value2}}")),
			},
			args{
				"Name1",
			},
			nil,
			true,
		},
		{
			"Error when run Devo query",
			fields{
				Table:            "",
				Column:           "dummy_column",
				queryGetValueTpl: template.Must(template.New("queryByName").Parse(oneStringColumnQueryGetValueTpl)),
				queryEngine:      newQE(),
			},
			args{
				"Name1",
			},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ltoc := &LogTableOneStringColumn{
				Table:                 tt.fields.Table,
				Column:                tt.fields.Column,
				BeginTable:            tt.fields.BeginTable,
				devoSender:            tt.fields.devoSender,
				queryEngine:           tt.fields.queryEngine,
				maxBeginTable:         tt.fields.maxBeginTable,
				saveTpl:               tt.fields.saveTpl,
				queryAll:              tt.fields.queryAll,
				queryGetValueTpl:      tt.fields.queryGetValueTpl,
				queryLastControlPoint: tt.fields.queryLastControlPoint,
				queryFirstDataLive:    tt.fields.queryFirstDataLive,
				queryGetNamesTpl:      tt.fields.queryGetNamesTpl,
			}
			got, err := ltoc.GetValue(tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("LogTableOneStringColumn.GetValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LogTableOneStringColumn.GetValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLogTableOneStringColumn_RefreshDataHead(t *testing.T) {
	type fields struct {
		Table                 string
		Column                string
		BeginTable            time.Time
		devoSender            devosender.DevoSender
		queryEngine           devoquery.QueryEngine
		maxBeginTable         time.Time
		saveTpl               *template.Template
		queryAll              string
		queryGetValueTpl      *template.Template
		queryLastControlPoint string
		queryFirstDataLive    string
		queryGetNamesTpl      *template.Template
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			"Error empty query",
			fields{
				Table:       "dummy_table",
				Column:      "dummy_column",
				queryEngine: newQE(),
			},
			true,
		},
		{
			"Error when run query to Devo",
			fields{
				Table:                 "dummy_table",
				Column:                "dummy_column",
				queryEngine:           newQE(),
				queryLastControlPoint: "from dummy_table",
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ltoc := &LogTableOneStringColumn{
				Table:                 tt.fields.Table,
				Column:                tt.fields.Column,
				BeginTable:            tt.fields.BeginTable,
				devoSender:            tt.fields.devoSender,
				queryEngine:           tt.fields.queryEngine,
				maxBeginTable:         tt.fields.maxBeginTable,
				saveTpl:               tt.fields.saveTpl,
				queryAll:              tt.fields.queryAll,
				queryGetValueTpl:      tt.fields.queryGetValueTpl,
				queryLastControlPoint: tt.fields.queryLastControlPoint,
				queryFirstDataLive:    tt.fields.queryFirstDataLive,
				queryGetNamesTpl:      tt.fields.queryGetNamesTpl,
			}
			if err := ltoc.RefreshDataHead(); (err != nil) != tt.wantErr {
				t.Errorf("LogTableOneStringColumn.RefreshDataHead() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLogTableOneStringColumn_AddControlPoint(t *testing.T) {
	type fields struct {
		Table                 string
		Column                string
		BeginTable            time.Time
		devoSender            devosender.DevoSender
		queryEngine           devoquery.QueryEngine
		maxBeginTable         time.Time
		saveTpl               *template.Template
		queryAll              string
		queryGetValueTpl      *template.Template
		queryLastControlPoint string
		queryFirstDataLive    string
		queryGetNamesTpl      *template.Template
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			"Error empty query",
			fields{
				queryEngine: newQE(),
			},
			true,
		},
		{
			"Error when run query to Devo",
			fields{
				queryEngine: newQE(),
				queryAll:    "from dummy_table",
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ltoc := &LogTableOneStringColumn{
				Table:                 tt.fields.Table,
				Column:                tt.fields.Column,
				BeginTable:            tt.fields.BeginTable,
				devoSender:            tt.fields.devoSender,
				queryEngine:           tt.fields.queryEngine,
				maxBeginTable:         tt.fields.maxBeginTable,
				saveTpl:               tt.fields.saveTpl,
				queryAll:              tt.fields.queryAll,
				queryGetValueTpl:      tt.fields.queryGetValueTpl,
				queryLastControlPoint: tt.fields.queryLastControlPoint,
				queryFirstDataLive:    tt.fields.queryFirstDataLive,
				queryGetNamesTpl:      tt.fields.queryGetNamesTpl,
			}
			if err := ltoc.AddControlPoint(); (err != nil) != tt.wantErr {
				t.Errorf("LogTableOneStringColumn.AddControlPoint() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGetValueAsNumber(t *testing.T) {
	type args struct {
		lte  LogTableEngine
		name string
	}
	tests := []struct {
		name    string
		args    args
		want    *float64
		wantErr bool
	}{
		{
			"Error when call GetValue",
			args{
				func() LogTableEngine {
					r, _ := NewLogTableOneStringColumn(
						newQE(),
						newDS(),
						"dummy_table",
						"dummy_column",
					)
					return r
				}(),
				"Name",
			},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetValueAsNumber(tt.args.lte, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetValueAsNumber() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetValueAsNumber() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetValueAsBool(t *testing.T) {
	type args struct {
		lte  LogTableEngine
		name string
	}
	tests := []struct {
		name    string
		args    args
		want    *bool
		wantErr bool
	}{
		{
			"Error when call GetValue",
			args{
				func() LogTableEngine {
					r, _ := NewLogTableOneStringColumn(
						newQE(),
						newDS(),
						"dummy_table",
						"dummy_column",
					)
					return r
				}(),
				"Name",
			},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetValueAsBool(tt.args.lte, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetValueAsBool() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetValueAsBool() = %v, want %v", got, tt.want)
			}
		})
	}
}
