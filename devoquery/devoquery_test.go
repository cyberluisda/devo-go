package devoquery

import (
	"reflect"
	"testing"
	"time"
)

func TestNewTokenEngine(t *testing.T) {
	type args struct {
		apiURL string
		token  string
	}
	tests := []struct {
		name    string
		args    args
		want    *QueryEngineToken
		wantErr bool
	}{
		{
			"Error: empty api url",
			args{
				apiURL: "",
				token:  "token",
			},
			nil,
			true,
		},
		{
			"Error: Invalid api url",
			args{
				apiURL: "apiURL",
				token:  "token",
			},
			nil,
			true,
		},
		{
			"Error: Empty token",
			args{
				apiURL: "http://apiurl.does.not.exits.org",
				token:  "",
			},
			nil,
			true,
		},
		{
			"Query engine created",
			args{
				apiURL: "http://apiurl.does.not.exits.org",
				token:  "token",
			},
			&QueryEngineToken{
				apiURL: "http://apiurl.does.not.exits.org",
				token:  "token",
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewTokenEngine(tt.args.apiURL, tt.args.token)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewTokenEngine() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewTokenEngine() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewTokenEngineDefaultQuery(t *testing.T) {
	// Values
	query := "from test.keep.free"
	type args struct {
		apiURL string
		token  string
		query  string
	}
	tests := []struct {
		name    string
		args    args
		want    *QueryEngineToken
		wantErr bool
	}{
		{
			"Error: Empty query",
			args{
				apiURL: "http://apiurl.does.not.exits.org",
				token:  "token",
			},
			nil,
			true,
		},
		{
			"Query engine created",
			args{
				apiURL: "http://apiurl.does.not.exits.org",
				token:  "token",
				query:  query,
			},
			&QueryEngineToken{
				apiURL:       "http://apiurl.does.not.exits.org",
				token:        "token",
				DefaultQuery: &query,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewTokenEngineDefaultQuery(tt.args.apiURL, tt.args.token, tt.args.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewTokenEngineDefaultQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewTokenEngineDefaultQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryEngineToken_RunNewQuery(t *testing.T) {
	type fields struct {
		token        string
		apiURL       string
		DefaultQuery *string
	}
	type args struct {
		from  time.Time
		to    time.Time
		query string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *QueryResult
		wantErr bool
	}{
		{
			"Error: Empty query",
			fields{
				token:  "token",
				apiURL: "http://api.does.not.extis.org",
			},
			args{
				from:  time.Now().Add(time.Minute * -5),
				to:    time.Now(),
				query: "",
			},
			nil,
			true,
		},
		{
			"Error: to value equal to from value with precission to second",
			fields{
				token:  "token",
				apiURL: "http://api.does.not.extis.org",
			},
			args{
				from:  time.Now(),
				to:    time.Now(),
				query: "from test.keep.free",
			},
			nil,
			true,
		},
		{
			"Error: to value before from value",
			fields{
				token:  "token",
				apiURL: "http://api.does.not.extis.org",
			},
			args{
				from:  time.Now().Add(time.Minute * 5),
				to:    time.Now(),
				query: "from test.keep.free",
			},
			nil,
			true,
		},
		{
			"Error: HTTP request failling",
			fields{
				token:  "token",
				apiURL: "http://api.does.not.extis.org",
			},
			args{
				from:  time.Now().Add(time.Minute * -5),
				to:    time.Now(),
				query: "from test.keep.free",
			},
			nil,
			true,
		},
		{
			"Error: HTTP response code not valid",
			fields{
				token:  "token",
				apiURL: DevoQueryApiv2EU,
			},
			args{
				from:  time.Now().Add(time.Minute * -5),
				to:    time.Now(),
				query: "from test.keep.free",
			},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dqt := &QueryEngineToken{
				token:        tt.fields.token,
				apiURL:       tt.fields.apiURL,
				DefaultQuery: tt.fields.DefaultQuery,
			}
			got, err := dqt.RunNewQuery(tt.args.from, tt.args.to, tt.args.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("QueryEngineToken.RunNewQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("QueryEngineToken.RunNewQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryEngineToken_RunDefaultQuery(t *testing.T) {
	// Values

	q := "from test.keep.free"
	qEmpty := ""

	type fields struct {
		token        string
		apiURL       string
		DefaultQuery *string
	}
	type args struct {
		from time.Time
		to   time.Time
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *QueryResult
		wantErr bool
	}{

		{
			"Error: Default query not defined",
			fields{
				token:  "token",
				apiURL: "http://api.does.not.extis.org",
			},
			args{
				from: time.Now().Add(time.Minute * -5),
				to:   time.Now(),
			},
			nil,
			true,
		},
		{
			"Error: Default query empty",
			fields{
				token:        "token",
				apiURL:       "http://api.does.not.extis.org",
				DefaultQuery: &qEmpty,
			},
			args{
				from: time.Now().Add(time.Minute * -5),
				to:   time.Now(),
			},
			nil,
			true,
		},
		{
			"Error: HTTP request failling",
			fields{
				token:        "token",
				apiURL:       "http://api.does.not.extis.org",
				DefaultQuery: &q,
			},
			args{
				from: time.Now().Add(time.Minute * -5),
				to:   time.Now(),
			},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dqt := &QueryEngineToken{
				token:        tt.fields.token,
				apiURL:       tt.fields.apiURL,
				DefaultQuery: tt.fields.DefaultQuery,
			}
			got, err := dqt.RunDefaultQuery(tt.args.from, tt.args.to)
			if (err != nil) != tt.wantErr {
				t.Errorf("QueryEngineToken.RunDefaultQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("QueryEngineToken.RunDefaultQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}
