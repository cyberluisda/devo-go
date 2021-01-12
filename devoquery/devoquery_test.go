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
				apiURL: "http://api.does.not.exists.org",
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
				apiURL: "http://api.does.not.exists.org",
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
				apiURL: "http://api.does.not.exists.org",
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
				apiURL: "http://api.does.not.exists.123454311878197982173187918273912873192837.org",
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
		{
			"Error: Unparseable content",
			fields{
				token:  "token",
				apiURL: "http://api.does.not.exists.org", // Empty response
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
				apiURL: "http://api.does.not.exists.org",
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
				apiURL:       "http://api.does.not.exists.org",
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
			"Error: Unparseable content",
			fields{
				token:        "token",
				apiURL:       "http://api.does.not.exists.org", // Empty response
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

func Test_parseQueryResult(t *testing.T) {
	type args struct {
		d []byte
	}
	tests := []struct {
		name    string
		args    args
		want    *QueryResult
		wantErr bool
	}{
		{
			"Error: Unexpected input format",
			args{
				d: []byte("this is not valid json"),
			},
			nil,
			true,
		},
		{
			"Error: Unexpected status code",
			args{
				d: []byte(`{"status": 123}`),
			},
			nil,
			true,
		},
		{
			"Columns definition",
			args{
				d: []byte(`{
					"status": 0,
					"object": {
						"m": {
							"col1": {
								"index": 0,
								"type" : "int8"
							},
							"col2": {
								"index": 1,
								"type" : "float32"
							}
						}
					}
				}`),
			},
			&QueryResult{
				Columns: map[string]ColumnResult{
					"col1": {
						Name:  "col1",
						Index: 0,
						Type:  "int8",
					},
					"col2": {
						Name:  "col2",
						Index: 1,
						Type:  "float32",
					},
				},
			},
			false,
		},
		{
			"Data",
			args{
				d: []byte(`{
					"status": 0,
					"cid": "CID",
					"object": {
						"m": {
							"col1": {
								"index": 0,
								"type" : "float32"
							},
							"col2": {
								"index": 1,
								"type" : "float32"
							}
						},
						"d": [
							[1.1, 1.2],
							[2.1, 2.2]
						]
					}
				}`),
			},
			&QueryResult{
				DevoQueryID: "CID",
				Columns: map[string]ColumnResult{
					"col1": {
						Name:  "col1",
						Index: 0,
						Type:  "float32",
					},
					"col2": {
						Name:  "col2",
						Index: 1,
						Type:  "float32",
					},
				},
				Values: [][]interface{}{
					{1.1, 1.2},
					{2.1, 2.2},
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseQueryResult(tt.args.d)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseQueryResult() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseQueryResult() = %v, want %v", got, tt.want)
			}
		})
	}
}
