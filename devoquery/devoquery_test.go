package devoquery

import (
	"reflect"
	"testing"
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
