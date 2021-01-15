package devosender

import (
	"fmt"
	"net"
	"reflect"
	"regexp"
	"sync"
	"testing"
)

func Test_replaceSequences(t *testing.T) {
	type args struct {
		s         string
		sequences map[string]string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			"Secuences empty",
			args{
				"original string",
				map[string]string{},
			},
			"original string",
		},
		{
			"Empty string",
			args{
				"",
				map[string]string{"a": "b"},
			},
			"",
		},
		{
			"Replace sequences",
			args{
				"AAAaAAA bbbbBbbb a",
				map[string]string{"a": "b"},
			},
			"AAAbAAA bbbbBbbb b",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := replaceSequences(tt.args.s, tt.args.sequences); got != tt.want {
				t.Errorf("replaceSequences() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_makeConnection(t *testing.T) {
	type fields struct {
		entryPoint        string
		syslogHostname    string
		defaultTag        string
		conn              net.Conn
		ReplaceSequences  map[string]string
		tls               *tlsSetup
		waitGroup         sync.WaitGroup
		asyncErrors       map[string]error
		asyncErrorsMutext sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			"Empty entryPoint",
			fields{},
			true,
		},
		{
			"Unexpected entryPoint format",
			fields{
				entryPoint: "this is not valid",
			},
			true,
		},
		{
			"Error when create clean connection",
			fields{
				entryPoint: "udp://",
			},
			true,
		},
		{
			"Error when create tls connection",
			fields{
				entryPoint: "tcp://doesnot.exist.12345678987654321.org:13003",
				tls: func() *tlsSetup {
					t := tlsSetup{}

					return &t
				}(),
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := &Client{
				entryPoint:        tt.fields.entryPoint,
				syslogHostname:    tt.fields.syslogHostname,
				defaultTag:        tt.fields.defaultTag,
				conn:              tt.fields.conn,
				ReplaceSequences:  tt.fields.ReplaceSequences,
				tls:               tt.fields.tls,
				waitGroup:         tt.fields.waitGroup,
				asyncErrors:       tt.fields.asyncErrors,
				asyncErrorsMutext: tt.fields.asyncErrorsMutext,
			}
			if err := dsc.makeConnection(); (err != nil) != tt.wantErr {
				t.Errorf("Client.makeConnection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_AddReplaceSequences(t *testing.T) {
	type fields struct {
		entryPoint        string
		syslogHostname    string
		defaultTag        string
		conn              net.Conn
		ReplaceSequences  map[string]string
		tls               *tlsSetup
		waitGroup         sync.WaitGroup
		asyncErrors       map[string]error
		asyncErrorsMutext sync.Mutex
	}
	type args struct {
		old string
		new string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"Error: empty old string",
			fields{},
			args{},
			true,
		},
		{
			"Error: empty new string",
			fields{},
			args{
				old: "old",
			},
			true,
		},
		{
			"Error: empty new string",
			fields{},
			args{
				old: "old",
				new: "new",
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := &Client{
				entryPoint:        tt.fields.entryPoint,
				syslogHostname:    tt.fields.syslogHostname,
				defaultTag:        tt.fields.defaultTag,
				conn:              tt.fields.conn,
				ReplaceSequences:  tt.fields.ReplaceSequences,
				tls:               tt.fields.tls,
				waitGroup:         tt.fields.waitGroup,
				asyncErrors:       tt.fields.asyncErrors,
				asyncErrorsMutext: tt.fields.asyncErrorsMutext,
			}
			if err := dsc.AddReplaceSequences(tt.args.old, tt.args.new); (err != nil) != tt.wantErr {
				t.Errorf("Client.AddReplaceSequences() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_AsyncErrors(t *testing.T) {
	type fields struct {
		entryPoint        string
		syslogHostname    string
		defaultTag        string
		conn              net.Conn
		ReplaceSequences  map[string]string
		tls               *tlsSetup
		waitGroup         sync.WaitGroup
		asyncErrors       map[string]error
		asyncErrorsMutext sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   map[string]error
	}{
		{
			"Return asyncMap",
			fields{
				asyncErrors: map[string]error{"id1": fmt.Errorf("Error 1")},
			},
			map[string]error{"id1": fmt.Errorf("Error 1")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := &Client{
				entryPoint:        tt.fields.entryPoint,
				syslogHostname:    tt.fields.syslogHostname,
				defaultTag:        tt.fields.defaultTag,
				conn:              tt.fields.conn,
				ReplaceSequences:  tt.fields.ReplaceSequences,
				tls:               tt.fields.tls,
				waitGroup:         tt.fields.waitGroup,
				asyncErrors:       tt.fields.asyncErrors,
				asyncErrorsMutext: tt.fields.asyncErrorsMutext,
			}
			if got := dsc.AsyncErrors(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.AsyncErrors() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_WaitForPendingAsyngMessages(t *testing.T) {
	type fields struct {
		entryPoint        string
		syslogHostname    string
		defaultTag        string
		conn              net.Conn
		ReplaceSequences  map[string]string
		tls               *tlsSetup
		waitGroup         sync.WaitGroup
		asyncErrors       map[string]error
		asyncErrorsMutext sync.Mutex
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			"None element to wait",
			fields{},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := &Client{
				entryPoint:        tt.fields.entryPoint,
				syslogHostname:    tt.fields.syslogHostname,
				defaultTag:        tt.fields.defaultTag,
				conn:              tt.fields.conn,
				ReplaceSequences:  tt.fields.ReplaceSequences,
				tls:               tt.fields.tls,
				waitGroup:         tt.fields.waitGroup,
				asyncErrors:       tt.fields.asyncErrors,
				asyncErrorsMutext: tt.fields.asyncErrorsMutext,
			}
			if err := dsc.WaitForPendingAsyngMessages(); (err != nil) != tt.wantErr {
				t.Errorf("Client.WaitForPendingAsyngMessages() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_SendWTagAsync(t *testing.T) {
	type args struct {
		t string
		m string
	}
	tests := []struct {
		name   string
		client *Client
		args   args
		want   *regexp.Regexp
	}{
		{
			"Expected id pattern",
			func() *Client {
				sender, _ := NewDevoSender("udp://example.org:80") // real public service which we can stablish udp connection
				return sender
			}(),
			args{
				t: "tag",
				m: "message",
			},
			regexp.MustCompile(`^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := tt.client
			if got := dsc.SendWTagAsync(tt.args.t, tt.args.m); !tt.want.Match([]byte(got)) {
				t.Errorf("Client.SendWTagAsync() = %v, want matching with %v", got, tt.want.String())
			}
		})
	}
}

func TestClient_SendAsync(t *testing.T) {
	type args struct {
		m string
	}
	tests := []struct {
		name   string
		client *Client
		args   args
		want   *regexp.Regexp
	}{
		{
			"Expected id pattern",
			func() *Client {
				sender, _ := NewDevoSender("udp://example.org:80") // real public service which we can stablish udp connection
				return sender
			}(),
			args{
				m: "message",
			},
			regexp.MustCompile(`^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := tt.client
			if got := dsc.SendAsync(tt.args.m); !tt.want.Match([]byte(got)) {
				t.Errorf("Client.SendWTagAsync() = %v, want matching with %v", got, tt.want.String())
			}
		})
	}
}
