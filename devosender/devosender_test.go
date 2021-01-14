package devosender

import (
	"net"
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
			if err := dsc.makeConnection(); (err != nil) != tt.wantErr {
				t.Errorf("Client.makeConnection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
