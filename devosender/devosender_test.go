package devosender

import (
	"fmt"
	"net"
	"os"
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

func TestClient_SendWTag(t *testing.T) {
	type args struct {
		t string
		m string
	}
	tests := []struct {
		name    string
		client  *Client
		args    args
		wantErr bool
	}{
		{
			"Send using udp",
			func() *Client {
				sender, _ := NewDevoSender("udp://example.org:80") // real public service which we can stablish udp connection
				return sender
			}(),
			args{
				t: "tag",
				m: "message",
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := tt.client
			if err := dsc.SendWTag(tt.args.t, tt.args.m); (err != nil) != tt.wantErr {
				t.Errorf("Client.SendWTag() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_Send(t *testing.T) {
	type args struct {
		m string
	}
	tests := []struct {
		name    string
		client  *Client
		args    args
		wantErr bool
	}{
		{
			"Send using udp",
			func() *Client {
				sender, _ := NewDevoSender("udp://example.org:80") // real public service which we can stablish udp connection
				sender.SetDefaultTag("tag")
				return sender
			}(),
			args{
				m: "message",
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := tt.client
			if err := dsc.Send(tt.args.m); (err != nil) != tt.wantErr {
				t.Errorf("Client.SendWTag() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_SetDefaultTag(t *testing.T) {
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
		t string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		wantTag string
	}{
		{
			"Set empty tag",
			fields{},
			args{},
			true,
			"",
		},
		{
			"Set tag",
			fields{},
			args{
				t: "new.tag",
			},
			false,
			"new.tag",
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
			if err := dsc.SetDefaultTag(tt.args.t); (err != nil) != tt.wantErr {
				t.Errorf("Client.SetDefaultTag() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantTag != dsc.defaultTag {
				t.Errorf("Client.SetDefaultTag() defaultTag = %s, wantTag %s", dsc.defaultTag, tt.wantTag)
			}
		})
	}
}

func TestClient_SetSyslogHostName(t *testing.T) {
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
		host string
	}
	tests := []struct {
		name               string
		fields             fields
		args               args
		wantSyslogHostname string
	}{
		{
			"Empty hostname parameter",
			fields{},
			args{},
			func() string {
				h, _ := os.Hostname()
				return h
			}(),
		},
		{
			"Set host parameter",
			fields{},
			args{"tarari"},
			"tarari",
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
			dsc.SetSyslogHostName(tt.args.host)
			if tt.wantSyslogHostname != dsc.syslogHostname {
				t.Errorf("Client.SetDefaultTag() syslogHostname = %s, want = %s", dsc.syslogHostname, tt.wantSyslogHostname)
			}
		})
	}
}

func TestClient_Close(t *testing.T) {
	tests := []struct {
		name    string
		client  *Client
		wantErr bool
	}{
		{
			"Empty connection",
			&Client{},
			true,
		},
		{
			"Close connection",
			func() *Client {
				s, err := NewDevoSender("udp://example.org:80")
				if err != nil {
					fmt.Println("UNEXPECTED error found", err)
				}

				return s
			}(),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := tt.client
			if err := dsc.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Client.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_Write(t *testing.T) {
	type args struct {
		p []byte
	}
	tests := []struct {
		name    string
		client  *Client
		args    args
		wantN   int
		wantErr bool
	}{
		{
			"Send using udp",
			func() *Client {
				sender, _ := NewDevoSender("udp://example.org:80") // real public service which we can stablish udp connection
				sender.SetDefaultTag("tag")
				return sender
			}(),
			args{
				p: []byte("message"),
			},
			len("message"),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := tt.client
			gotN, err := dsc.Write(tt.args.p)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotN != tt.wantN {
				t.Errorf("Client.Write() = %v, want %v", gotN, tt.wantN)
			}
		})
	}
}

func TestClient_PurgeAsyncErrors(t *testing.T) {
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
	}{
		{
			"nil asyncErrors",
			fields{},
		},
		{
			"Clean asyncErrors",
			fields{
				asyncErrors: map[string]error{
					"a": fmt.Errorf("demo error"),
				},
			},
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
			dsc.PurgeAsyncErrors()
			if dsc.asyncErrors != nil && len(dsc.asyncErrors) > 0 {
				t.Errorf("PurgeAsyncErrors() async errors still existing: %d", len(dsc.asyncErrors))
			}
		})
	}
}

func TestClient_GetEntryPoint(t *testing.T) {
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
		want   string
	}{
		{
			"Return entrypoint",
			fields{
				entryPoint: "entrypoint",
			},
			"entrypoint",
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
			if got := dsc.GetEntryPoint(); got != tt.want {
				t.Errorf("Client.GetEntryPoint() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewClientBuilder(t *testing.T) {
	tests := []struct {
		name string
		want *ClientBuilder
	}{
		{
			"Builder instantiate with default values",
			&ClientBuilder{
				tlsRenegotiation:      tls.RenegotiateNever,
				tlsInsecureSkipVerify: false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewClientBuilder(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewClientBuilder() = %v, want %v", got, tt.want)
			}
		})
	}
}
