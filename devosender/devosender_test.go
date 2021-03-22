package devosender

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"reflect"
	"regexp"
	"sync"
	"testing"
	"time"
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
		tcp               tcpConfig
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
				tcp: tcpConfig{
					tcpDialer: &net.Dialer{},
				},
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
				tcp:               tt.fields.tcp,
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

func TestClientBuilder_EntryPoint(t *testing.T) {
	type fields struct {
		entrypoint            string
		key                   []byte
		cert                  []byte
		chain                 []byte
		keyFileName           string
		certFileName          string
		chainFileName         *string
		tlsInsecureSkipVerify bool
		tlsRenegotiation      tls.RenegotiationSupport
	}
	type args struct {
		entrypoint string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ClientBuilder
	}{
		{
			"Set entrypoint",
			fields{
				"old entrypoint",
				nil,
				nil,
				nil,
				"",
				"",
				nil,
				false,
				tls.RenegotiateNever,
			},
			args{"new entrypoint"},
			&ClientBuilder{
				entrypoint: "new entrypoint",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsb := &ClientBuilder{
				entrypoint:            tt.fields.entrypoint,
				key:                   tt.fields.key,
				cert:                  tt.fields.cert,
				chain:                 tt.fields.chain,
				keyFileName:           tt.fields.keyFileName,
				certFileName:          tt.fields.certFileName,
				chainFileName:         tt.fields.chainFileName,
				tlsInsecureSkipVerify: tt.fields.tlsInsecureSkipVerify,
				tlsRenegotiation:      tt.fields.tlsRenegotiation,
			}
			if got := dsb.EntryPoint(tt.args.entrypoint); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClientBuilder.EntryPoint() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientBuilder_TLSFiles(t *testing.T) {
	type fields struct {
		entrypoint            string
		key                   []byte
		cert                  []byte
		chain                 []byte
		keyFileName           string
		certFileName          string
		chainFileName         *string
		tlsInsecureSkipVerify bool
		tlsRenegotiation      tls.RenegotiationSupport
	}
	type args struct {
		keyFileName   string
		certFileName  string
		chainFileName *string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ClientBuilder
	}{
		{
			"Set without chainFileName",
			fields{
				"",
				nil,
				nil,
				nil,
				"old key file name",
				"old cert file name",
				func() *string {
					oldChain := "old chain file name"
					return &oldChain
				}(),
				false,
				tls.RenegotiateNever,
			},
			args{
				"new key file name",
				"new cert file name",
				nil,
			},
			&ClientBuilder{
				keyFileName:   "new key file name",
				certFileName:  "new cert file name",
				chainFileName: nil,
			},
		},
		{
			"Set all",
			fields{
				"",
				nil,
				nil,
				nil,
				"old key file name",
				"old cert file name",
				func() *string {
					oldChain := "old chain file name"
					return &oldChain
				}(),
				false,
				tls.RenegotiateNever,
			},
			args{
				"new key file name",
				"new cert file name",
				func() *string {
					chain := "new chain file name"
					return &chain
				}(),
			},
			&ClientBuilder{
				keyFileName:  "new key file name",
				certFileName: "new cert file name",
				chainFileName: func() *string {
					chain := "new chain file name"
					return &chain
				}(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsb := &ClientBuilder{
				entrypoint:            tt.fields.entrypoint,
				key:                   tt.fields.key,
				cert:                  tt.fields.cert,
				chain:                 tt.fields.chain,
				keyFileName:           tt.fields.keyFileName,
				certFileName:          tt.fields.certFileName,
				chainFileName:         tt.fields.chainFileName,
				tlsInsecureSkipVerify: tt.fields.tlsInsecureSkipVerify,
				tlsRenegotiation:      tt.fields.tlsRenegotiation,
			}
			if got := dsb.TLSFiles(tt.args.keyFileName, tt.args.certFileName, tt.args.chainFileName); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClientBuilder.TLSFiles() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientBuilder_TLSCerts(t *testing.T) {
	type fields struct {
		entrypoint            string
		key                   []byte
		cert                  []byte
		chain                 []byte
		keyFileName           string
		certFileName          string
		chainFileName         *string
		tlsInsecureSkipVerify bool
		tlsRenegotiation      tls.RenegotiationSupport
	}
	type args struct {
		key   []byte
		cert  []byte
		chain []byte
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ClientBuilder
	}{
		{
			"Set nil values",
			fields{
				"",
				[]byte("old key"),
				[]byte("old cert"),
				[]byte("old chain"),
				"",
				"",
				nil,
				false,
				tls.RenegotiateNever,
			},
			args{
				nil,
				nil,
				nil,
			},
			&ClientBuilder{},
		},
		{
			"Set values",
			fields{
				"",
				[]byte("old key"),
				[]byte("old cert"),
				[]byte("old chain"),
				"",
				"",
				nil,
				false,
				tls.RenegotiateNever,
			},
			args{
				[]byte("new key"),
				[]byte("new cert"),
				[]byte("new chain"),
			},
			&ClientBuilder{
				key:   []byte("new key"),
				cert:  []byte("new cert"),
				chain: []byte("new chain"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsb := &ClientBuilder{
				entrypoint:            tt.fields.entrypoint,
				key:                   tt.fields.key,
				cert:                  tt.fields.cert,
				chain:                 tt.fields.chain,
				keyFileName:           tt.fields.keyFileName,
				certFileName:          tt.fields.certFileName,
				chainFileName:         tt.fields.chainFileName,
				tlsInsecureSkipVerify: tt.fields.tlsInsecureSkipVerify,
				tlsRenegotiation:      tt.fields.tlsRenegotiation,
			}
			if got := dsb.TLSCerts(tt.args.key, tt.args.cert, tt.args.chain); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClientBuilder.TLSCerts() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientBuilder_TLSInsecureSkipVerify(t *testing.T) {
	type fields struct {
		entrypoint            string
		key                   []byte
		cert                  []byte
		chain                 []byte
		keyFileName           string
		certFileName          string
		chainFileName         *string
		tlsInsecureSkipVerify bool
		tlsRenegotiation      tls.RenegotiationSupport
	}
	type args struct {
		insecureSkipVerify bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ClientBuilder
	}{
		{
			"Set insecureSkipVerify to true",
			fields{
				"",
				nil,
				nil,
				nil,
				"",
				"",
				nil,
				false,
				tls.RenegotiateNever,
			},
			args{
				true,
			},
			&ClientBuilder{
				tlsInsecureSkipVerify: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsb := &ClientBuilder{
				entrypoint:            tt.fields.entrypoint,
				key:                   tt.fields.key,
				cert:                  tt.fields.cert,
				chain:                 tt.fields.chain,
				keyFileName:           tt.fields.keyFileName,
				certFileName:          tt.fields.certFileName,
				chainFileName:         tt.fields.chainFileName,
				tlsInsecureSkipVerify: tt.fields.tlsInsecureSkipVerify,
				tlsRenegotiation:      tt.fields.tlsRenegotiation,
			}
			if got := dsb.TLSInsecureSkipVerify(tt.args.insecureSkipVerify); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClientBuilder.TLSInsecureSkipVerify() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientBuilder_TLSRenegotiation(t *testing.T) {
	type fields struct {
		entrypoint            string
		key                   []byte
		cert                  []byte
		chain                 []byte
		keyFileName           string
		certFileName          string
		chainFileName         *string
		tlsInsecureSkipVerify bool
		tlsRenegotiation      tls.RenegotiationSupport
	}
	type args struct {
		renegotiation tls.RenegotiationSupport
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ClientBuilder
	}{
		{
			"Set tlsRenegotitation to RenegotiateOnceAsClient value",
			fields{
				"",
				nil,
				nil,
				nil,
				"",
				"",
				nil,
				false,
				tls.RenegotiateNever,
			},
			args{
				tls.RenegotiateOnceAsClient,
			},
			&ClientBuilder{
				tlsRenegotiation: tls.RenegotiateOnceAsClient,
			},
		},
		{
			"Set tlsRenegotitation to RenegotiateFreelyAsClient value",
			fields{
				"",
				nil,
				nil,
				nil,
				"",
				"",
				nil,
				false,
				tls.RenegotiateNever,
			},
			args{
				tls.RenegotiateFreelyAsClient,
			},
			&ClientBuilder{
				tlsRenegotiation: tls.RenegotiateFreelyAsClient,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsb := &ClientBuilder{
				entrypoint:            tt.fields.entrypoint,
				key:                   tt.fields.key,
				cert:                  tt.fields.cert,
				chain:                 tt.fields.chain,
				keyFileName:           tt.fields.keyFileName,
				certFileName:          tt.fields.certFileName,
				chainFileName:         tt.fields.chainFileName,
				tlsInsecureSkipVerify: tt.fields.tlsInsecureSkipVerify,
				tlsRenegotiation:      tt.fields.tlsRenegotiation,
			}
			if got := dsb.TLSRenegotiation(tt.args.renegotiation); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClientBuilder.TLSRenegotiation() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientBuilder_DevoCentralEntryPoint(t *testing.T) {
	type fields struct {
		entrypoint            string
		key                   []byte
		cert                  []byte
		chain                 []byte
		keyFileName           string
		certFileName          string
		chainFileName         *string
		tlsInsecureSkipVerify bool
		tlsRenegotiation      tls.RenegotiationSupport
		tcpTimeout            time.Duration
		tcpKeepAlive          time.Duration
		connExpiration        time.Duration
	}
	type args struct {
		relay ClienBuilderDevoCentralRelay
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ClientBuilder
	}{
		{
			"Set EU Devo entrypoint",
			fields{},
			args{ClientBuilderRelayEU},
			&ClientBuilder{
				entrypoint: DevoCentralRelayEU,
			},
		},
		{
			"Set US Devo entrypoint",
			fields{},
			args{ClientBuilderRelayUS},
			&ClientBuilder{
				entrypoint: DevoCentralRelayUS,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsb := &ClientBuilder{
				entrypoint:            tt.fields.entrypoint,
				key:                   tt.fields.key,
				cert:                  tt.fields.cert,
				chain:                 tt.fields.chain,
				keyFileName:           tt.fields.keyFileName,
				certFileName:          tt.fields.certFileName,
				chainFileName:         tt.fields.chainFileName,
				tlsInsecureSkipVerify: tt.fields.tlsInsecureSkipVerify,
				tlsRenegotiation:      tt.fields.tlsRenegotiation,
				tcpTimeout:            tt.fields.tcpTimeout,
				tcpKeepAlive:          tt.fields.tcpKeepAlive,
				connExpiration:        tt.fields.connExpiration,
			}
			if got := dsb.DevoCentralEntryPoint(tt.args.relay); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClientBuilder.DevoCentralEntryPoint() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientBuilder_TCPTimeout(t *testing.T) {
	type fields struct {
		entrypoint            string
		key                   []byte
		cert                  []byte
		chain                 []byte
		keyFileName           string
		certFileName          string
		chainFileName         *string
		tlsInsecureSkipVerify bool
		tlsRenegotiation      tls.RenegotiationSupport
		tcpTimeout            time.Duration
		tcpKeepAlive          time.Duration
		connExpiration        time.Duration
	}
	type args struct {
		t time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ClientBuilder
	}{
		{
			"Set TCP timeout",
			fields{},
			args{time.Minute},
			&ClientBuilder{
				tcpTimeout: time.Minute,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsb := &ClientBuilder{
				entrypoint:            tt.fields.entrypoint,
				key:                   tt.fields.key,
				cert:                  tt.fields.cert,
				chain:                 tt.fields.chain,
				keyFileName:           tt.fields.keyFileName,
				certFileName:          tt.fields.certFileName,
				chainFileName:         tt.fields.chainFileName,
				tlsInsecureSkipVerify: tt.fields.tlsInsecureSkipVerify,
				tlsRenegotiation:      tt.fields.tlsRenegotiation,
				tcpTimeout:            tt.fields.tcpTimeout,
				tcpKeepAlive:          tt.fields.tcpKeepAlive,
				connExpiration:        tt.fields.connExpiration,
			}
			if got := dsb.TCPTimeout(tt.args.t); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClientBuilder.TCPTimeout() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientBuilder_TCPKeepAlive(t *testing.T) {
	type fields struct {
		entrypoint            string
		key                   []byte
		cert                  []byte
		chain                 []byte
		keyFileName           string
		certFileName          string
		chainFileName         *string
		tlsInsecureSkipVerify bool
		tlsRenegotiation      tls.RenegotiationSupport
		tcpTimeout            time.Duration
		tcpKeepAlive          time.Duration
		connExpiration        time.Duration
	}
	type args struct {
		t time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ClientBuilder
	}{
		{
			"Set KeepAlive duration",
			fields{},
			args{time.Minute * 2},
			&ClientBuilder{
				tcpKeepAlive: time.Minute * 2,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsb := &ClientBuilder{
				entrypoint:            tt.fields.entrypoint,
				key:                   tt.fields.key,
				cert:                  tt.fields.cert,
				chain:                 tt.fields.chain,
				keyFileName:           tt.fields.keyFileName,
				certFileName:          tt.fields.certFileName,
				chainFileName:         tt.fields.chainFileName,
				tlsInsecureSkipVerify: tt.fields.tlsInsecureSkipVerify,
				tlsRenegotiation:      tt.fields.tlsRenegotiation,
				tcpTimeout:            tt.fields.tcpTimeout,
				tcpKeepAlive:          tt.fields.tcpKeepAlive,
				connExpiration:        tt.fields.connExpiration,
			}
			if got := dsb.TCPKeepAlive(tt.args.t); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClientBuilder.TCPKeepAlive() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientBuilder_ConnectionExpiration(t *testing.T) {
	type fields struct {
		entrypoint            string
		key                   []byte
		cert                  []byte
		chain                 []byte
		keyFileName           string
		certFileName          string
		chainFileName         *string
		tlsInsecureSkipVerify bool
		tlsRenegotiation      tls.RenegotiationSupport
		tcpTimeout            time.Duration
		tcpKeepAlive          time.Duration
		connExpiration        time.Duration
	}
	type args struct {
		t time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ClientBuilder
	}{
		{
			"Set Connection expiration",
			fields{},
			args{time.Minute * 3},
			&ClientBuilder{
				connExpiration: time.Minute * 3,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsb := &ClientBuilder{
				entrypoint:            tt.fields.entrypoint,
				key:                   tt.fields.key,
				cert:                  tt.fields.cert,
				chain:                 tt.fields.chain,
				keyFileName:           tt.fields.keyFileName,
				certFileName:          tt.fields.certFileName,
				chainFileName:         tt.fields.chainFileName,
				tlsInsecureSkipVerify: tt.fields.tlsInsecureSkipVerify,
				tlsRenegotiation:      tt.fields.tlsRenegotiation,
				tcpTimeout:            tt.fields.tcpTimeout,
				tcpKeepAlive:          tt.fields.tcpKeepAlive,
				connExpiration:        tt.fields.connExpiration,
			}
			if got := dsb.ConnectionExpiration(tt.args.t); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClientBuilder.ConnectionExpiration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseDevoCentralEntrySite(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		args    args
		want    ClienBuilderDevoCentralRelay
		wantErr bool
	}{
		{
			"Parse US entrypoint",
			args{"US"},
			ClientBuilderRelayUS,
			false,
		},
		{
			"Parse us entrypoint",
			args{"US"},
			ClientBuilderRelayUS,
			false,
		},
		{
			"Parse EU entrypoint",
			args{"EU"},
			ClientBuilderRelayEU,
			false,
		},
		{
			"Parse eu entrypoint",
			args{"eu"},
			ClientBuilderRelayEU,
			false,
		},
		{
			"Parse invalid entrypoint",
			args{"THIS IS NOT VALID"},
			0,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDevoCentralEntrySite(tt.args.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDevoCentralEntrySite() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseDevoCentralEntrySite() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientBuilder_Build(t *testing.T) {
	type fields struct {
		entrypoint            string
		key                   []byte
		cert                  []byte
		chain                 []byte
		keyFileName           string
		certFileName          string
		chainFileName         *string
		tlsInsecureSkipVerify bool
		tlsRenegotiation      tls.RenegotiationSupport
	}
	tests := []struct {
		name    string
		fields  fields
		want    *Client
		wantErr bool
	}{
		{
			"Clean udp connection",
			fields{
				entrypoint: "udp://example.org:80",
			},
			func() *Client {
				r, _ := NewDevoSender("udp://example.org:80")
				return r
			}(),
			false,
		},
		{
			"Error tcp connection",
			fields{
				entrypoint: "tcp://dosnotexiststhishostname:1234",
			},
			nil,
			true,
		},
		{
			"Error keyFile and certFile",
			fields{
				entrypoint:   "udp://example.org:80",
				keyFileName:  "/ensure/this/file/does/not/exists",
				certFileName: "/ensure/this/file/does/not/exists",
			},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsb := &ClientBuilder{
				entrypoint:            tt.fields.entrypoint,
				key:                   tt.fields.key,
				cert:                  tt.fields.cert,
				chain:                 tt.fields.chain,
				keyFileName:           tt.fields.keyFileName,
				certFileName:          tt.fields.certFileName,
				chainFileName:         tt.fields.chainFileName,
				tlsInsecureSkipVerify: tt.fields.tlsInsecureSkipVerify,
				tlsRenegotiation:      tt.fields.tlsRenegotiation,
			}
			got, err := dsb.Build()
			if (err != nil) != tt.wantErr {
				t.Errorf("ClientBuilder.Build() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Fields to be ignored assigned from source
			if got != nil {
				if got.conn != nil {
					got.conn.Close()
				}
				got.conn = tt.want.conn
				got.connectionUsedTimestamp = tt.want.connectionUsedTimestamp
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClientBuilder.Build() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
