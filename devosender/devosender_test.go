package devosender

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"reflect"
	"regexp"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/cyberluisda/devo-go/devosender/compressor"
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
		asyncItems        map[string]interface{}
		asyncItemsMutext  sync.Mutex
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
				asyncItems:        tt.fields.asyncItems,
				asyncItemsMutext:  tt.fields.asyncItemsMutext,
			}
			if err := dsc.makeConnection(); (err != nil) != tt.wantErr {
				t.Errorf("Client.makeConnection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_AddReplaceSequences_nil(t *testing.T) {
	var dsc *Client
	wantErr := ErrNilPointerReceiver
	if err := dsc.AddReplaceSequences("old", "new"); err != wantErr {
		t.Errorf("Client.AddReplaceSequences() with nil pointer: error = %v, wantErr %v", err, wantErr)
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
		asyncItems        map[string]interface{}
		asyncItemsMutext  sync.Mutex
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
				asyncItems:        tt.fields.asyncItems,
				asyncItemsMutext:  tt.fields.asyncItemsMutext,
			}
			if err := dsc.AddReplaceSequences(tt.args.old, tt.args.new); (err != nil) != tt.wantErr {
				t.Errorf("Client.AddReplaceSequences() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_AsyncErrors_nil(t *testing.T) {
	var dsc *Client
	want := map[string]error{"": ErrNilPointerReceiver}
	if got := dsc.AsyncErrors(); !reflect.DeepEqual(got, want) {
		t.Errorf("Client.AsyncErrors() with nil pointer: got = %v, want %v", got, want)
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
		asyncItems        map[string]interface{}
		asyncItemsMutext  sync.Mutex
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
				asyncItems:        tt.fields.asyncItems,
				asyncItemsMutext:  tt.fields.asyncItemsMutext,
			}
			if got := dsc.AsyncErrors(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.AsyncErrors() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_AsyncErrorsIds(t *testing.T) {
	tests := []struct {
		name string
		dsc  *Client
		want []string
	}{
		{
			"Nil",
			nil,
			nil,
		},
		{
			"Nil error list",
			&Client{},
			nil,
		},
		{
			"Empty error list",
			&Client{
				asyncErrors: map[string]error{},
			},
			nil,
		},
		{
			"IDs with errors",
			&Client{
				asyncErrors: map[string]error{
					"ID-1": errors.New("test error"),
					"ID-2": errors.New("test error 2"),
					"ID-3": errors.New("test error 3"),
				},
			},
			[]string{"ID-1", "ID-2", "ID-3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.dsc.AsyncErrorsIds()
			sort.Strings(got)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.AsyncErrorsIds() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_AsyncError(t *testing.T) {
	type args struct {
		id string
	}
	tests := []struct {
		name    string
		dsc     *Client
		args    args
		want    bool
		wantErr bool
	}{
		{
			"Nil",
			nil,
			args{"any id"},
			false,
			true,
		},
		{
			"Empty error list",
			&Client{},
			args{"any id"},
			false,
			false,
		},
		{
			"Id does not found",
			&Client{
				asyncErrors: map[string]error{
					"ID-1": errors.New("test error"),
				},
			},
			args{"any id"},
			false,
			false,
		},
		{
			"ID found",
			&Client{
				asyncErrors: map[string]error{
					"ID-1": errors.New("test error"),
				},
			},
			args{"ID-1"},
			true,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.dsc.AsyncError(tt.args.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.AsyncError() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Client.AsyncError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_AsyncErrorsNumber_nil(t *testing.T) {
	var dsc *Client
	want := 0
	if got := dsc.AsyncErrorsNumber(); got != want {
		t.Errorf("Client.AsyncErrorsNumber() with nil pointer: got = %v, want %v", got, want)
	}
}

func TestClient_AsyncErrorsNumber(t *testing.T) {
	type fields struct {
		asyncErrors       map[string]error
		asyncErrorsMutext sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{
			"Empty",
			fields{},
			0,
		},
		{
			"With errors",
			fields{
				asyncErrors: map[string]error{
					"12324":    nil,
					"asdfsadf": nil,
				},
			},
			2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := &Client{
				asyncErrors:       tt.fields.asyncErrors,
				asyncErrorsMutext: tt.fields.asyncErrorsMutext,
			}
			if got := dsc.AsyncErrorsNumber(); got != tt.want {
				t.Errorf("Client.AsyncErrorsNumber() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_WaitForPendingAsyncMessages_nil(t *testing.T) {
	var dsc *Client
	wantErr := ErrNilPointerReceiver
	if err := dsc.WaitForPendingAsyncMessages(); err != wantErr {
		t.Errorf("Client.WaitForPendingAsyncMessages() with nil pointer: error = %v, wantErr %v", err, wantErr)
	}
}

func TestClient_WaitForPendingAsyncMessages(t *testing.T) {
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
		asyncItems        map[string]interface{}
		asyncItemsMutext  sync.Mutex
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
				asyncItems:        tt.fields.asyncItems,
				asyncItemsMutext:  tt.fields.asyncItemsMutext,
			}
			if err := dsc.WaitForPendingAsyncMessages(); (err != nil) != tt.wantErr {
				t.Errorf("Client.WaitForPendingAsyncMessages() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_WaitForPendingAsyncMsgsOrTimeout_nil(t *testing.T) {
	var dsc *Client
	wantErr := ErrNilPointerReceiver
	if err := dsc.WaitForPendingAsyncMsgsOrTimeout(0); err != wantErr {
		t.Errorf("Client.WaitForPendingAsyncMsgsOrTimeout() with nil pointer: error = %v, wantErr %v", err, wantErr)
	}
}

func TestClient_WaitForPendingAsyncMsgsOrTimeout(t *testing.T) {
	type fields struct {
		waitGroup sync.WaitGroup
	}
	type args struct {
		timeout time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"None element to wait",
			fields{},
			args{time.Second * 1},
			false,
		},
		{
			"Timeout reached",
			fields{
				waitGroup: func() sync.WaitGroup {
					r := sync.WaitGroup{}
					r.Add(1)
					return r
				}(),
			},
			args{
				time.Millisecond * 50,
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := &Client{
				waitGroup: tt.fields.waitGroup,
			}
			if err := dsc.WaitForPendingAsyncMsgsOrTimeout(tt.args.timeout); (err != nil) != tt.wantErr {
				t.Errorf("Client.WaitForPendingAsyncMsgsOrTimeout() error = %v, wantErr %v", err, tt.wantErr)
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
			"Nil client",
			nil,
			args{
				t: "tag",
				m: "message",
			},
			regexp.MustCompile("^$"),
		},
		{
			"Expected id pattern",
			func() *Client {
				r, _ := NewClientBuilder().EntryPoint("udp://example.org:80").Build() // real public service which we can stablish udp connection
				return r
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

func TestClient_SendWTagAndCompressorAsync(t *testing.T) {
	type args struct {
		t string
		m string
		c *compressor.Compressor
	}
	tests := []struct {
		name   string
		client *Client
		args   args
		want   *regexp.Regexp
	}{
		{
			"Nil client",
			nil,
			args{
				t: "tag",
				m: "message",
			},
			regexp.MustCompile("^$"),
		},
		{
			"Nil compressor",
			func() *Client {
				r, _ := NewClientBuilder().EntryPoint("udp://example.org:80").Build() // real public service which we can stablish udp connection
				return r
			}(),
			args{
				t: "tag",
				m: "message",
			},
			regexp.MustCompile(`^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$`),
		},
		{
			"With Compressor",
			func() *Client {
				r, _ := NewClientBuilder().EntryPoint("udp://example.org:80").Build() // real public service which we can stablish udp connection
				return r
			}(),
			args{
				t: "tag",
				m: "message",
				c: &compressor.Compressor{Algorithm: compressor.CompressorZlib, MinimumSize: 0},
			},
			regexp.MustCompile(`^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := tt.client
			if got := dsc.SendWTagAndCompressorAsync(tt.args.t, tt.args.m, tt.args.c); !tt.want.Match([]byte(got)) {
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
			"Nil Client",
			nil,
			args{
				m: "message",
			},
			regexp.MustCompile("^$"),
		},
		{
			"Expected id pattern",
			func() *Client {
				r, _ := NewClientBuilder().EntryPoint("udp://example.org:80").Build() // real public service which we can stablish udp connection
				return r
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

func TestClient_SendWTag_nil(t *testing.T) {
	var dsc *Client
	wantErr := ErrNilPointerReceiver
	if err := dsc.SendWTag("tag", "msg"); err != wantErr {
		t.Errorf("Client.SendWTag() with nil pointer: error = %v, wantErr %v", err, wantErr)
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
				r, _ := NewClientBuilder().EntryPoint("udp://example.org:80").Build() // real public service which we can stablish udp connection
				return r
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

func TestClient_SendWTagAndCompressor_nil(t *testing.T) {
	var dsc *Client
	wantErr := ErrNilPointerReceiver
	if err := dsc.SendWTagAndCompressor("tag", "msg", nil); err != wantErr {
		t.Errorf("Client.SendWTagAndCompressor() with nil pointer: error = %v, wantErr %v", err, wantErr)
	}
}

func TestClient_SendWTagAndCompressor(t *testing.T) {
	type args struct {
		t string
		m string
		c *compressor.Compressor
	}
	tests := []struct {
		name    string
		client  *Client
		args    args
		wantErr bool
	}{
		{
			"Nil compressor",
			func() *Client {
				r, _ := NewClientBuilder().EntryPoint("udp://example.org:80").Build() // real public service which we can stablish udp connection
				return r
			}(),
			args{
				t: "tag",
				m: "message",
			},
			false,
		},
		{
			"Wtih compressor",
			func() *Client {
				r, _ := NewClientBuilder().EntryPoint("udp://example.org:80").Build() // real public service which we can stablish udp connection
				return r
			}(),
			args{
				t: "tag",
				m: "message",
				c: &compressor.Compressor{Algorithm: compressor.CompressorGzip, MinimumSize: 0},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := tt.client
			if err := dsc.SendWTagAndCompressor(tt.args.t, tt.args.m, tt.args.c); (err != nil) != tt.wantErr {
				t.Errorf("Client.SendWTag() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_Send_nil(t *testing.T) {
	var dsc *Client
	wantErr := ErrNilPointerReceiver
	if err := dsc.Send("msg"); err != wantErr {
		t.Errorf("Client.Send() with nil pointer: error = %v, wantErr %v", err, wantErr)
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
				r, _ := NewClientBuilder().EntryPoint("udp://example.org:80").Build() // real public service which we can stablish udp connection
				r.SetDefaultTag("tag")
				return r
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

func TestClient_SetDefaultTag_nil(t *testing.T) {
	var dsc *Client
	wantErr := ErrNilPointerReceiver
	if err := dsc.SetDefaultTag("tag"); err != wantErr {
		t.Errorf("Client.SetDefaultTag() with nil pointer: error = %v, wantErr %v", err, wantErr)
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
		asyncItems        map[string]interface{}
		asyncItemsMutext  sync.Mutex
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
				asyncItems:        tt.fields.asyncItems,
				asyncItemsMutext:  tt.fields.asyncItemsMutext,
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

func TestClient_SetSyslogHostName_nil(t *testing.T) {
	var dsc *Client
	dsc.SetSyslogHostName("host")
	if dsc != nil {
		t.Errorf("Client.SetSyslogHostName() with not nil pointer failed")
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
		asyncItems        map[string]interface{}
		asyncItemsMutext  sync.Mutex
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
				asyncItems:        tt.fields.asyncItems,
				asyncItemsMutext:  tt.fields.asyncItemsMutext,
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
			"Nil client",
			nil,
			true,
		},
		{
			"Empty connection",
			&Client{},
			true,
		},
		{
			"Close connection",
			func() *Client {
				r, err := NewClientBuilder().EntryPoint("udp://example.org:80").Build()
				if err != nil {
					fmt.Println("UNEXPECTED error found", err)
				}

				return r
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
			"Nil client",
			nil,
			args{[]byte{}},
			0,
			true,
		},
		{
			"Send using udp",
			func() *Client {
				r, _ := NewClientBuilder().EntryPoint("udp://example.org:80").Build() // real public service which we can stablish udp connection
				r.SetDefaultTag("tag")
				return r
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

func TestClient_PurgeAsyncErrors_nil(t *testing.T) {
	var dsc *Client
	dsc.PurgeAsyncErrors()
	if dsc != nil {
		t.Errorf("Client.PurgeAsyncErrors() with not nil pointer failed")
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
		asyncItems        map[string]interface{}
		asyncItemsMutext  sync.Mutex
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
				asyncItems:        tt.fields.asyncItems,
				asyncItemsMutext:  tt.fields.asyncItemsMutext,
			}
			dsc.PurgeAsyncErrors()
			if dsc.asyncErrors != nil && len(dsc.asyncErrors) > 0 {
				t.Errorf("PurgeAsyncErrors() async errors still existing: %d", len(dsc.asyncErrors))
			}
		})
	}
}

func TestClient_GetEntryPoint_nil(t *testing.T) {
	var dsc *Client
	want := ""
	if got := dsc.GetEntryPoint(); got != want {
		t.Errorf("Client.GetEntryPoint() with nil pointer: got = '%s', want '%s'", got, want)
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
		asyncItems        map[string]interface{}
		asyncItemsMutext  sync.Mutex
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
				asyncItems:        tt.fields.asyncItems,
				asyncItemsMutext:  tt.fields.asyncItemsMutext,
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
				compressorMinSize:     ClientBuilderDefaultCompressorMinSize,
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

func TestClientBuilder_TLSNoLoadPublicCAs(t *testing.T) {
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
		noLoadPublicCAs       bool
	}
	type args struct {
		noLoadPublicCAs bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ClientBuilder
	}{
		{
			"Set noLoadPublicCAs to true",
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
				false,
			},
			args{
				true,
			},
			&ClientBuilder{
				noLoadPublicCAs: true,
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
				noLoadPublicCAs:       tt.fields.noLoadPublicCAs,
			}
			if got := dsb.TLSNoLoadPublicCAs(tt.args.noLoadPublicCAs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClientBuilder.TLSNoLoadPublicCAs() = %v, want %v", got, tt.want)
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

func TestClientBuilder_DefaultCompressor(t *testing.T) {
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
		compressorAlgorithm   compressor.CompressorAlgorithm
		compressorMinSize     int
	}
	type args struct {
		c compressor.CompressorAlgorithm
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ClientBuilder
	}{
		{
			"Set CompressorAlgorithm",
			fields{},
			args{compressor.CompressorGzip},
			&ClientBuilder{
				compressorAlgorithm: compressor.CompressorGzip,
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
				compressorAlgorithm:   tt.fields.compressorAlgorithm,
				compressorMinSize:     tt.fields.compressorMinSize,
			}
			if got := dsb.DefaultCompressor(tt.args.c); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClientBuilder.DefaultCompressor() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientBuilder_CompressorMinSize(t *testing.T) {
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
		compressorAlgorithm   compressor.CompressorAlgorithm
		compressorMinSize     int
	}
	type args struct {
		s int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ClientBuilder
	}{
		{
			"Set Compressor min size",
			fields{},
			args{123},
			&ClientBuilder{
				compressorMinSize: 123,
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
				compressorAlgorithm:   tt.fields.compressorAlgorithm,
				compressorMinSize:     tt.fields.compressorMinSize,
			}
			if got := dsb.CompressorMinSize(tt.args.s); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClientBuilder.CompressorMinSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientBuilder_DefaultDevoTag(t *testing.T) {
	type fields struct {
		defaultDevoTag string
	}
	type args struct {
		t string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ClientBuilder
	}{
		{
			"Set tag",
			fields{},
			args{"test.keep.free"},
			&ClientBuilder{
				defaultDevoTag: "test.keep.free",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsb := &ClientBuilder{
				defaultDevoTag: tt.fields.defaultDevoTag,
			}
			if got := dsb.DefaultDevoTag(tt.args.t); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClientBuilder.DefaultDevoTag() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestClientBuilder_IsConnWorkingCheckPayload(t *testing.T) {
	type fields struct {
		isConnWorkingCheckPayload string
	}
	type args struct {
		s string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ClientBuilder
	}{
		{
			"Empty",
			fields{"old"},
			args{""},
			&ClientBuilder{
				isConnWorkingCheckPayload: "",
			},
		},
		{
			"Overlength value",
			fields{"old"},
			args{"lengt is greater than 4"},
			&ClientBuilder{
				isConnWorkingCheckPayload: "old",
			},
		},
		{
			"Valid value",
			fields{""},
			args{"\n"},
			&ClientBuilder{
				isConnWorkingCheckPayload: "\n",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsb := &ClientBuilder{
				isConnWorkingCheckPayload: tt.fields.isConnWorkingCheckPayload,
			}
			if got := dsb.IsConnWorkingCheckPayload(tt.args.s); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClientBuilder.IsConnWorkingCheckPayload() = %+v, want %+v", got, tt.want)
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
		entrypoint                string
		key                       []byte
		cert                      []byte
		chain                     []byte
		keyFileName               string
		certFileName              string
		chainFileName             *string
		tlsInsecureSkipVerify     bool
		tlsRenegotiation          tls.RenegotiationSupport
		tcpTimeout                time.Duration
		tcpKeepAlive              time.Duration
		connExpiration            time.Duration
		compressorAlgorithm       compressor.CompressorAlgorithm
		compressorMinSize         int
		defaultDevoTag            string
		isConnWorkingCheckPayload string
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
				return r.(*Client)
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
		{
			"Error timeout connection",
			fields{
				entrypoint: "tcp://example.org:80",
				tcpTimeout: time.Millisecond,
			},
			nil,
			true,
		},
		{
			"Error invalid tls key/cert",
			fields{
				entrypoint: "udp://example.org:80",
				key:        []byte{00, 01},
				cert:       []byte{00, 01},
			},
			nil,
			true,
		},
		{
			"TCP KeepAlive set",
			fields{
				entrypoint:   "udp://example.org:80",
				tcpKeepAlive: time.Minute,
			},
			func() *Client {
				r, _ := NewDevoSender("udp://example.org:80")
				c := r.(*Client)
				c.tcp.tcpDialer.KeepAlive = time.Minute
				return c
			}(),
			false,
		},
		{
			"ConnectionExpiration set",
			fields{
				entrypoint:     "udp://example.org:80",
				connExpiration: time.Minute * 2,
			},
			func() *Client {
				r, _ := NewDevoSender("udp://example.org:80")
				c := r.(*Client)
				c.maxTimeConnActive = time.Minute * 2
				return c
			}(),
			false,
		},
		{
			"With compressor",
			fields{
				entrypoint:          "udp://example.org:80",
				compressorAlgorithm: compressor.CompressorGzip,
				compressorMinSize:   23,
			},
			func() *Client {
				r, _ := NewDevoSender("udp://example.org:80")
				c := r.(*Client)
				c.compressor = &compressor.Compressor{Algorithm: compressor.CompressorGzip, MinimumSize: 23}
				return r.(*Client)
			}(),
			false,
		},
		{
			"With default Devo tag",
			fields{
				entrypoint:     "udp://localhost:13000",
				defaultDevoTag: "test.keep.free",
			},
			func() *Client {
				r, _ := NewDevoSender("udp://localhost:13000")
				c := r.(*Client)
				c.SetDefaultTag("test.keep.free")
				return r.(*Client)
			}(),
			false,
		},
		{
			"With isConnWorkingCheckPayload",
			fields{
				entrypoint:                "udp://localhost:13000",
				isConnWorkingCheckPayload: "\n",
			},
			func() *Client {
				r, _ := NewDevoSender("udp://localhost:13000")
				c := r.(*Client)
				c.isConnWorkingPayload = []byte("\n")

				return r.(*Client)
			}(),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsb := &ClientBuilder{
				entrypoint:                tt.fields.entrypoint,
				key:                       tt.fields.key,
				cert:                      tt.fields.cert,
				chain:                     tt.fields.chain,
				keyFileName:               tt.fields.keyFileName,
				certFileName:              tt.fields.certFileName,
				chainFileName:             tt.fields.chainFileName,
				tlsInsecureSkipVerify:     tt.fields.tlsInsecureSkipVerify,
				tlsRenegotiation:          tt.fields.tlsRenegotiation,
				tcpTimeout:                tt.fields.tcpTimeout,
				tcpKeepAlive:              tt.fields.tcpKeepAlive,
				connExpiration:            tt.fields.connExpiration,
				compressorAlgorithm:       tt.fields.compressorAlgorithm,
				compressorMinSize:         tt.fields.compressorMinSize,
				defaultDevoTag:            tt.fields.defaultDevoTag,
				isConnWorkingCheckPayload: tt.fields.isConnWorkingCheckPayload,
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

func TestClientBuilder_Build__TLSNoLoadPublicCAs(t *testing.T) {

	key, cert, _, certCA, err := createCerts()
	if err != nil {
		t.Errorf("While generate Devo domain key and cert and CA cert")
		return
	}

	c, err := NewClientBuilder().
		EntryPoint("tcp://example.com:80").
		TLSCerts(
			key,
			cert,
			[]byte{},
		).
		TLSNoLoadPublicCAs(true).
		Build()
	if err != nil {
		t.Errorf("While build client and TLSNoLoadPublicCAs=true: %v", err)
		return
	}

	sysCerts, err := x509.SystemCertPool()
	if err != nil {
		t.Errorf("While load system certs to compare with RootCAs: %v", err)
		return
	}
	if c.tls.tlsConfig.RootCAs.Equal(sysCerts) {
		t.Errorf("ClientBuilder.Build with TLSNoLoadPublicCAs(true) expecting empty RootCAs but got SystemCerts in RootCAs")
	}

	c, err = NewClientBuilder().
		EntryPoint("tcp://example.com:80").
		TLSCerts(
			key,
			cert,
			[]byte{},
		).
		TLSNoLoadPublicCAs(false).
		Build()
	if err != nil {
		t.Errorf("While build client and TLSNoLoadPublicCAs=false: %v", err)
		return
	}

	if c.tls.tlsConfig.RootCAs == nil {
		t.Errorf("ClientBuilder.Build with TLSNoLoadPublicCAs(false) expecting nil RootCAs: got = %v", c.tls.tlsConfig.RootCAs)
	}

	c, err = NewClientBuilder().
		EntryPoint("tcp://example.com:80").
		TLSCerts(
			key,
			cert,
			certCA,
		).
		TLSNoLoadPublicCAs(true).
		Build()
	if err != nil {
		t.Errorf("While build client and TLSNoLoadPublicCAs=false: %v", err)
		return
	}

	if c.tls.tlsConfig.RootCAs == nil || c.tls.tlsConfig.RootCAs.Equal(sysCerts) {
		t.Errorf(
			"ClientBuilder.Build with TLSNoLoadPublicCAs(true) and custom CAS expecting RootCAs "+
				"not nil and different from SystemCerts but got (RootCAS == nil) = %v, RootCAs.Equal(SystemCerts) = %v, RootCAS = %v",
			c.tls.tlsConfig.RootCAs == nil,
			c.tls.tlsConfig.RootCAs.Equal(sysCerts),
			c.tls.tlsConfig.RootCAs,
		)
	}
}

// createCerts return a key, cert, keyCA, certCA, and error of generated
// certs and CA used to sign the cert
func createCerts() ([]byte, []byte, []byte, []byte, error) {
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(2019),
		Subject: pkix.Name{
			Organization:  []string{"Company, INC."},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"San Francisco"},
			StreetAddress: []string{"Golden Gate Bridge"},
			PostalCode:    []string{"94016"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	// create our private and public key
	caPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// create the CA
	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// pem encode
	caPEM := new(bytes.Buffer)
	pem.Encode(caPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	})

	caPrivKeyPEM := new(bytes.Buffer)
	pem.Encode(caPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(caPrivKey),
	})

	// set up our server certificate
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(2019),
		Subject: pkix.Name{
			Organization:  []string{"Company, INC."},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"San Francisco"},
			StreetAddress: []string{"Golden Gate Bridge"},
			PostalCode:    []string{"94016"},
		},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(10, 0, 0),
		SubjectKeyId: []byte{1, 2, 3, 4, 6},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	certPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, cert, ca, &certPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	certPEM := new(bytes.Buffer)
	pem.Encode(certPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	})

	certPrivKeyPEM := new(bytes.Buffer)
	pem.Encode(certPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(certPrivKey),
	})

	return certPrivKeyPEM.Bytes(), certPEM.Bytes(), caPrivKeyPEM.Bytes(), caPEM.Bytes(), nil
}

func TestClient_AsyncIds_nil(t *testing.T) {
	var dsc *Client
	var want []string
	if got := dsc.AsyncIds(); !reflect.DeepEqual(got, want) {
		t.Errorf("Client.GetEntryPoint() with nil pointer: got = %v, want %v", got, want)
	}
}

func TestClient_AsyncIds(t *testing.T) {
	type fields struct {
		entryPoint              string
		syslogHostname          string
		defaultTag              string
		conn                    net.Conn
		ReplaceSequences        map[string]string
		tls                     *tlsSetup
		waitGroup               sync.WaitGroup
		asyncErrors             map[string]error
		asyncErrorsMutext       sync.Mutex
		tcp                     tcpConfig
		connectionUsedTimestamp time.Time
		connectionUsedTSMutext  sync.Mutex
		maxTimeConnActive       time.Duration
		asyncItems              map[string]interface{}
		asyncItemsMutext        sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{
			"Empty input",
			fields{},
			[]string{},
		},
		{
			"Empty ids",
			fields{
				asyncItems: make(map[string]interface{}),
			},
			[]string{},
		},
		{
			"With ids",
			fields{
				asyncItems: map[string]interface{}{
					"one":  nil,
					"two":  "three",
					"four": 5,
				},
			},
			[]string{"one", "two", "four"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := &Client{
				entryPoint:              tt.fields.entryPoint,
				syslogHostname:          tt.fields.syslogHostname,
				defaultTag:              tt.fields.defaultTag,
				conn:                    tt.fields.conn,
				ReplaceSequences:        tt.fields.ReplaceSequences,
				tls:                     tt.fields.tls,
				waitGroup:               tt.fields.waitGroup,
				asyncErrors:             tt.fields.asyncErrors,
				asyncErrorsMutext:       tt.fields.asyncErrorsMutext,
				tcp:                     tt.fields.tcp,
				connectionUsedTimestamp: tt.fields.connectionUsedTimestamp,
				connectionUsedTSMutext:  tt.fields.connectionUsedTSMutext,
				maxTimeConnActive:       tt.fields.maxTimeConnActive,
				asyncItems:              tt.fields.asyncItems,
				asyncItemsMutext:        tt.fields.asyncItemsMutext,
			}
			got := dsc.AsyncIds()

			// sort to ignore order
			sort.Strings(got)
			sort.Strings(tt.want)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.AsyncIds() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_AreAsyncOps(t *testing.T) {
	type fields struct {
		entryPoint              string
		syslogHostname          string
		defaultTag              string
		conn                    net.Conn
		ReplaceSequences        map[string]string
		tls                     *tlsSetup
		waitGroup               sync.WaitGroup
		asyncErrors             map[string]error
		asyncErrorsMutext       sync.Mutex
		tcp                     tcpConfig
		connectionUsedTimestamp time.Time
		connectionUsedTSMutext  sync.Mutex
		maxTimeConnActive       time.Duration
		asyncItems              map[string]interface{}
		asyncItemsMutext        sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			"Empty input",
			fields{},
			false,
		},
		{
			"Empty ids",
			fields{
				asyncItems: make(map[string]interface{}),
			},
			false,
		},
		{
			"With ids",
			fields{
				asyncItems: map[string]interface{}{
					"one":  nil,
					"two":  "three",
					"four": 5,
				},
			},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := &Client{
				entryPoint:              tt.fields.entryPoint,
				syslogHostname:          tt.fields.syslogHostname,
				defaultTag:              tt.fields.defaultTag,
				conn:                    tt.fields.conn,
				ReplaceSequences:        tt.fields.ReplaceSequences,
				tls:                     tt.fields.tls,
				waitGroup:               tt.fields.waitGroup,
				asyncErrors:             tt.fields.asyncErrors,
				asyncErrorsMutext:       tt.fields.asyncErrorsMutext,
				tcp:                     tt.fields.tcp,
				connectionUsedTimestamp: tt.fields.connectionUsedTimestamp,
				connectionUsedTSMutext:  tt.fields.connectionUsedTSMutext,
				maxTimeConnActive:       tt.fields.maxTimeConnActive,
				asyncItems:              tt.fields.asyncItems,
				asyncItemsMutext:        tt.fields.asyncItemsMutext,
			}
			if got := dsc.AreAsyncOps(); got != tt.want {
				t.Errorf("Client.AreAsyncOps() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_IsAsyncActive_nil(t *testing.T) {
	var dsc *Client
	want := false
	if got := dsc.IsAsyncActive(""); got != want {
		t.Errorf("Client.IsAsyncActive() with nil pointer: got = %v, want %v", got, want)
	}
}

func TestClient_IsAsyncActive(t *testing.T) {
	type fields struct {
		asyncItems       map[string]interface{}
		asyncItemsMutext sync.Mutex
	}
	type args struct {
		id string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			"Empty input and param",
			fields{},
			args{},
			false,
		},
		{
			"Empty input and param",
			fields{},
			args{"value"},
			false,
		},
		{
			"Empty input b",
			fields{
				asyncItems: make(map[string]interface{}),
			},
			args{"value"},
			false,
		},
		{
			"With ids but not found",
			fields{
				asyncItems: map[string]interface{}{
					"one":  nil,
					"two":  "three",
					"four": 5,
				},
			},
			args{"not found"},
			false,
		},
		{
			"With ids found",
			fields{
				asyncItems: map[string]interface{}{
					"one":  nil,
					"two":  "three",
					"four": 5,
				},
			},
			args{"two"},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := &Client{
				asyncItems:       tt.fields.asyncItems,
				asyncItemsMutext: tt.fields.asyncItemsMutext,
			}
			if got := dsc.IsAsyncActive(tt.args.id); got != tt.want {
				t.Errorf("Client.IsAsyncActive() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_AsyncsNumber_nil(t *testing.T) {
	var dsc *Client
	want := 0
	if got := dsc.AsyncsNumber(); got != want {
		t.Errorf("Client.AsyncsNumber() with nil pointer: got = %v, want %v", got, want)
	}
}

func TestClient_AsyncsNumber(t *testing.T) {
	type fields struct {
		asyncItems       map[string]interface{}
		asyncItemsMutext sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{
			"Empty",
			fields{},
			0,
		},
		{
			"With async ops",
			fields{
				asyncItems: map[string]interface{}{
					"12345":  nil,
					"55432":  nil,
					"765454": nil,
					"aba":    nil,
				},
			},
			4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := &Client{
				asyncItems:       tt.fields.asyncItems,
				asyncItemsMutext: tt.fields.asyncItemsMutext,
			}
			if got := dsc.AsyncsNumber(); got != tt.want {
				t.Errorf("Client.AsyncsNumber() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_IsConnWorking(t *testing.T) {
	tests := []struct {
		name    string
		dsc     *Client
		want    bool
		wantErr bool
	}{
		{
			"Nil client",
			nil,
			false,
			false,
		},
		{
			"Nil connection",
			&Client{},
			false,
			false,
		},
		{
			"Error missing payload",
			&Client{
				conn: func() net.Conn {
					c, err := NewClientBuilder().
						EntryPoint("udp://localhost:13000").
						Build()
					if err != nil {
						panic(err)
					}

					return c.conn
				}(),
			},
			false,
			true,
		},
		{
			"No working",
			&Client{
				isConnWorkingPayload: []byte("\n"),
				conn: func() net.Conn {
					c, err := NewClientBuilder().
						EntryPoint("udp://localhost:13000").
						Build()
					if err != nil {
						panic(err)
					}

					// Close connection to force error if connection was stablished
					err = c.conn.Close()
					if err != nil {
						panic(err)
					}

					return c.conn
				}(),
			},
			false,
			false,
		},
		{
			"Working",
			&Client{
				isConnWorkingPayload: []byte("\n"),
				conn: func() net.Conn {
					c, err := NewClientBuilder().
						EntryPoint("udp://example.com:80"). // Live server on internet
						Build()
					if err != nil {
						panic(err)
					}

					return c.conn
				}(),
			},
			true,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.dsc.IsConnWorking()
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.IsConnWorking() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got != tt.want {
				t.Errorf("Client.IsConnWorking() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_String_nil(t *testing.T) {
	var dsc *Client
	want := "<nil>"
	if got := dsc.String(); got != want {
		t.Errorf("Client.String() with nil pointer: got = %s, want %s", got, want)
	}
}

func TestClient_String(t *testing.T) {
	type fields struct {
		entryPoint              string
		syslogHostname          string
		defaultTag              string
		conn                    net.Conn
		ReplaceSequences        map[string]string
		tls                     *tlsSetup
		waitGroup               sync.WaitGroup
		asyncErrors             map[string]error
		asyncErrorsMutext       sync.Mutex
		tcp                     tcpConfig
		connectionUsedTimestamp time.Time
		connectionUsedTSMutext  sync.Mutex
		maxTimeConnActive       time.Duration
		asyncItems              map[string]interface{}
		asyncItemsMutext        sync.Mutex
		lastSendCallTimestamp   time.Time
	}

	var testConn net.Conn
	testTlsSetup := &tlsSetup{
		tlsConfig: &tls.Config{},
	}

	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			"Empty",
			fields{},
			`entryPoint: '', syslogHostname: '', defaultTag: '', connAddr: '<nil>', ` +
				`ReplaceSequences: map[], tls: <nil>, #asyncErrors: 0, tcp: {<nil>} -> <nil>, ` +
				`connectionUsedTimestamp: '0001-01-01 00:00:00 +0000 UTC', maxTimeConnActive: '0s', ` +
				`#asyncItems: 0, lastSendCallTimestamp: '0001-01-01 00:00:00 +0000 UTC'`,
		},
		{
			"With values",
			fields{
				entryPoint:     "The entryPoint",
				syslogHostname: "The syslogHostname",
				defaultTag:     "The defaultTag",
				conn: func() net.Conn {
					tcpConn, err := net.DialTimeout("udp", "example.com:80", time.Second*2)
					if err != nil {
						log.Fatalf("Error when create test connection: %v", err)
					}
					testConn = tcpConn
					return tcpConn
				}(),
				ReplaceSequences: map[string]string{
					"a": "b",
				},
				tls: testTlsSetup,
				asyncErrors: map[string]error{
					"error-1": nil,
				},
				tcp:                     tcpConfig{},
				connectionUsedTimestamp: time.Unix(1978, 0),
				maxTimeConnActive:       time.Second,
				asyncItems: map[string]interface{}{
					"async-1": nil,
				},
				lastSendCallTimestamp: time.Unix(1978, 1),
			},
			`entryPoint: 'The entryPoint', syslogHostname: 'The syslogHostname', defaultTag: ` +
				`'The defaultTag', connAddr: '` + testConn.LocalAddr().String() + ` -> ` +
				testConn.RemoteAddr().String() + `', ReplaceSequences: map[a:b], tls: ` +
				fmt.Sprintf("%v", testTlsSetup) + `, #asyncErrors: 1, tcp: {<nil>} -> <nil>, ` +
				`connectionUsedTimestamp: '` + fmt.Sprintf("%v", time.Unix(1978, 0)) +
				`', maxTimeConnActive: '1s', #asyncItems: 1, lastSendCallTimestamp: '` +
				fmt.Sprintf("%v", time.Unix(1978, 1)) + `'`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := &Client{
				entryPoint:              tt.fields.entryPoint,
				syslogHostname:          tt.fields.syslogHostname,
				defaultTag:              tt.fields.defaultTag,
				conn:                    tt.fields.conn,
				ReplaceSequences:        tt.fields.ReplaceSequences,
				tls:                     tt.fields.tls,
				waitGroup:               tt.fields.waitGroup,
				asyncErrors:             tt.fields.asyncErrors,
				asyncErrorsMutext:       tt.fields.asyncErrorsMutext,
				tcp:                     tt.fields.tcp,
				connectionUsedTimestamp: tt.fields.connectionUsedTimestamp,
				connectionUsedTSMutext:  tt.fields.connectionUsedTSMutext,
				maxTimeConnActive:       tt.fields.maxTimeConnActive,
				asyncItems:              tt.fields.asyncItems,
				asyncItemsMutext:        tt.fields.asyncItemsMutext,
				lastSendCallTimestamp:   tt.fields.lastSendCallTimestamp,
			}
			if got := dsc.String(); got != tt.want {
				t.Errorf("Client.String() = \"%v\", want \"%v\"", got, tt.want)
			}
		})
	}
}

func TestClient_LastSendCallTimestamp_nil(t *testing.T) {
	var dsc *Client
	want := time.Time{}
	if got := dsc.LastSendCallTimestamp(); got != want {
		t.Errorf("Client.LastSendCallTimestamp() with nil pointer: got = %v, want %v", got, want)
	}
}

func TestClient_LastSendCallTimestamp(t *testing.T) {
	type fields struct {
		entryPoint              string
		syslogHostname          string
		defaultTag              string
		conn                    net.Conn
		ReplaceSequences        map[string]string
		tls                     *tlsSetup
		waitGroup               sync.WaitGroup
		asyncErrors             map[string]error
		asyncErrorsMutext       sync.Mutex
		tcp                     tcpConfig
		connectionUsedTimestamp time.Time
		connectionUsedTSMutext  sync.Mutex
		maxTimeConnActive       time.Duration
		asyncItems              map[string]interface{}
		asyncItemsMutext        sync.Mutex
		lastSendCallTimestamp   time.Time
		statsMutex              sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
		want   time.Time
	}{
		{
			"Empty",
			fields{},
			time.Time{},
		},
		{
			"With value",
			fields{
				lastSendCallTimestamp: time.Unix(1978, 2),
			},
			time.Unix(1978, 2),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := &Client{
				entryPoint:              tt.fields.entryPoint,
				syslogHostname:          tt.fields.syslogHostname,
				defaultTag:              tt.fields.defaultTag,
				conn:                    tt.fields.conn,
				ReplaceSequences:        tt.fields.ReplaceSequences,
				tls:                     tt.fields.tls,
				waitGroup:               tt.fields.waitGroup,
				asyncErrors:             tt.fields.asyncErrors,
				asyncErrorsMutext:       tt.fields.asyncErrorsMutext,
				tcp:                     tt.fields.tcp,
				connectionUsedTimestamp: tt.fields.connectionUsedTimestamp,
				connectionUsedTSMutext:  tt.fields.connectionUsedTSMutext,
				maxTimeConnActive:       tt.fields.maxTimeConnActive,
				asyncItems:              tt.fields.asyncItems,
				asyncItemsMutext:        tt.fields.asyncItemsMutext,
				lastSendCallTimestamp:   tt.fields.lastSendCallTimestamp,
				statsMutex:              tt.fields.statsMutex,
			}
			if got := dsc.LastSendCallTimestamp(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Client.LastSendCallTimestamp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_sendCalled(t *testing.T) {
	type fields struct {
		lastSendCallTimestamp time.Time
	}
	tests := []struct {
		name      string
		fields    fields
		origValue time.Time
		offset    time.Duration
	}{
		{
			"Timestamp is updated",
			fields{
				lastSendCallTimestamp: time.Unix(1978, 0),
			},
			time.Unix(1978, 0),
			time.Second * -2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsc := &Client{
				lastSendCallTimestamp: tt.fields.lastSendCallTimestamp,
			}
			dsc.sendCalled()
			if tt.origValue == dsc.lastSendCallTimestamp {
				t.Errorf("Client.sendCalled(), Want orig timestamp %v was updated but want the same", tt.origValue)
			}
			target := time.Now().Add(tt.offset)
			if !dsc.lastSendCallTimestamp.After(target) {
				t.Errorf(
					"Client.sendCalled(), Want last.SendCallTimestamp was before that now + offset(%s) = %s, got = %v",
					tt.offset, target, dsc.lastSendCallTimestamp)
			}
		})
	}
}

func Test_connectionError_Error(t *testing.T) {
	type fields struct {
		Mode string
		Err  error
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			"Error",
			fields{
				Mode: "test mode",
				Err:  errors.New("test error"),
			},
			"while create new DevoSender (test mode): test error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ce := &connectionError{
				Mode: tt.fields.Mode,
				Err:  tt.fields.Err,
			}
			if got := ce.Error(); got != tt.want {
				t.Errorf("connectionError.Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_isConnectionError(t *testing.T) {
	type args struct {
		e error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			"nil error",
			args{nil},
			false,
		},
		{
			"Other error",
			args{errors.New("test error")},
			false,
		},
		{
			"isConnection error",
			args{
				&connectionError{
					Mode: "test mode",
					Err:  errors.New("test error"),
				},
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isConnectionError(tt.args.e); got != tt.want {
				t.Errorf("isConnectionError() = %v, want %v", got, tt.want)
			}
		})
	}
}
