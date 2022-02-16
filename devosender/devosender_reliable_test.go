package devosender

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cyberluisda/devo-go/applogger"
	"github.com/cyberluisda/devo-go/devosender/compressor"
	"github.com/cyberluisda/devo-go/devosender/status"
)

func TestReliableClientBuilder_ClientReconnDaemonWaitBtwChecks(t *testing.T) {
	type fields struct {
		clientReconnOpts daemonOpts
	}
	type args struct {
		d time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ReliableClientBuilder
	}{
		{
			"Duration eq to 0",
			fields{
				daemonOpts{waitBtwChecks: time.Hour},
			},
			args{0 * time.Millisecond},
			&ReliableClientBuilder{
				clientReconnOpts: daemonOpts{
					waitBtwChecks: time.Hour,
				},
			},
		},
		{
			"Duration less to 0",
			fields{
				daemonOpts{},
			},
			args{-1 * time.Second},
			&ReliableClientBuilder{
				clientReconnOpts: daemonOpts{
					waitBtwChecks: 0,
				},
			},
		},
		{
			"Duration greater than 0",
			fields{
				daemonOpts{},
			},
			args{time.Minute},
			&ReliableClientBuilder{
				clientReconnOpts: daemonOpts{
					waitBtwChecks: time.Minute,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrcb := &ReliableClientBuilder{
				clientReconnOpts: tt.fields.clientReconnOpts,
			}
			if got := dsrcb.ClientReconnDaemonWaitBtwChecks(tt.args.d); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReliableClientBuilder.ClientReconnDaemonWaitBtwChecks() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestReliableClientBuilder_HouseKeepingDaemonWaitBtwChecks(t *testing.T) {
	type fields struct {
		houseKeepingDaemonOpts daemonOpts
	}
	type args struct {
		d time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ReliableClientBuilder
	}{
		{
			"Zero value",
			fields{daemonOpts{waitBtwChecks: time.Second}},
			args{},
			&ReliableClientBuilder{
				houseKeepingDaemonOpts: daemonOpts{waitBtwChecks: time.Second},
			},
		},
		{
			"Value greater than 0",
			fields{daemonOpts{waitBtwChecks: time.Second}},
			args{time.Minute},
			&ReliableClientBuilder{
				houseKeepingDaemonOpts: daemonOpts{waitBtwChecks: time.Minute},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrcb := &ReliableClientBuilder{
				houseKeepingDaemonOpts: tt.fields.houseKeepingDaemonOpts,
			}
			if got := dsrcb.HouseKeepingDaemonWaitBtwChecks(tt.args.d); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReliableClientBuilder.HouseKeepingDaemonWaitBtwChecks() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestReliableClientBuilder_ClientReconnDaemonInitDelay(t *testing.T) {
	type fields struct {
		clientReconnOpts daemonOpts
	}
	type args struct {
		d time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ReliableClientBuilder
	}{
		{
			"Duration eq to 0",
			fields{
				daemonOpts{initDelay: time.Hour},
			},
			args{0 * time.Millisecond},
			&ReliableClientBuilder{
				clientReconnOpts: daemonOpts{
					initDelay: time.Hour,
				},
			},
		},
		{
			"Duration less to 0",
			fields{
				daemonOpts{},
			},
			args{-1 * time.Second},
			&ReliableClientBuilder{
				clientReconnOpts: daemonOpts{
					initDelay: 0,
				},
			},
		},
		{
			"Duration greater than 0",
			fields{
				daemonOpts{},
			},
			args{time.Minute},
			&ReliableClientBuilder{
				clientReconnOpts: daemonOpts{
					initDelay: time.Minute,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrcb := &ReliableClientBuilder{
				clientReconnOpts: tt.fields.clientReconnOpts,
			}
			if got := dsrcb.ClientReconnDaemonInitDelay(tt.args.d); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReliableClientBuilder.ClientReconnDaemonInitDelay() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestReliableClientBuilder_HouseKeepingDaemonInitDelay(t *testing.T) {
	type fields struct {
		houseKeepingDaemonOpts daemonOpts
	}
	type args struct {
		d time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ReliableClientBuilder
	}{
		{
			"Zero value",
			fields{daemonOpts{initDelay: time.Second}},
			args{},
			&ReliableClientBuilder{
				houseKeepingDaemonOpts: daemonOpts{initDelay: time.Second},
			},
		},
		{
			"Value greater than 0",
			fields{daemonOpts{initDelay: time.Second}},
			args{time.Minute},
			&ReliableClientBuilder{
				houseKeepingDaemonOpts: daemonOpts{initDelay: time.Minute},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrcb := &ReliableClientBuilder{
				houseKeepingDaemonOpts: tt.fields.houseKeepingDaemonOpts,
			}
			if got := dsrcb.HouseKeepingDaemonInitDelay(tt.args.d); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReliableClientBuilder.HouseKeepingDaemonInitDelay() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestReliableClientBuilder_DaemonStopTimeout(t *testing.T) {
	type fields struct {
		daemonStopTimeout time.Duration
	}
	type args struct {
		d time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ReliableClientBuilder
	}{
		{
			"Duration eq to 0",
			fields{
				time.Hour,
			},
			args{0 * time.Millisecond},
			&ReliableClientBuilder{
				daemonStopTimeout: time.Hour,
			},
		},
		{
			"Duration less to 0",
			fields{
				time.Hour,
			},
			args{-1 * time.Second},
			&ReliableClientBuilder{
				daemonStopTimeout: time.Hour,
			},
		},
		{
			"Duration greater than 0",
			fields{
				0,
			},
			args{time.Minute},
			&ReliableClientBuilder{
				daemonStopTimeout: time.Minute,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrcb := &ReliableClientBuilder{
				daemonStopTimeout: tt.fields.daemonStopTimeout,
			}
			if got := dsrcb.DaemonStopTimeout(tt.args.d); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReliableClientBuilder.DaemonStopTimeout() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReliableClientBuilder_EnableStandByModeTimeout(t *testing.T) {
	type fields struct {
		enableStandByModeTimeout time.Duration
	}
	type args struct {
		d time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ReliableClientBuilder
	}{
		{
			"Duration eq to 0",
			fields{
				time.Hour,
			},
			args{0 * time.Millisecond},
			&ReliableClientBuilder{
				enableStandByModeTimeout: 0,
			},
		},
		{
			"Duration less to 0",
			fields{
				time.Hour,
			},
			args{-1 * time.Second},
			&ReliableClientBuilder{
				enableStandByModeTimeout: time.Second * -1,
			},
		},
		{
			"Duration greater than 0",
			fields{
				0,
			},
			args{time.Minute},
			&ReliableClientBuilder{
				enableStandByModeTimeout: time.Minute,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrcb := &ReliableClientBuilder{
				enableStandByModeTimeout: tt.fields.enableStandByModeTimeout,
			}
			if got := dsrcb.EnableStandByModeTimeout(tt.args.d); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReliableClientBuilder.EnableStandByModeTimeout() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestReliableClientBuilder_FlushTimeout(t *testing.T) {
	type fields struct {
		flushTimeout time.Duration
	}
	type args struct {
		d time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ReliableClientBuilder
	}{
		{
			"Duration eq to 0",
			fields{
				time.Hour,
			},
			args{0 * time.Millisecond},
			&ReliableClientBuilder{
				flushTimeout: time.Hour,
			},
		},
		{
			"Duration less to 0",
			fields{
				time.Hour,
			},
			args{-1 * time.Second},
			&ReliableClientBuilder{
				flushTimeout: time.Hour,
			},
		},
		{
			"Duration greater than 0",
			fields{
				0,
			},
			args{time.Minute},
			&ReliableClientBuilder{
				flushTimeout: time.Minute,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrcb := &ReliableClientBuilder{
				flushTimeout: tt.fields.flushTimeout,
			}
			if got := dsrcb.FlushTimeout(tt.args.d); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReliableClientBuilder.FlushTimeout() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestReliableClientBuilder_MaxRecordsResendByFlush(t *testing.T) {
	type fields struct {
		maxRecordsResendByFlush int
	}
	type args struct {
		max int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ReliableClientBuilder
	}{
		{
			"Param eq to 0",
			fields{
				maxRecordsResendByFlush: 20,
			},
			args{0},
			&ReliableClientBuilder{
				maxRecordsResendByFlush: 0,
			},
		},
		{
			"Param less to 0",
			fields{
				maxRecordsResendByFlush: 20,
			},
			args{-1},
			&ReliableClientBuilder{
				maxRecordsResendByFlush: -1,
			},
		},
		{
			"Param greater than 0",
			fields{
				0,
			},
			args{500},
			&ReliableClientBuilder{
				maxRecordsResendByFlush: 500,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrcb := &ReliableClientBuilder{
				maxRecordsResendByFlush: tt.fields.maxRecordsResendByFlush,
			}
			if got := dsrcb.MaxRecordsResendByFlush(tt.args.max); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReliableClientBuilder.MaxRecordsResendByFlush() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestReliableClientBuilder_Build(t *testing.T) {
	type fields struct {
		clientBuilder            *ClientBuilder
		statusBuilder            status.Builder
		retryDaemonOpts          daemonOpts
		clientReconnOpts         daemonOpts
		houseKeepingDaemonOpts   daemonOpts
		daemonStopTimeout        time.Duration
		enableStandByModeTimeout time.Duration
		flushTimeout             time.Duration
		appLogger                applogger.SimpleAppLogger
	}
	tests := []struct {
		name    string
		fields  fields
		want    *ReliableClient
		wantErr bool
	}{
		{
			"Empty builder error: empty status path",
			fields{},
			nil,
			true,
		},
		{
			"Inner client builder nil",
			fields{
				statusBuilder: status.NewNutsDBStatusBuilder().DbPath("/tmp/test-builder-build"),
			},
			nil,
			true,
		},
		{
			"Client build error",
			fields{
				statusBuilder: status.NewNutsDBStatusBuilder().DbPath("/tmp/test-builder-build"),
				clientBuilder: &ClientBuilder{
					keyFileName:  "/tmp/doesnotexists",
					certFileName: "/tmp/doesnotexists",
				},
			},
			nil,
			true,
		},
		{
			"Get client without conn",
			fields{
				// Copied directly from NewReliableClientBuilder
				statusBuilder: status.NewNutsDBStatusBuilder().DbPath("/tmp/test-builder-build").FilesToConsolidateDb(2),
				appLogger:     &applogger.NoLogAppLogger{},

				retryDaemonOpts: daemonOpts{
					initDelay:     time.Minute,
					waitBtwChecks: time.Millisecond * 100,
				},
				clientReconnOpts: daemonOpts{
					initDelay:     time.Minute,
					waitBtwChecks: time.Millisecond * 100,
				},
				houseKeepingDaemonOpts: daemonOpts{
					initDelay:     time.Minute,
					waitBtwChecks: time.Millisecond * 100,
				},

				clientBuilder: &ClientBuilder{
					entrypoint: "udp://localhost:1234",
				},
			},
			&ReliableClient{
				Client: &Client{
					entryPoint: "udp://localhost:1234",
				},
				retryDaemon: reliableClientDaemon{
					daemonOpts: daemonOpts{
						initDelay:     time.Minute,
						waitBtwChecks: time.Millisecond * 100,
					},
				},
				reconnDaemon: reliableClientDaemon{
					daemonOpts: daemonOpts{
						initDelay:     time.Minute,
						waitBtwChecks: time.Millisecond * 100,
					},
				},
				houseKeepingDaemon: reliableClientDaemon{
					daemonOpts: daemonOpts{
						initDelay:     time.Minute,
						waitBtwChecks: time.Millisecond * 100,
					},
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrcb := &ReliableClientBuilder{
				clientBuilder:            tt.fields.clientBuilder,
				statusBuilder:            tt.fields.statusBuilder,
				retryDaemonOpts:          tt.fields.retryDaemonOpts,
				clientReconnOpts:         tt.fields.clientReconnOpts,
				houseKeepingDaemonOpts:   tt.fields.houseKeepingDaemonOpts,
				daemonStopTimeout:        tt.fields.daemonStopTimeout,
				enableStandByModeTimeout: tt.fields.enableStandByModeTimeout,
				flushTimeout:             tt.fields.flushTimeout,
				appLogger:                tt.fields.appLogger,
			}
			got, err := dsrcb.Build()
			if (err != nil) != tt.wantErr {
				t.Errorf("ReliableClientBuilder.Build() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Copy dinamic fields that has no sense to be compared
			if tt.want != nil && got != nil {
				if got.Client != nil && tt.want.Client != nil {
					tt.want.Client.syslogHostname = got.Client.syslogHostname
					tt.want.Client.conn = got.Client.conn
					tt.want.Client.connectionUsedTimestamp = got.Client.connectionUsedTimestamp
					tt.want.tcp = got.Client.tcp
				}
				tt.want.status = got.status
				tt.want.daemonStopped = got.daemonStopped

				// Comparint string value, It abstracts it from test inner structure like mutex
				gotStr := got.String()
				wantStr := tt.want.String()
				if gotStr != wantStr {
					t.Errorf("ReliableClientBuilder.Build() = %s, want %s", gotStr, wantStr)
				}
			}
		})
	}

	// Remove temporal status path
	os.RemoveAll("/tmp/test-builder-build")
}

func TestReliableClient_SendAsync(t *testing.T) {
	type args struct {
		m string
	}
	tests := []struct {
		name           string
		reliableClient *ReliableClient
		args           args
		wantPattern    string
	}{
		{
			"Send async without connection",
			func() *ReliableClient {

				// Remove status path if exists
				os.RemoveAll("/tmp/tests-reliable-sends-async-no-conn")

				r, err := NewReliableClientBuilder().
					ClientBuilder(
						NewClientBuilder().EntryPoint("tcp://this-is-not-exists:1234"),
					).
					StatusBuilder(
						status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-sends-async-no-conn")).
					Build()
				if err != nil {
					panic(err)
				}
				return r
			}(),
			args{"This is the message"},
			nonConnIDPrefix + `\w{8}-\w{4}-\w{4}-\w{4}-\w{12}`,
			// non-conn-483b88ce-88b0-4f66-a78d-e8dfbddf70b0
		},
		{
			"Send async wit connection",
			func() *ReliableClient {

				// Remove status path if exists
				os.RemoveAll("/tmp/tests-reliable-sends-async-no-conn")

				r, err := NewReliableClientBuilder().
					ClientBuilder(
						NewClientBuilder().EntryPoint("udp://localhost:13000"),
					).
					StatusBuilder(
						status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-sends-async-with-conn")).
					Build()
				if err != nil {
					panic(err)
				}
				r.SetDefaultTag("my.app.tests.reliable")
				return r
			}(),
			args{"This is the message"},
			`\w{8}-\w{4}-\w{4}-\w{4}-\w{12}`,
			// 483b88ce-88b0-4f66-a78d-e8dfbddf70b0
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ptrn := regexp.MustCompile(tt.wantPattern)
			if got := tt.reliableClient.SendAsync(tt.args.m); !ptrn.Match([]byte(got)) {
				t.Errorf("ReliableClient.SendAsync() = %v, wantPattern %v", got, tt.wantPattern)
			}
		})
	}

	// Remove temporal paths
	os.RemoveAll("/tmp/tests-reliable-sends-async-no-conn")
	os.RemoveAll("/tmp/tests-reliable-sends-async-with-conn")
}

func TestReliableClient_SendWTagAndCompressorAsync(t *testing.T) {
	type args struct {
		t string
		m string
		c *compressor.Compressor
	}
	tests := []struct {
		name           string
		reliableClient *ReliableClient
		args           args
		wantPattern    string
	}{
		{
			"Send async without connection",
			func() *ReliableClient {

				// Remove status path if exists
				os.RemoveAll("/tmp/tests-reliable-SendWTagAndCompressorAsync-no-conn")

				r, err := NewReliableClientBuilder().
					ClientBuilder(
						NewClientBuilder().EntryPoint("tcp://this-is-not-exists:1234"),
					).
					StatusBuilder(
						status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-SendWTagAndCompressorAsync-no-conn")).
					Build()
				if err != nil {
					panic(err)
				}
				return r
			}(),
			args{
				"my.app.tests.reliable",
				"This is the message",
				nil,
			},
			nonConnIDPrefix + `\w{8}-\w{4}-\w{4}-\w{4}-\w{12}`,
			// non-conn-483b88ce-88b0-4f66-a78d-e8dfbddf70b0
		},
		{
			"Send async wit connection",
			func() *ReliableClient {

				// Remove status path if exists
				os.RemoveAll("/tmp/tests-reliable-SendWTagAndCompressorAsync-no-conn")

				r, err := NewReliableClientBuilder().
					ClientBuilder(
						NewClientBuilder().EntryPoint("udp://localhost:13000"),
					).
					StatusBuilder(
						status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-SendWTagAndCompressorAsync-with-conn")).
					Build()
				if err != nil {
					panic(err)
				}
				return r
			}(),
			args{
				"my.app.tests.reliable",
				"This is the message",
				&compressor.Compressor{Algorithm: compressor.CompressorZlib},
			},
			`\w{8}-\w{4}-\w{4}-\w{4}-\w{12}`,
			// 483b88ce-88b0-4f66-a78d-e8dfbddf70b0
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ptrn := regexp.MustCompile(tt.wantPattern)
			if got := tt.reliableClient.SendWTagAndCompressorAsync(tt.args.t, tt.args.m, tt.args.c); !ptrn.Match([]byte(got)) {
				t.Errorf("ReliableClient.my.app.tests.reliable() = %v, wantPattern %v", got, tt.wantPattern)
			}
		})
	}

	// Remove temporal paths
	os.RemoveAll("/tmp/tests-reliable-SendWTagAndCompressorAsync-no-conn")
	os.RemoveAll("/tmp/tests-reliable-SendWTagAndCompressorAsync-with-conn")
}

func TestReliableClient_Flush(t *testing.T) {
	type fields struct {
		Client                      *Client
		clientBuilder               *ClientBuilder
		status                      status.Status
		retryDaemon                 reliableClientDaemon
		reconnDaemon                reliableClientDaemon
		daemonStopTimeout           time.Duration
		standByMode                 bool
		enableStandByModeTimeout    time.Duration
		daemonStopped               chan bool
		flushTimeout                time.Duration
		maxRecordsResendInFlushCall int
		appLogger                   applogger.SimpleAppLogger
	}
	type asyncMsgs struct {
		t string
		m string
		c *compressor.Compressor
	}
	tests := []struct {
		name                           string
		fields                         fields
		preAsyncMessages               []asyncMsgs
		clientNillAFterPreAsyncMessags bool
		wantErr                        bool
	}{
		{
			"Error collecting pending events",
			fields{
				standByMode: true, // prevent sync
				status: func() status.Status {
					os.RemoveAll("/tmp/tests-reliable-Flush")
					sb := status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-Flush")
					r, err := sb.Build()
					if err != nil {
						panic(err)
					}

					// Force error closing status.db
					err = r.Close()
					if err != nil {
						panic(err)
					}

					return r
				}(),
			},
			nil,
			false,
			true,
		},
		{
			"Timeout reached when waiting for pending messages",
			fields{
				standByMode: false,
				Client: func() *Client {
					r, err := NewClientBuilder().EntryPoint("udp://example.com:80").Build()
					if err != nil {
						panic(err)
					}

					return r
				}(),
				status: func() status.Status {
					os.RemoveAll("/tmp/tests-reliable-Flush-pending")
					sb := status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-Flush-pending")
					r, err := sb.Build()
					if err != nil {
						panic(err)
					}
					return r
				}(),
			},
			nil,
			false,
			true,
		},
		{
			"Flush mark as resend by error in pending messages",
			fields{
				standByMode: false,
				Client: func() *Client {
					r, err := NewClientBuilder().EntryPoint("udp://localhost:13000").Build()
					if err != nil {
						panic(err)
					}
					return r
				}(),
				flushTimeout: time.Second,
				status: func() status.Status {
					os.RemoveAll("/tmp/tests-reliable-Flush-pending-errors")
					sb := status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-Flush-pending-errors")
					r, err := sb.Build()
					if err != nil {
						panic(err)
					}
					return r
				}(),
				appLogger: &applogger.NoLogAppLogger{},
			},
			[]asyncMsgs{
				{m: "Error because tag is not defined"},
			},
			false,
			false,
		},
		{
			"Resend pending errored events with now conn",
			fields{
				standByMode: false,
				Client: func() *Client {
					r, err := NewClientBuilder().EntryPoint("udp://localhost:13000").Build()
					if err != nil {
						panic(err)
					}
					return r
				}(),
				flushTimeout: time.Second,
				status: func() status.Status {
					os.RemoveAll("/tmp/tests-reliable-Flush-pending-errors-no-conn")
					sb := status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-Flush-pending-errors-no-conn")
					r, err := sb.Build()
					if err != nil {
						panic(err)
					}
					return r
				}(),
			},
			[]asyncMsgs{
				{m: "Error because tag is not defined"},
			},
			true,
			false,
		},
		{
			"Max events limit reached",
			fields{
				standByMode: false,
				Client: func() *Client {
					r, err := NewClientBuilder().EntryPoint("udp://localhost:13000").Build()
					if err != nil {
						panic(err)
					}
					return r
				}(),
				flushTimeout: time.Second,
				status: func() status.Status {
					os.RemoveAll("/tmp/tests-reliable-Flush-max-events-limit-reached")
					sb := status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-Flush-max-events-limit-reached")
					r, err := sb.Build()
					if err != nil {
						panic(err)
					}
					return r
				}(),
				appLogger: &applogger.WriterAppLogger{
					Writer: os.Stdout,
					Level:  applogger.INFO,
				},
				maxRecordsResendInFlushCall: 1,
			},
			[]asyncMsgs{
				{t: "tag1", m: "Mesg 1"},
				{t: "tag2", m: "Mesg 2"},
			},
			false,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrc := &ReliableClient{
				Client:                      tt.fields.Client,
				clientBuilder:               tt.fields.clientBuilder,
				status:                      tt.fields.status,
				retryDaemon:                 tt.fields.retryDaemon,
				reconnDaemon:                tt.fields.reconnDaemon,
				daemonStopTimeout:           tt.fields.daemonStopTimeout,
				standByMode:                 tt.fields.standByMode,
				enableStandByModeTimeout:    tt.fields.enableStandByModeTimeout,
				daemonStopped:               tt.fields.daemonStopped,
				flushTimeout:                tt.fields.flushTimeout,
				maxRecordsResendInFlushCall: tt.fields.maxRecordsResendInFlushCall,
				appLogger:                   tt.fields.appLogger,
			}

			for _, am := range tt.preAsyncMessages {
				dsrc.SendWTagAndCompressorAsync(am.t, am.m, am.c)
			}

			if tt.clientNillAFterPreAsyncMessags {
				dsrc.Client.Close()
				dsrc.Client = nil
			}

			if err := dsrc.Flush(); (err != nil) != tt.wantErr {
				t.Errorf("ReliableClient.Flush() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	// Clean previous status if exists
	os.RemoveAll("/tmp/tests-reliable-Flush")
	os.RemoveAll("/tmp/tests-reliable-Flush-pending")
	os.RemoveAll("/tmp/tests-reliable-Flush-pending-errors")
	os.RemoveAll("/tmp/tests-reliable-Flush-pending-errors-no-conn")
	os.RemoveAll("/tmp/tests-reliable-Flush-max-events-limit-reached")
}

func TestReliableClient_Close(t *testing.T) {
	type fields struct {
		Client                   *Client
		clientBuilder            *ClientBuilder
		status                   status.Status
		retryDaemon              reliableClientDaemon
		reconnDaemon             reliableClientDaemon
		daemonStopTimeout        time.Duration
		standByMode              bool
		enableStandByModeTimeout time.Duration
		daemonStopped            chan bool
		flushTimeout             time.Duration
		appLogger                applogger.SimpleAppLogger
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			"All errors when close",
			fields{
				status: func() status.Status {
					os.RemoveAll("/tmp/tests-reliable-Close")
					sb := status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-Close")
					r, err := sb.Build()
					if err != nil {
						panic(err)
					}

					// Close, to force error
					err = r.Close()
					if err != nil {
						panic(err)
					}

					return r
				}(),
				Client: func() *Client {
					c, err := NewClientBuilder().EntryPoint("udp://localhost:13000").Build()
					if err != nil {
						panic(err)
					}
					err = c.conn.Close()
					if err != nil {
						panic(err)
					}

					// Set con to nil to force client close error
					c.conn = nil
					return c
				}(),
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrc := &ReliableClient{
				Client:                   tt.fields.Client,
				clientBuilder:            tt.fields.clientBuilder,
				status:                   tt.fields.status,
				retryDaemon:              tt.fields.retryDaemon,
				reconnDaemon:             tt.fields.reconnDaemon,
				daemonStopTimeout:        tt.fields.daemonStopTimeout,
				standByMode:              tt.fields.standByMode,
				enableStandByModeTimeout: tt.fields.enableStandByModeTimeout,
				daemonStopped:            tt.fields.daemonStopped,
				flushTimeout:             tt.fields.flushTimeout,
				appLogger:                tt.fields.appLogger,
			}
			if err := dsrc.Close(); (err != nil) != tt.wantErr {
				t.Errorf("ReliableClient.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	os.RemoveAll("/tmp/tests-reliable-Close")

}

func TestReliableClient_StandBy(t *testing.T) {
	tests := []struct {
		name           string
		reliableClient *ReliableClient
		wantErr        bool
	}{
		{
			"Timeout error while wait for pending",
			func() *ReliableClient {
				os.RemoveAll("/tmp/tests-reliable-StandBy")
				r, err := NewReliableClientBuilder().
					StatusBuilder(
						status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-StandBy")).
					ClientBuilder(
						NewClientBuilder().
							EntryPoint("udp://example.com:80"),
					).
					EnableStandByModeTimeout(time.Microsecond).
					Build()
				if err != nil {
					panic(err)
				}
				return r
			}(),
			true,
		},
		{
			"Error while close client",
			func() *ReliableClient {
				os.RemoveAll("/tmp/tests-reliable-StandBy-close-client")
				r, err := NewReliableClientBuilder().
					StatusBuilder(
						status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-StandBy-close-client")).
					ClientBuilder(
						NewClientBuilder().
							EntryPoint("udp://example.com:80"),
					).
					Build()
				if err != nil {
					panic(err)
				}

				// Client closed to force error
				r.Client.Close()
				return r
			}(),
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.reliableClient.StandBy(); (err != nil) != tt.wantErr {
				t.Errorf("ReliableClient.StandBy() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	os.RemoveAll("/tmp/tests-reliable-StandBy")
	os.RemoveAll("/tmp/tests-reliable-StandBy-close-client")
}

func TestReliableClient_WakeUp(t *testing.T) {
	tests := []struct {
		name           string
		reliableClient *ReliableClient
		wantErr        bool
	}{
		{
			"Error while rebuild client",
			func() *ReliableClient {
				os.RemoveAll("/tmp/tests-reliable-WakeUp")
				r, err := NewReliableClientBuilder().
					StatusBuilder(
						status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-WakeUp")).
					ClientBuilder(
						NewClientBuilder(), // Empty EntryPoint to force error
					).
					Build()
				if err != nil {
					panic(err)
				}

				return r
			}(),
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.reliableClient.WakeUp(); (err != nil) != tt.wantErr {
				t.Errorf("ReliableClient.WakeUp() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	os.RemoveAll("/tmp/tests-reliable-WakeUp")
}

func TestReliableClient__check_closed_conn(t *testing.T) {
	// Open new server
	tcm := &tcpMockRelay{}
	err := tcm.Start()
	if err != nil {
		t.Errorf("Error while start tcp mockrelay: %v", err)
		return
	}

	os.RemoveAll("/tmp/devosedner-tests-ReliableClient_IsStandBy_closedRelayConn")

	rc, err := NewReliableClientBuilder().
		StatusBuilder(
			status.NewNutsDBStatusBuilder().DbPath("/tmp/devosedner-tests-ReliableClient_IsStandBy_closedRelayConn")).
		ClientBuilder(
			NewClientBuilder().
				EntryPoint("tcp://localhost:" + fmt.Sprintf("%d", tcm.Port)).
				IsConnWorkingCheckPayload("\n")).
		EnableStandByModeTimeout(time.Millisecond * 100).
		RetryDaemonInitDelay(time.Second).
		ClientReconnDaemonInitDelay(time.Second).
		Build()
	if err != nil {
		t.Errorf("Error while create reliable client: %v", err)
		return
	}

	if rc.IsStandBy() {
		t.Error("ReliableClient.TestReliableClient_IsStandBy() with server started, want false, got true", rc.Client)
	}
	ok, _ := rc.IsConnWorking()
	if !ok {
		t.Error("ReliableClient.IsConnWorking() with server started, want true, got false", rc.Client)
	}

	// Close server
	err = tcm.Stop()
	if err != nil {
		t.Errorf("Error while create stop mock reliable client: %v", err)
	}
	// time.Sleep(time.Millisecond * 5000) // Wait for server close

	if rc.IsStandBy() {
		t.Error("ReliableClient.IsStandBy() with server stopped, want false, got true", rc.Client)
	}
	ok, _ = rc.IsConnWorking()
	if ok {
		t.Error("ReliableClient.IsConnWorking() with server stopped, want false, got true", rc.Client)
	}

	// Cleant tmp
	rc.Close()
	os.RemoveAll("/tmp/devosedner-tests-ReliableClient_IsStandBy_closedRelayConn")
}

func TestReliableClient_String(t *testing.T) {
	type fields struct {
		Client                   *Client
		clientBuilder            *ClientBuilder
		status                   status.Status
		retryDaemon              reliableClientDaemon
		reconnDaemon             reliableClientDaemon
		daemonStopTimeout        time.Duration
		standByMode              bool
		enableStandByModeTimeout time.Duration
		dbInitCleanedup          bool
		daemonStopped            chan bool
		flushTimeout             time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			"Empty",
			fields{},
			"Client: {<nil>}, status: {<nil>}, retryDaemon: { waitBtwChecks: 0s, " +
				"initDelay: 0s, stop: false}, reconnDaemon: { waitBtwChecks: 0s, initDelay: 0s, " +
				"stop: false}, houseKeepingDaemon: { waitBtwChecks: 0s, initDelay: 0s, " +
				"stop: false}, daemonStopTimeout: 0s, standByMode: false, enableStandByModeTimeout: 0s, " +
				"daemonStopped: <nil>, flushTimeout: 0s",
		},
		{
			"With some values",
			fields{
				Client: &Client{
					entryPoint: "udp://example.com:80",
				},
				status: func() status.Status {
					os.RemoveAll("/tmp/tests-reliable-String")
					sb := status.NewNutsDBStatusBuilder().
						DbPath("/tmp/tests-reliable-String").
						EventsTTLSeconds(20).
						BufferSize(123).
						FilesToConsolidateDb(4)
					r, err := sb.Build()
					if err != nil {
						panic(err)
					}
					return r
				}(),
				retryDaemon: reliableClientDaemon{
					daemonOpts: daemonOpts{
						waitBtwChecks: time.Minute,
						initDelay:     time.Second * 2,
					},
					stop: true,
				},
				reconnDaemon: reliableClientDaemon{
					daemonOpts: daemonOpts{
						waitBtwChecks: time.Second * 10,
						initDelay:     time.Second,
					},
					stop: true,
				},
				daemonStopTimeout:        time.Second * 5,
				standByMode:              true,
				enableStandByModeTimeout: time.Second * 3,
				dbInitCleanedup:          true,
				flushTimeout:             time.Minute * 2,
			},
			"Client: {entryPoint: 'udp://example.com:80', syslogHostname: '', defaultTag: '', " +
				"connAddr: '<nil>', ReplaceSequences: map[], tls: <nil>, #asyncErrors: 0, " +
				"tcp: {<nil>} -> <nil>, connectionUsedTimestamp: '0001-01-01 00:00:00 +0000 UTC', " +
				"maxTimeConnActive: '0s', #asyncItems: 0, lastSendCallTimestamp: " +
				"'0001-01-01 00:00:00 +0000 UTC'}, status: {KeyCount: 0, ListIdx: map[], " +
				"consolidationDbNumFilesThreshold: 4, dbFiles: 1, initialized: true, bufferSize: 123, " +
				"eventTTL: 20, recreateDbClientAfterConsolidation: true}, retryDaemon: " +
				"{ waitBtwChecks: 1m0s, initDelay: 2s, stop: true}, reconnDaemon: { " +
				"waitBtwChecks: 10s, initDelay: 1s, stop: true}, houseKeepingDaemon: " +
				"{ waitBtwChecks: 0s, initDelay: 0s, stop: false}, " +
				"daemonStopTimeout: 5s, standByMode: true, enableStandByModeTimeout: 3s, " +
				"daemonStopped: <nil>, flushTimeout: 2m0s",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrc := &ReliableClient{
				Client:                   tt.fields.Client,
				clientBuilder:            tt.fields.clientBuilder,
				status:                   tt.fields.status,
				retryDaemon:              tt.fields.retryDaemon,
				reconnDaemon:             tt.fields.reconnDaemon,
				daemonStopTimeout:        tt.fields.daemonStopTimeout,
				standByMode:              tt.fields.standByMode,
				enableStandByModeTimeout: tt.fields.enableStandByModeTimeout,
				daemonStopped:            tt.fields.daemonStopped,
				flushTimeout:             tt.fields.flushTimeout,
			}
			if got := dsrc.String(); got != tt.want {
				t.Errorf("ReliableClient.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReliableClient_daemonsSartup(t *testing.T) {
	type fields struct {
		status             status.Status
		retryDaemon        reliableClientDaemon
		reconnDaemon       reliableClientDaemon
		houseKeepingDaemon reliableClientDaemon
		appLogger          applogger.SimpleAppLogger
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			"Error: status db nil",
			fields{
				appLogger: &applogger.NoLogAppLogger{},
			},
			true,
		},
		{
			"Error: db intialization",
			fields{
				appLogger: &applogger.NoLogAppLogger{},
				status: func() status.Status {
					os.RemoveAll("/tmp/tests-reliable-daemonsSartup")
					sb := status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-daemonsSartup")
					r, err := sb.Build()
					if err != nil {
						panic(err)
					}
					// Close db to force error
					r.Close()
					return r
				}(),
			},
			true,
		},
		{
			"Error: Retry events daemon",
			fields{
				appLogger: &applogger.NoLogAppLogger{},
				status: func() status.Status {
					os.RemoveAll("/tmp/tests-reliable-daemonsSartup-retryEvents")
					sb := status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-daemonsSartup-retryEvents")
					r, err := sb.Build()
					if err != nil {
						panic(err)
					}
					return r
				}(),
				// Force error
				retryDaemon: reliableClientDaemon{
					daemonOpts: daemonOpts{
						waitBtwChecks: 0 * time.Second}},
			},
			true,
		},
		{
			"Error: Client reconnection daemon",
			fields{
				appLogger: &applogger.NoLogAppLogger{},
				status: func() status.Status {
					os.RemoveAll("/tmp/tests-reliable-daemonsSartup-clientReconn")
					sb := status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-daemonsSartup-clientReconn")
					r, err := sb.Build()
					if err != nil {
						panic(err)
					}
					return r
				}(),
				// retryEvents daemon should work
				retryDaemon: reliableClientDaemon{
					daemonOpts: daemonOpts{
						waitBtwChecks: 1 * time.Second,
						initDelay:     time.Minute}},
				// Force error
				reconnDaemon: reliableClientDaemon{
					daemonOpts: daemonOpts{
						waitBtwChecks: -1 * time.Second}},
			},
			true,
		},
		{
			"Error: housekeeping daemon",
			fields{
				appLogger: &applogger.NoLogAppLogger{},
				status: func() status.Status {
					os.RemoveAll("/tmp/tests-reliable-daemonsSartup-housekeeping")
					sb := status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-daemonsSartup-housekeeping")
					r, err := sb.Build()
					if err != nil {
						panic(err)
					}
					return r
				}(),
				// retryEvents daemon should work
				retryDaemon: reliableClientDaemon{
					daemonOpts: daemonOpts{
						waitBtwChecks: 1 * time.Second,
						initDelay:     time.Minute}},
				// reconn daemon should work
				reconnDaemon: reliableClientDaemon{
					daemonOpts: daemonOpts{
						waitBtwChecks: 1 * time.Second,
						initDelay:     time.Minute}},
				// Force error
				houseKeepingDaemon: reliableClientDaemon{
					daemonOpts: daemonOpts{
						waitBtwChecks: -2 * time.Second}},
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrc := &ReliableClient{
				status:             tt.fields.status,
				retryDaemon:        tt.fields.retryDaemon,
				reconnDaemon:       tt.fields.reconnDaemon,
				houseKeepingDaemon: tt.fields.houseKeepingDaemon,
				appLogger:          tt.fields.appLogger,
			}
			if err := dsrc.daemonsSartup(); (err != nil) != tt.wantErr {
				t.Errorf("ReliableClient.daemonsSartup() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	// Clean status db
	os.RemoveAll("/tmp/tests-reliable-daemonsSartup")
	os.RemoveAll("/tmp/tests-reliable-daemonsSartup-retryEvents")
	os.RemoveAll("/tmp/tests-reliable-daemonsSartup-clientReconn")
	os.RemoveAll("/tmp/tests-reliable-daemonsSartup-housekeeping")
}

func TestReliableClient_daemonsSartup_errorAsyncClosing(t *testing.T) {
	os.RemoveAll("/tmp/tests-reliable-daemonsSartup-errorAsyncClosing")
	dsrc, err := NewReliableClientBuilder().
		StatusBuilder(
			status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-daemonsSartup-errorAsyncClosing")).
		ClientBuilder(
			NewClientBuilder().EntryPoint("udp://example.com:80")).
		Build()
	if err != nil {
		panic(err)
	}

	time.Sleep(time.Millisecond * 100) // Enough time to dbInitCleanup

	dsrc.status.Close() // Force error when daemon close database

	t.Logf("Send term signal")
	syscall.Kill(syscall.Getpid(), syscall.SIGTERM)

	os.RemoveAll("/tmp/tests-reliable-daemonsSartup-errorAsyncClosing")
}

func TestReliableClient_startRetryEventsDaemon(t *testing.T) {
	tests := []struct {
		name    string
		dsrc    *ReliableClient
		wantErr bool
	}{
		{
			"waitBtwChecks error",
			&ReliableClient{},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.dsrc.startRetryEventsDaemon(); (err != nil) != tt.wantErr {
				t.Errorf("ReliableClient.startRetryEventsDaemon() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestReliableClient_clientReconnectionDaemon__server_restarted(t *testing.T) {
	// Open new server
	tcm := &tcpMockRelay{}
	err := tcm.Start()
	if err != nil {
		t.Errorf("Error while start tcp mockrelay: %v", err)
		return
	}

	os.RemoveAll("/tmp/devosedner-tests-ReliableClient_clientReconnectionDaemon")

	rc, err := NewReliableClientBuilder().
		StatusBuilder(
			status.NewNutsDBStatusBuilder().DbPath("/tmp/devosedner-tests-ReliableClient_clientReconnectionDaemon")).
		ClientBuilder(
			NewClientBuilder().
				EntryPoint("tcp://localhost:" + fmt.Sprintf("%d", tcm.Port)).
				IsConnWorkingCheckPayload("\n")).
		EnableStandByModeTimeout(time.Millisecond * 100).
		RetryDaemonInitDelay(time.Minute).                   // Prevent daemon runs during tests
		ClientReconnDaemonInitDelay(time.Millisecond * 300). // Give me time to stop server
		ClientReconnDaemonWaitBtwChecks(time.Millisecond * 300).
		Build()
	if err != nil {
		t.Errorf("Error while create reliable client: %v", err)
		return
	}

	// Close server
	err = tcm.Stop()
	if err != nil {
		t.Errorf("Error while create stop mock reliable client: %v", err)
		return
	}

	// Wait until isConnWorking will be failling or timeout
	tries := 3
	for ok, _ := rc.IsConnWorking(); ok; { // we are sure that IsConnWorking does not return error
		tries--
		if tries == 0 {
			t.Errorf("Timeout reached while wait for IsConnWorking return false")
			return
		}
		time.Sleep(time.Millisecond * 100)
	}

	// Restart the server
	err = tcm.Start()
	if err != nil {
		t.Errorf("Error while restart tcp mockrelay: %v", err)
		return
	}

	// Ensure we are waiting enouth time to reconn dameon does its job
	time.Sleep(time.Millisecond * 400)

	ok, _ := rc.IsConnWorking()
	if !ok {
		t.Error("ReliableClient.IsConnWorking() with server restarted, want true, got false", rc.Client)
	}

	// Cleant tmp
	tcm.Stop()
	rc.Close()
	os.RemoveAll("/tmp/devosedner-tests-ReliableClient_clientReconnectionDaemon")
}

func TestReliableClient_clientReconnectionDaemon__recreate_error(t *testing.T) {
	// Open new server
	tcm := &tcpMockRelay{}
	err := tcm.Start()
	if err != nil {
		t.Errorf("Error while start tcp mockrelay: %v", err)
		return
	}
	defer tcm.Stop()

	os.RemoveAll("/tmp/devosedner-tests-ReliableClient_clientReconnectionDaemon_recreate_error")

	var appLogBuf bytes.Buffer

	rc, err := NewReliableClientBuilder().
		StatusBuilder(
			status.NewNutsDBStatusBuilder().DbPath("/tmp/devosedner-tests-ReliableClient_clientReconnectionDaemon_recreate_error")).
		ClientBuilder(
			NewClientBuilder().
				EntryPoint("NO_VALID_PROTOCOL://localhost")).
		EnableStandByModeTimeout(time.Millisecond * 50).
		RetryDaemonInitDelay(time.Minute).        // Prevent retry daemon runs during tests
		HouseKeepingDaemonInitDelay(time.Minute). // Prevent retry daemon runs during tests
		ClientReconnDaemonWaitBtwChecks(time.Millisecond * 50).
		ClientReconnDaemonInitDelay(time.Millisecond * 50).
		AppLogger(&applogger.WriterAppLogger{
			Writer: &appLogBuf,
			Level:  applogger.ERROR,
		}).
		Build()
	if err != nil {
		t.Errorf("Error while create reliable client: %v", err)
		return
	}

	// Wait enought time to get reconn daemon shoot twice
	time.Sleep(time.Millisecond * 120)

	got := appLogBuf.String()
	want := `ERROR Error While create new client in Reconnection daemon: while create ` +
		`new DevoSender (Clear): while parse entrypoint NO_VALID_PROTOCOL://localhost: ` +
		`parse "NO_VALID_PROTOCOL://localhost": first path segment in URL cannot contain colon` + "\n" +
		`ERROR Error While create new client in Reconnection daemon: while create ` +
		`new DevoSender (Clear): while parse entrypoint NO_VALID_PROTOCOL://localhost: ` +
		`parse "NO_VALID_PROTOCOL://localhost": first path segment in URL cannot contain colon` + "\n"
	if got != want {
		t.Errorf("ReliableClient.clientReconnectionDaemon() log = %v, want %v", got, want)
	}

	// Cleant tmp
	rc.Close()
	os.RemoveAll("/tmp/devosedner-tests-ReliableClient_clientReconnectionDaemon_recreate_error")
}

func TestReliableClient_clientReconnectionDaemon(t *testing.T) {
	tests := []struct {
		name    string
		dsrc    *ReliableClient
		wantErr bool
	}{
		{
			"waitBtwChecks error",
			&ReliableClient{},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.dsrc.clientReconnectionDaemon(); (err != nil) != tt.wantErr {
				t.Errorf("ReliableClient.clientReconnectionDaemon() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestReliableClient_statusHouseKeepingDaemon__housekeeping_error(t *testing.T) {
	// applogger to check errors
	var buf bytes.Buffer
	al := &applogger.WriterAppLogger{
		Writer: &buf,
		Level:  applogger.INFO,
	}
	// Create client with appLogger
	rc := &ReliableClient{
		houseKeepingDaemon: reliableClientDaemon{
			daemonOpts: daemonOpts{
				waitBtwChecks: time.Millisecond * 50,
			},
		},
		appLogger: al,
	}

	// starts daemon execution
	rc.statusHouseKeepingDaemon()

	// wait and stop daemon
	time.Sleep(time.Millisecond * 50)
	rc.houseKeepingDaemon.stop = true

	// Checks that message is expected
	got := buf.String()
	wantPrefix := "ERROR Error While perform status.HouseKeeping in statusHouseKeepingDaemon: receiver func call with nil pointer\n"
	if !strings.HasPrefix(got, wantPrefix) {
		t.Errorf("ReliableClient.statusHouseKeepingDaemon() logger msg = %v, wantPrefix %v", got, wantPrefix)
	}
}

func TestReliableClient_statusHouseKeepingDaemon__recreateDb(t *testing.T) {
	// applogger to check errors
	var buf bytes.Buffer
	al := &applogger.WriterAppLogger{
		Writer: &buf,
		Level:  applogger.DEBUG,
	}

	os.RemoveAll("/tmp/devosender-tests-ReliableClient_statusHouseKeepingDaemon__recreateDb")

	statusdb, err := status.NewNutsDBStatusBuilder().
		DbPath("/tmp/devosender-tests-ReliableClient_statusHouseKeepingDaemon__recreateDb").
		DbSegmentSize(256).
		FilesToConsolidateDb(1024). // To prevent consolidate files right now
		RecreateDbClientAfterConsolidation(false).
		Build()
	if err != nil {
		panic(err)
	}

	// status db with events to create files
	for i := 1; i <= 10; i++ {
		id := fmt.Sprintf("id-%d", i)
		msg := fmt.Sprintf("msg number %d", i)
		err := statusdb.New(
			&status.EventRecord{
				AsyncIDs:  []string{id},
				Msg:       msg,
				Timestamp: time.Now(), // Required to prevent expired records
			},
		)

		if err != nil {
			panic(err)
		}
	}

	err = statusdb.Close()
	if err != nil {
		panic(err)
	}

	// Create client with appLogger
	rc := &ReliableClient{
		houseKeepingDaemon: reliableClientDaemon{
			daemonOpts: daemonOpts{
				waitBtwChecks: time.Millisecond * 100,
			},
		},
		appLogger: al,
		status: func() status.Status {
			r, err := status.NewNutsDBStatusBuilder().
				DbPath("/tmp/devosender-tests-ReliableClient_statusHouseKeepingDaemon__recreateDb").
				DbSegmentSize(256). // Important: This value should be enought to adming Merges
				FilesToConsolidateDb(2).
				RecreateDbClientAfterConsolidation(true).
				Build()
			if err != nil {
				panic(err)
			}

			// Remove events to ensure consolidation remove files
			for i := 2; i <= 10; i++ {
				id := fmt.Sprintf("id-%d", i)
				err := r.FinishRecord(id)

				if err != nil {
					msg := fmt.Sprintf("Error while finsih record which id is %s: %v", id, err)
					panic(msg)
				}
			}
			return r
		}(),
	}

	// starts daemon execution
	rc.statusHouseKeepingDaemon()

	// wait for consolidation call stop daemon
	time.Sleep(time.Millisecond * 200)

	// Checks that message is expected
	got := buf.String()
	wantPrefix := "DEBUG statusHouseKeepingDaemon working: { waitBtwChecks: 100ms, initDelay: 0s, stop: false}\n" +
		"DEBUG statusHouseKeepingDaemon shot: { waitBtwChecks: 100ms, initDelay: 0s, stop: false}\n" +
		"DEBUG Status HouseKeeping results: Before: KeyCount: 38, ListIdx: map[], consolidationDbNumFilesThreshold: 2, dbFiles: 17, initialized: true, bufferSize: 500000, eventTTL: 3600, recreateDbClientAfterConsolidation: true, After: KeyCount: 3, ListIdx: map[], consolidationDbNumFilesThreshold: 2, dbFiles: 2, initialized: true, bufferSize: 500000, eventTTL: 3600, recreateDbClientAfterConsolidation: true\n"
	if !strings.HasPrefix(got, wantPrefix) {
		t.Errorf("ReliableClient.statusHouseKeepingDaemon() logger msg = %v, wantPrefix %v", got, wantPrefix)
	}

	os.RemoveAll("/tmp/devosender-tests-ReliableClient_statusHouseKeepingDaemon__recreateDb")
}

func TestReliableClient_statusHouseKeepingDaemon(t *testing.T) {
	tests := []struct {
		name    string
		dsrc    *ReliableClient
		wantErr bool
	}{
		{
			"waitBtwChecks error",
			&ReliableClient{},
			true,
		},
		{
			"Daemon stopped",
			&ReliableClient{
				appLogger: &applogger.NoLogAppLogger{},
				houseKeepingDaemon: reliableClientDaemon{
					daemonOpts: daemonOpts{
						waitBtwChecks: time.Second,
					},
					stop: true,
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.dsrc.statusHouseKeepingDaemon(); (err != nil) != tt.wantErr {
				t.Errorf("ReliableClient.statusHouseKeepingDaemon() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestReliableClient_resendRecord(t *testing.T) {
	type args struct {
		r *status.EventRecord
	}
	tests := []struct {
		name           string
		reliableClient *ReliableClient
		args           args
		wantErr        bool
	}{
		{
			"Client StandBy no-conn record",
			func() *ReliableClient {
				os.RemoveAll("/tmp/tests-reliable-resendRecord")

				r, err := NewReliableClientBuilder().
					StatusBuilder(
						status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-resendRecord")).
					ClientBuilder(
						NewClientBuilder().EntryPoint("upd://localhost:13000")).
					Build()
				if err != nil {
					panic(err)
				}

				// Create record
				r.status.New(
					&status.EventRecord{
						AsyncIDs: []string{nonConnIDPrefix + "11111"},
					},
				)

				// pass to standby mode
				err = r.StandBy()
				if err != nil {
					panic(err)
				}

				return r
			}(),
			args{
				&status.EventRecord{
					AsyncIDs: []string{nonConnIDPrefix + "11111"},
				},
			},
			false,
		},
		{
			"Client nil no-conn record",
			func() *ReliableClient {
				os.RemoveAll("/tmp/tests-reliable-resendRecord-client-nil")

				r, err := NewReliableClientBuilder().
					StatusBuilder(
						status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-resendRecord-client-nil")).
					ClientBuilder(
						NewClientBuilder().EntryPoint("udp://localhost:13000")).
					Build()
				if err != nil {
					panic(err)
				}

				// Create record
				r.status.New(
					&status.EventRecord{
						AsyncIDs: []string{nonConnIDPrefix + "11111"},
					},
				)

				// Force client to nil
				r.Client.Close()
				r.Client = nil
				return r
			}(),
			args{
				&status.EventRecord{
					AsyncIDs: []string{nonConnIDPrefix + "11111"},
				},
			},
			false,
		},
		{
			"Client StandBy record",
			func() *ReliableClient {
				os.RemoveAll("/tmp/tests-reliable-resendRecord-usual-record")

				r, err := NewReliableClientBuilder().
					StatusBuilder(
						status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-resendRecord-usual-record")).
					ClientBuilder(
						NewClientBuilder().EntryPoint("udp://localhost:13000")).
					Build()
				if err != nil {
					panic(err)
				}

				// Create record
				r.status.New(
					&status.EventRecord{
						AsyncIDs:  []string{"11111"},
						Timestamp: time.Now(), // prevents record expiration
					},
				)

				// pass to standby mode
				err = r.StandBy()
				if err != nil {
					panic(err)
				}
				return r
			}(),
			args{
				&status.EventRecord{
					AsyncIDs: []string{"11111"},
				},
			},
			false,
		},
		{
			"Error updating record",
			func() *ReliableClient {
				os.RemoveAll("/tmp/tests-reliable-resendRecord-error-update")

				r, err := NewReliableClientBuilder().
					StatusBuilder(
						status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-resendRecord-error-update")).
					ClientBuilder(
						NewClientBuilder().EntryPoint("udp://localhost:13000")).
					Build()
				if err != nil {
					panic(err)
				}

				// Create record
				r.status.New(
					&status.EventRecord{
						AsyncIDs: []string{"11111"},
					},
				)

				// Close status database to force error
				r.status.Close()

				return r
			}(),
			args{
				&status.EventRecord{
					AsyncIDs: []string{"11111"},
				},
			},
			true,
		},
		{
			"With compressor",
			func() *ReliableClient {
				os.RemoveAll("/tmp/tests-reliable-resendRecord-compressor")

				r, err := NewReliableClientBuilder().
					StatusBuilder(
						status.NewNutsDBStatusBuilder().DbPath("/tmp/tests-reliable-resendRecord-compressor")).
					ClientBuilder(
						NewClientBuilder().EntryPoint("udp://localhost:13000")).
					Build()
				if err != nil {
					panic(err)
				}

				// Create record
				r.status.New(
					&status.EventRecord{
						AsyncIDs:   []string{"11111"},
						Compressor: &compressor.Compressor{Algorithm: compressor.CompressorGzip},
						Tag:        "test.keep.free",
						Msg:        "The message",
						Timestamp:  time.Now(), // prevents record expiration
					},
				)
				return r
			}(),
			args{
				&status.EventRecord{
					AsyncIDs:   []string{"11111"},
					Compressor: &compressor.Compressor{Algorithm: compressor.CompressorGzip},
					Tag:        "test.keep.free",
					Msg:        "The message",
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.reliableClient.resendRecord(tt.args.r); (err != nil) != tt.wantErr {
				t.Errorf("ReliableClient.resendRecord() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	os.RemoveAll("/tmp/tests-reliable-resendRecord")
	os.RemoveAll("/tmp/tests-reliable-resendRecord-client-nil")
	os.RemoveAll("/tmp/tests-reliable-resendRecord-usual-record")
	os.RemoveAll("/tmp/tests-reliable-resendRecord-error-update")
	os.RemoveAll("/tmp/tests-reliable-resendRecord-compressor")
}

type tcpMockRelay struct {
	Host   string
	Port   int
	Lines  []string
	conns  []net.Conn
	list   net.Listener
	Errors []error
	stop   bool
}

func (tmr *tcpMockRelay) Start() error {
	if tmr.Host == "" {
		tmr.Host = "0.0.0.0"
	}

	var err error
	tmr.list, err = net.Listen(
		"tcp", tmr.Host+":"+fmt.Sprintf(
			"%d", tmr.Port))
	if err != nil {
		return fmt.Errorf("While create Listener: %w", err)
	}

	if tmr.Port == 0 {
		tmr.Port = int(tmr.list.Addr().(*net.TCPAddr).Port)
	}

	go func() {
		for !tmr.stop {
			c, err := tmr.list.Accept()
			if err != nil {
				tmr.Errors = append(tmr.Errors, fmt.Errorf("While listener accepts new conn: %w", err))
				return
			}

			go tmr.handleConnection(c)
		}
	}()

	return nil
}

func (tmr *tcpMockRelay) Stop() error {
	var errReturned error

	tmr.stop = true

	for _, conn := range tmr.conns {
		err := conn.Close()
		if err != nil {
			errReturned = fmt.Errorf("%v, While close connection: %w", errReturned, err)
		}
	}

	if tmr.list != nil {
		err := tmr.list.Close()
		if err != nil {
			errReturned = fmt.Errorf("%v, While close listener: %w", errReturned, err)
		}
	}

	return errReturned
}

func (tmr *tcpMockRelay) handleConnection(c net.Conn) {
	tmr.conns = append(tmr.conns, c)

	for !tmr.stop {

		bs := make([]byte, 256)
		n, err := c.Read(bs)
		if err == io.EOF {
			return
		}
		if err != nil {
			return
		}

		if n > 0 {
			tmr.Lines = append(tmr.Lines, string(bs))
		}
	}
}
