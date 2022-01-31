package devosender

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/xujiajun/nutsdb"

	"github.com/cyberluisda/devo-go/applogger"
	"github.com/cyberluisda/devo-go/devosender/compressor"
)

func TestReliableClientBuilder_DbSegmentSize(t *testing.T) {
	type fields struct {
		dbOpts nutsdb.Options
	}
	type args struct {
		size int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ReliableClientBuilder
	}{
		{
			"With value",
			fields{nutsdb.Options{}},
			args{128},
			&ReliableClientBuilder{
				dbOpts: nutsdb.Options{
					SegmentSize: 128,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrcb := &ReliableClientBuilder{
				dbOpts: tt.fields.dbOpts,
			}
			if got := dsrcb.DbSegmentSize(tt.args.size); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReliableClientBuilder.DbSegmentSize() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestReliableClientBuilder_DbEntryIdxMode(t *testing.T) {
	type fields struct {
		dbOpts nutsdb.Options
	}
	type args struct {
		mode nutsdb.EntryIdxMode
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ReliableClientBuilder
	}{
		{
			"Default value",
			fields{nutsdb.Options{}},
			args{},
			&ReliableClientBuilder{
				dbOpts: nutsdb.Options{
					EntryIdxMode: nutsdb.HintKeyValAndRAMIdxMode,
				},
			},
		},
		{
			"With value",
			fields{nutsdb.Options{}},
			args{nutsdb.HintKeyAndRAMIdxMode},
			&ReliableClientBuilder{
				dbOpts: nutsdb.Options{
					EntryIdxMode: nutsdb.HintKeyAndRAMIdxMode,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrcb := &ReliableClientBuilder{
				dbOpts: tt.fields.dbOpts,
			}
			if got := dsrcb.DbEntryIdxMode(tt.args.mode); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReliableClientBuilder.DbEntryIdxMode() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestReliableClientBuilder_DbRWMode(t *testing.T) {
	type fields struct {
		dbOpts nutsdb.Options
	}
	type args struct {
		mode nutsdb.RWMode
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ReliableClientBuilder
	}{
		{
			"Default value",
			fields{nutsdb.Options{}},
			args{},
			&ReliableClientBuilder{
				dbOpts: nutsdb.Options{
					RWMode:               nutsdb.FileIO,
					StartFileLoadingMode: nutsdb.FileIO,
				},
			},
		},
		{
			"With value",
			fields{nutsdb.Options{}},
			args{nutsdb.MMap},
			&ReliableClientBuilder{
				dbOpts: nutsdb.Options{
					RWMode:               nutsdb.MMap,
					StartFileLoadingMode: nutsdb.MMap,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrcb := &ReliableClientBuilder{
				dbOpts: tt.fields.dbOpts,
			}
			if got := dsrcb.DbRWMode(tt.args.mode); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReliableClientBuilder.DbRWMode() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

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

func TestReliableClientBuilder_ConsolidateDbDaemonWaitBtwChecks(t *testing.T) {
	type fields struct {
		consolidateDbDaemonOpts daemonOpts
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
				consolidateDbDaemonOpts: daemonOpts{waitBtwChecks: time.Second},
			},
		},
		{
			"Value greater than 0",
			fields{daemonOpts{waitBtwChecks: time.Second}},
			args{time.Minute},
			&ReliableClientBuilder{
				consolidateDbDaemonOpts: daemonOpts{waitBtwChecks: time.Minute},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrcb := &ReliableClientBuilder{
				consolidateDbDaemonOpts: tt.fields.consolidateDbDaemonOpts,
			}
			if got := dsrcb.ConsolidateDbDaemonWaitBtwChecks(tt.args.d); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReliableClientBuilder.ConsolidateDbDaemonWaitBtwChecks() = %+v, want %+v", got, tt.want)
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

func TestReliableClientBuilder_ConsolidateDbDaemonInitDelay(t *testing.T) {
	type fields struct {
		consolidateDbDaemonOpts daemonOpts
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
				consolidateDbDaemonOpts: daemonOpts{initDelay: time.Second},
			},
		},
		{
			"Value greater than 0",
			fields{daemonOpts{initDelay: time.Second}},
			args{time.Minute},
			&ReliableClientBuilder{
				consolidateDbDaemonOpts: daemonOpts{initDelay: time.Minute},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrcb := &ReliableClientBuilder{
				consolidateDbDaemonOpts: tt.fields.consolidateDbDaemonOpts,
			}
			if got := dsrcb.ConsolidateDbDaemonInitDelay(tt.args.d); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReliableClientBuilder.ConsolidateDbDaemonInitDelay() = %+v, want %+v", got, tt.want)
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

func TestReliableClientBuilder_ConsolidateDbNumFiles(t *testing.T) {
	type fields struct {
		consolidateDbNumFiles uint8
	}
	type args struct {
		i uint8
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *ReliableClientBuilder
	}{
		{
			"Param value less than 2",
			fields{consolidateDbNumFiles: 23},
			args{0},
			&ReliableClientBuilder{
				consolidateDbNumFiles: 23,
			},
		},
		{
			"Param value equals to 2",
			fields{consolidateDbNumFiles: 24},
			args{2},
			&ReliableClientBuilder{
				consolidateDbNumFiles: 2,
			},
		},
		{
			"Param value greater than 2",
			fields{},
			args{25},
			&ReliableClientBuilder{
				consolidateDbNumFiles: 25,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrcb := &ReliableClientBuilder{
				consolidateDbNumFiles: tt.fields.consolidateDbNumFiles,
			}
			if got := dsrcb.ConsolidateDbNumFiles(tt.args.i); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReliableClientBuilder.ConsolidateDbNumFiles() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestReliableClientBuilder_Build(t *testing.T) {
	type fields struct {
		clientBuilder            *ClientBuilder
		dbOpts                   nutsdb.Options
		retryDaemonOpts          daemonOpts
		clientReconnOpts         daemonOpts
		consolidateDbDaemonOpts  daemonOpts
		consolidateDbNumFiles    uint8
		daemonStopTimeout        time.Duration
		bufferEventsSize         uint
		eventTimeToLive          uint32
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
				dbOpts: nutsdb.Options{
					Dir: "/tmp/test-builder-build",
				},
			},
			nil,
			true,
		},
		{
			"Client build error",
			fields{
				dbOpts: nutsdb.Options{
					Dir: "/tmp/test-builder-build",
				},
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
				dbOpts:                nutsdbOptionsWithDir("/tmp/test-builder-build"),
				appLogger:             &applogger.NoLogAppLogger{},
				consolidateDbNumFiles: 2,

				retryDaemonOpts: daemonOpts{
					initDelay:     time.Minute,
					waitBtwChecks: time.Millisecond * 100,
				},
				clientReconnOpts: daemonOpts{
					initDelay:     time.Minute,
					waitBtwChecks: time.Millisecond * 100,
				},
				consolidateDbDaemonOpts: daemonOpts{
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
				dbOpts: nutsdbOptionsWithDir("/tmp/test-builder-build"),
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
				consolidateDaemon: reliableClientDaemon{
					daemonOpts: daemonOpts{
						initDelay:     time.Minute,
						waitBtwChecks: time.Millisecond * 100,
					},
				},
				dbInitCleanedup:       true,
				consolidateDbNumFiles: 2,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrcb := &ReliableClientBuilder{
				clientBuilder:            tt.fields.clientBuilder,
				dbOpts:                   tt.fields.dbOpts,
				retryDaemonOpts:          tt.fields.retryDaemonOpts,
				clientReconnOpts:         tt.fields.clientReconnOpts,
				consolidateDbDaemonOpts:  tt.fields.consolidateDbDaemonOpts,
				consolidateDbNumFiles:    tt.fields.consolidateDbNumFiles,
				daemonStopTimeout:        tt.fields.daemonStopTimeout,
				bufferEventsSize:         tt.fields.bufferEventsSize,
				eventTimeToLive:          tt.fields.eventTimeToLive,
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
				tt.want.db = got.db
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
					DbPath("/tmp/tests-reliable-sends-async-no-conn").
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
					DbPath("/tmp/tests-reliable-sends-async-with-conn").
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
					DbPath("/tmp/tests-reliable-SendWTagAndCompressorAsync-no-conn").
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
					DbPath("/tmp/tests-reliable-SendWTagAndCompressorAsync-with-conn").
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
		Client                   *Client
		clientBuilder            *ClientBuilder
		db                       *nutsdb.DB
		bufferSize               uint
		eventTTLSeconds          uint32
		retryDaemon              reliableClientDaemon
		reconnDaemon             reliableClientDaemon
		daemonStopTimeout        time.Duration
		standByMode              bool
		enableStandByModeTimeout time.Duration
		dbInitCleanedup          bool
		daemonStopped            chan bool
		flushTimeout             time.Duration
		appLogger                applogger.SimpleAppLogger
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
				db: func() *nutsdb.DB {
					// Clean previous status if exists
					os.RemoveAll("/tmp/tests-reliable-Flush")
					opts := nutsdbOptionsWithDir("/tmp/tests-reliable-Flush")

					r, err := nutsdb.Open(opts)
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
				db: func() *nutsdb.DB {
					// Clean previous status if exists
					os.RemoveAll("/tmp/tests-reliable-Flush-pending")
					opts := nutsdbOptionsWithDir("/tmp/tests-reliable-Flush-pending")

					r, err := nutsdb.Open(opts)
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
				db: func() *nutsdb.DB {
					// Clean previous status if exists
					os.RemoveAll("/tmp/tests-reliable-Flush-pending-errors")
					opts := nutsdbOptionsWithDir("/tmp/tests-reliable-Flush-pending-errors")

					r, err := nutsdb.Open(opts)
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
				db: func() *nutsdb.DB {
					// Clean previous status if exists
					os.RemoveAll("/tmp/tests-reliable-Flush-pending-errors-no-conn")
					opts := nutsdbOptionsWithDir("/tmp/tests-reliable-Flush-pending-errors-no-conn")

					r, err := nutsdb.Open(opts)
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrc := &ReliableClient{
				Client:                   tt.fields.Client,
				clientBuilder:            tt.fields.clientBuilder,
				db:                       tt.fields.db,
				bufferSize:               tt.fields.bufferSize,
				eventTTLSeconds:          tt.fields.eventTTLSeconds,
				retryDaemon:              tt.fields.retryDaemon,
				reconnDaemon:             tt.fields.reconnDaemon,
				daemonStopTimeout:        tt.fields.daemonStopTimeout,
				standByMode:              tt.fields.standByMode,
				enableStandByModeTimeout: tt.fields.enableStandByModeTimeout,
				dbInitCleanedup:          tt.fields.dbInitCleanedup,
				daemonStopped:            tt.fields.daemonStopped,
				flushTimeout:             tt.fields.flushTimeout,
				appLogger:                tt.fields.appLogger,
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
}

func TestReliableClient_Close(t *testing.T) {
	type fields struct {
		Client                   *Client
		clientBuilder            *ClientBuilder
		db                       *nutsdb.DB
		bufferSize               uint
		eventTTLSeconds          uint32
		retryDaemon              reliableClientDaemon
		reconnDaemon             reliableClientDaemon
		daemonStopTimeout        time.Duration
		standByMode              bool
		enableStandByModeTimeout time.Duration
		dbInitCleanedup          bool
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
				db: func() *nutsdb.DB {
					// Clean previous status if exists
					os.RemoveAll("/tmp/tests-reliable-Close")
					opts := nutsdbOptionsWithDir("/tmp/tests-reliable-Close")

					r, err := nutsdb.Open(opts)
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
				db:                       tt.fields.db,
				bufferSize:               tt.fields.bufferSize,
				eventTTLSeconds:          tt.fields.eventTTLSeconds,
				retryDaemon:              tt.fields.retryDaemon,
				reconnDaemon:             tt.fields.reconnDaemon,
				daemonStopTimeout:        tt.fields.daemonStopTimeout,
				standByMode:              tt.fields.standByMode,
				enableStandByModeTimeout: tt.fields.enableStandByModeTimeout,
				dbInitCleanedup:          tt.fields.dbInitCleanedup,
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
					DbPath("/tmp/tests-reliable-StandBy").
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
					DbPath("/tmp/tests-reliable-StandBy-close-client").
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
					DbPath("/tmp/tests-reliable-WakeUp").
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
		DbPath("/tmp/devosedner-tests-ReliableClient_IsStandBy_closedRelayConn").
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

func TestReliableClient_ConsolidateStatusDb(t *testing.T) {
	tests := []struct {
		name              string
		dsrc              *ReliableClient
		wantErr           bool
		wantNumberOfFiles int
	}{
		{
			"Nil ReliableClient error",
			nil,
			true,
			0,
		},
		{
			"Nil stuts db error",
			&ReliableClient{},
			true,
			0,
		},
		{
			"Threshold not enought error",
			&ReliableClient{
				db: func() *nutsdb.DB {
					os.RemoveAll("/tmp/devosedner-tests-ReliableClient_ConsolidateStatusDb")

					opts := nutsdbOptionsWithDir("/tmp/devosedner-tests-ReliableClient_ConsolidateStatusDb")
					r, err := nutsdb.Open(opts)
					if err != nil {
						panic(err)
					}

					return r
				}(),
			},
			true,
			0,
		},
		{
			"Consolidation error",
			&ReliableClient{
				dbOpts:    nutsdbOptionsWithDir("/tmp/devosedner-tests-ReliableClient_ConsolidateStatusDb_noNeeded"),
				appLogger: &applogger.NoLogAppLogger{},
				db: func() *nutsdb.DB {
					os.RemoveAll("/tmp/devosedner-tests-ReliableClient_ConsolidateStatusDb_noNeeded")

					opts := nutsdbOptionsWithDir("/tmp/devosedner-tests-ReliableClient_ConsolidateStatusDb_noNeeded")
					r, err := nutsdb.Open(opts)
					if err != nil {
						panic(err)
					}

					// Write one event to get at least one file
					err = r.Update(func(tx *nutsdb.Tx) error {
						val := bytes.Repeat([]byte("0"), 68)
						err := tx.Put("tmp", []byte("key-1"), val, 0)
						if err != nil {
							return fmt.Errorf("While write test key %s: %w", "key-1", err)
						}
						return nil
					})
					if err != nil {
						panic(err)
					}

					return r
				}(),
				consolidateDbNumFiles: 1,
			},
			true,
			1,
		},
		{
			"Consolidation not needed",
			&ReliableClient{
				dbOpts:    nutsdbOptionsWithDir("/tmp/devosedner-tests-ReliableClient_ConsolidateStatusDb_noNeeded"),
				appLogger: &applogger.NoLogAppLogger{},
				db: func() *nutsdb.DB {
					os.RemoveAll("/tmp/devosedner-tests-ReliableClient_ConsolidateStatusDb_noNeeded")

					opts := nutsdbOptionsWithDir("/tmp/devosedner-tests-ReliableClient_ConsolidateStatusDb_noNeeded")
					r, err := nutsdb.Open(opts)
					if err != nil {
						panic(err)
					}

					// Write one event to get at least one file
					err = r.Update(func(tx *nutsdb.Tx) error {
						val := bytes.Repeat([]byte("0"), 68)
						err := tx.Put("tmp", []byte("key-1"), val, 0)
						if err != nil {
							return fmt.Errorf("While write test key %s: %w", "key-1", err)
						}
						return nil
					})
					if err != nil {
						panic(err)
					}

					return r
				}(),
				consolidateDbNumFiles: 2,
			},
			false,
			1,
		},
		{
			"Consolidation",
			&ReliableClient{
				dbOpts:    nutsdbOptionsWithDir("/tmp/devosedner-tests-ReliableClient_ConsolidateStatusDb_done"),
				appLogger: &applogger.NoLogAppLogger{},
				db: func() *nutsdb.DB {
					os.RemoveAll("/tmp/devosedner-tests-ReliableClient_ConsolidateStatusDb_done")

					opts := nutsdbOptionsWithDir("/tmp/devosedner-tests-ReliableClient_ConsolidateStatusDb_done")
					opts.SegmentSize = 128 // Very small size
					r, err := nutsdb.Open(opts)
					if err != nil {
						panic(err)
					}

					// Write keys gt than 128 bytes to ensure we have more than two files
					err = r.Update(func(tx *nutsdb.Tx) error {
						val := bytes.Repeat([]byte("0"), 68)
						for i := 1; i <= 20; i++ {
							key := fmt.Sprintf("key-%d", i)
							err := tx.Put("tmp", []byte(key), val, 0)
							if err != nil {
								return fmt.Errorf("While write test key %s: %w", key, err)
							}
						}
						return nil
					})
					if err != nil {
						panic(err)
					}

					// Delete keys to get "recoverable" space on files
					err = r.Update(func(tx *nutsdb.Tx) error {
						for i := 1; i <= 20; i++ {
							key := fmt.Sprintf("key-%d", i)
							err := tx.Delete("tmp", []byte(key))
							if err != nil {
								return fmt.Errorf("While delete test key %s: %w", key, err)
							}
						}
						return nil
					})
					if err != nil {
						panic(err)
					}

					return r
				}(),
				consolidateDbNumFiles: 2,
			},
			false,
			0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.dsrc.ConsolidateStatusDb(); (err != nil) != tt.wantErr {
				t.Errorf("ReliableClient.ConsolidateStatusDb() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.dsrc != nil && tt.dsrc.dbOpts.Dir != "" {
				if nfiles := numberOfFiles(tt.dsrc.dbOpts.Dir); nfiles != tt.wantNumberOfFiles {
					t.Errorf("ReliableClient.ConsolidateStatusDb() numberOfFiles = %v, want %v", nfiles, tt.wantNumberOfFiles)
				}
			}
		})
	}

	//Clen tmp
	os.RemoveAll("/tmp/devosedner-tests-ReliableClient_ConsolidateStatusDb")
	os.RemoveAll("/tmp/devosedner-tests-ReliableClient_ConsolidateStatusDb_noNeeded")
	os.RemoveAll("/tmp/devosedner-tests-ReliableClient_ConsolidateStatusDb_done")
}

func TestReliableClient_String(t *testing.T) {
	type fields struct {
		Client                   *Client
		clientBuilder            *ClientBuilder
		db                       *nutsdb.DB
		bufferSize               uint
		eventTTLSeconds          uint32
		retryDaemon              reliableClientDaemon
		reconnDaemon             reliableClientDaemon
		daemonStopTimeout        time.Duration
		standByMode              bool
		enableStandByModeTimeout time.Duration
		dbInitCleanedup          bool
		daemonStopped            chan bool
		flushTimeout             time.Duration
		consolidateDbNumFiles    uint8
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			"Empty",
			fields{},
			"Client: {<nil>}, db: <nil>, bufferSize: 0, eventTTLSeconds: 0, retryDaemon: " +
				"{ waitBtwChecks: 0s, initDelay: 0s, stop: false}, reconnDaemon: " +
				"{ waitBtwChecks: 0s, initDelay: 0s, stop: false}, consolidateDbDaemon: " +
				"{ waitBtwChecks: 0s, initDelay: 0s, stop: false}, daemonStopTimeout: 0s, " +
				"standByMode: false, enableStandByModeTimeout: 0s, dbInitCleanedup: false, " +
				"daemonStopped: <nil>, flushTimeout: 0s",
		},
		{
			"With some values",
			fields{
				Client: &Client{
					entryPoint: "udp://example.com:80",
				},
				db:              &nutsdb.DB{},
				bufferSize:      123,
				eventTTLSeconds: 20,
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
				consolidateDbNumFiles:    4,
			},
			"Client: {entryPoint: 'udp://example.com:80', syslogHostname: '', defaultTag: '', " +
				"connAddr: '<nil>', ReplaceSequences: map[], tls: <nil>, #asyncErrors: 0, " +
				"tcp: {<nil>} -> <nil>, connectionUsedTimestamp: '0001-01-01 00:00:00 +0000 UTC', " +
				"maxTimeConnActive: '0s', #asyncItems: 0, lastSendCallTimestamp: " +
				"'0001-01-01 00:00:00 +0000 UTC'}, db: {KeyCount: 0, ListIdx: map[], " +
				"consolidationDbNumFilesThreshold: 4, dbFiles: 0}, bufferSize: 123, " +
				"eventTTLSeconds: 20, retryDaemon: { waitBtwChecks: 1m0s, initDelay: 2s, " +
				"stop: true}, reconnDaemon: { waitBtwChecks: 10s, initDelay: 1s, stop: true}, " +
				"consolidateDbDaemon: { waitBtwChecks: 0s, initDelay: 0s, stop: false}, " +
				"daemonStopTimeout: 5s, standByMode: true, enableStandByModeTimeout: 3s, " +
				"dbInitCleanedup: true, daemonStopped: <nil>, flushTimeout: 2m0s",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrc := &ReliableClient{
				Client:                   tt.fields.Client,
				clientBuilder:            tt.fields.clientBuilder,
				db:                       tt.fields.db,
				bufferSize:               tt.fields.bufferSize,
				eventTTLSeconds:          tt.fields.eventTTLSeconds,
				retryDaemon:              tt.fields.retryDaemon,
				reconnDaemon:             tt.fields.reconnDaemon,
				daemonStopTimeout:        tt.fields.daemonStopTimeout,
				standByMode:              tt.fields.standByMode,
				enableStandByModeTimeout: tt.fields.enableStandByModeTimeout,
				dbInitCleanedup:          tt.fields.dbInitCleanedup,
				daemonStopped:            tt.fields.daemonStopped,
				flushTimeout:             tt.fields.flushTimeout,
				consolidateDbNumFiles:    tt.fields.consolidateDbNumFiles,
			}
			if got := dsrc.String(); got != tt.want {
				t.Errorf("ReliableClient.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReliableClient_daemonsSartup(t *testing.T) {
	type fields struct {
		db                    *nutsdb.DB
		retryDaemon           reliableClientDaemon
		reconnDaemon          reliableClientDaemon
		consolidateDaemon     reliableClientDaemon
		appLogger             applogger.SimpleAppLogger
		consolidateDbNumFiles uint8
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			"Error: status db nil",
			fields{
				appLogger:             &applogger.NoLogAppLogger{},
				consolidateDbNumFiles: 2, //ConsolidateStatusDb required field
			},
			true,
		},
		{
			"Error: db intialization",
			fields{
				appLogger:             &applogger.NoLogAppLogger{},
				consolidateDbNumFiles: 2, //ConsolidateStatusDb required field
				db: func() *nutsdb.DB {
					// Ensure db stauts is clean
					os.RemoveAll("/tmp/tests-reliable-daemonsSartup")
					opts := nutsdbOptionsWithDir("/tmp/tests-reliable-daemonsSartup")
					r, err := nutsdb.Open(opts)
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
				appLogger:             &applogger.NoLogAppLogger{},
				consolidateDbNumFiles: 2, //ConsolidateStatusDb required field
				db: func() *nutsdb.DB {
					// Ensure db stauts is clean
					os.RemoveAll("/tmp/tests-reliable-daemonsSartup-retryEvents")
					opts := nutsdbOptionsWithDir("/tmp/tests-reliable-daemonsSartup-retryEvents")
					r, err := nutsdb.Open(opts)
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
				appLogger:             &applogger.NoLogAppLogger{},
				consolidateDbNumFiles: 2, //ConsolidateStatusDb required field
				db: func() *nutsdb.DB {
					// Ensure db stauts is clean
					os.RemoveAll("/tmp/tests-reliable-daemonsSartup-clientReconn")
					opts := nutsdbOptionsWithDir("/tmp/tests-reliable-daemonsSartup-clientReconn")
					r, err := nutsdb.Open(opts)
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
			"Error: consolidate db daemon",
			fields{
				appLogger:             &applogger.NoLogAppLogger{},
				consolidateDbNumFiles: 2, //ConsolidateStatusDb required field
				db: func() *nutsdb.DB {
					// Ensure db stauts is clean
					os.RemoveAll("/tmp/tests-reliable-daemonsSartup-clientReconn")
					opts := nutsdbOptionsWithDir("/tmp/tests-reliable-daemonsSartup-clientReconn")
					r, err := nutsdb.Open(opts)
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
				consolidateDaemon: reliableClientDaemon{
					daemonOpts: daemonOpts{
						waitBtwChecks: -2 * time.Second}},
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrc := &ReliableClient{
				db:                    tt.fields.db,
				retryDaemon:           tt.fields.retryDaemon,
				reconnDaemon:          tt.fields.reconnDaemon,
				consolidateDaemon:     tt.fields.consolidateDaemon,
				appLogger:             tt.fields.appLogger,
				consolidateDbNumFiles: tt.fields.consolidateDbNumFiles,
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
}

func TestReliableClient_daemonsSartup_errorAsyncClosing(t *testing.T) {
	os.RemoveAll("/tmp/tests-reliable-daemonsSartup-errorAsyncClosing")
	dsrc, err := NewReliableClientBuilder().
		DbPath("/tmp/tests-reliable-daemonsSartup-errorAsyncClosing").
		ClientBuilder(
			NewClientBuilder().EntryPoint("udp://example.com:80")).
		Build()
	if err != nil {
		panic(err)
	}

	time.Sleep(time.Millisecond * 100) // Enough time to dbInitCleanup

	dsrc.db.Close() // Force error when daemon close database

	t.Logf("Send term signal")
	syscall.Kill(syscall.Getpid(), syscall.SIGTERM)

	os.RemoveAll("/tmp/tests-reliable-daemonsSartup-errorAsyncClosing")
}

func TestReliableClient_dbInitCleanup(t *testing.T) {
	type fields struct {
		db                    *nutsdb.DB
		dbInitCleanedup       bool
		appLogger             applogger.SimpleAppLogger
		consolidateDbNumFiles uint8
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			"Previously initiated",
			fields{
				dbInitCleanedup: true,
			},
			false,
		},
		{
			"With previous no-conn data",
			fields{
				db: func() *nutsdb.DB {
					// Ensure previous execution data is cleaned
					os.RemoveAll("/tmp/tests-reliable-dbInitCleanup")

					// Using internal funcs of temporal reliable client with current db state for easy setup
					rClient, err := NewReliableClientBuilder().
						DbPath("/tmp/tests-reliable-dbInitCleanup").
						ClientBuilder(
							NewClientBuilder().EntryPoint("udp://localhost:13000")).
						Build()
					if err != nil {
						panic(err)
					}

					// Add data
					err = rClient.StandBy()
					if err != nil {
						panic(err)
					}
					rClient.SendWTagAsync("tag.1", "no con msg")
					// Close client
					rClient.Close()

					// Open new database
					opts := nutsdbOptionsWithDir("/tmp/tests-reliable-dbInitCleanup")
					r, err := nutsdb.Open(opts)
					if err != nil {
						panic(err)
					}
					return r
				}(),
				appLogger:             &applogger.NoLogAppLogger{},
				consolidateDbNumFiles: 1,
			},
			false,
		},
		{
			"With previous conn data",
			fields{
				db: func() *nutsdb.DB {
					// Ensure previous execution data is cleaned
					os.RemoveAll("/tmp/tests-reliable-dbInitCleanup-conndata")

					// Using internal funcs of temporal reliable client with current db state for easy setup
					rClient, err := NewReliableClientBuilder().
						DbPath("/tmp/tests-reliable-dbInitCleanup-conndata").
						ClientBuilder(
							NewClientBuilder().EntryPoint("udp://localhost:13000")).
						Build()
					if err != nil {
						panic(err)
					}

					// Add data
					err = rClient.StandBy()
					if err != nil {
						panic(err)
					}
					id := rClient.SendWTagAsync("tag.1", "no con msg")
					internalRecord, err := rClient.getRecord(id)
					if err != nil {
						panic(err)
					}
					// Close client
					rClient.Close()

					// Open new database
					opts := nutsdbOptionsWithDir("/tmp/tests-reliable-dbInitCleanup-conndata")
					r, err := nutsdb.Open(opts)
					if err != nil {
						panic(err)
					}

					// Now change uid to simulate conn data,
					// We do directly to status database instead of using rClient because internal daemons will fix
					// the status
					newID := id[len(nonConnIDPrefix):]
					err = r.Update(func(tx *nutsdb.Tx) error {
						return updateRecordInTx(tx, internalRecord, newID, 20)
					})
					if err != nil {
						panic(err)
					}

					return r
				}(),
				// appLogger: &applogger.NoLogAppLogger{},
				appLogger:             &applogger.WriterAppLogger{Writer: os.Stdout, Level: applogger.DEBUG},
				consolidateDbNumFiles: 1,
			},
			false,
		},
		{
			"Truncate keys in order",
			fields{
				db: func() *nutsdb.DB {
					// Ensure previous execution data is cleaned
					os.RemoveAll("/tmp/tests-reliable-dbInitCleanup-truncKeysInOrder")

					// Open new database
					opts := nutsdbOptionsWithDir("/tmp/tests-reliable-dbInitCleanup-truncKeysInOrder")
					r, err := nutsdb.Open(opts)
					if err != nil {
						panic(err)
					}

					// Add new entry to keys in order that should be truncated
					id := newNoConnID()
					err = r.Update(func(tx *nutsdb.Tx) error {
						return tx.RPush(ctrlBucket, keysInOrderKey, []byte(id))
					})
					if err != nil {
						panic(err)
					}

					return r
				}(),
				appLogger:             &applogger.WriterAppLogger{Writer: os.Stdout, Level: applogger.DEBUG},
				consolidateDbNumFiles: 1,
			},
			false,
		},
		{
			"Missing in keys in order",
			fields{
				db: func() *nutsdb.DB {
					// Ensure previous execution data is cleaned
					os.RemoveAll("/tmp/tests-reliable-dbInitCleanup-missingKeysInOrder")

					// Using internal funcs of temporal reliable client with current db state for easy setup
					rClient, err := NewReliableClientBuilder().
						DbPath("/tmp/tests-reliable-dbInitCleanup-missingKeysInOrder").
						ClientBuilder(
							NewClientBuilder().EntryPoint("udp://localhost:13000")).
						// prevent consolidate db and resend events db during test
						ConsolidateDbDaemonInitDelay(time.Minute).
						RetryDaemonInitDelay(time.Minute).
						Build()
					if err != nil {
						panic(err)
					}

					// Stand by mode to prevent flush events
					err = rClient.StandBy()
					if err != nil {
						panic(err)
					}

					// Add raw record to simulate "no-conn" id
					err = rClient.newRecord(&reliableClientRecord{
						AsyncIDs:  []string{uuid.NewV4().String()},
						Msg:       "the msg",
						Tag:       "tag.2",
						Timestamp: time.Now().Add(time.Minute), // to prevent expiration
					})
					if err != nil {
						panic(err)
					}
					// We leave client opened to prevent record marked as no-close

					// Open new database
					opts := nutsdbOptionsWithDir("/tmp/tests-reliable-dbInitCleanup-missingKeysInOrder")
					r, err := nutsdb.Open(opts)
					if err != nil {
						panic(err)
					}

					// Truncate keysInOrder to force missing id
					err = r.Update(func(tx *nutsdb.Tx) error {
						err := tx.LTrim(ctrlBucket, keysInOrderKey, 0, 0)
						if err != nil {
							return err
						}
						_, err = tx.LPop(ctrlBucket, keysInOrderKey)
						return err
					})
					if err != nil {
						panic(err)
					}

					return r
				}(),
				// appLogger: &applogger.NoLogAppLogger{},
				appLogger:             &applogger.WriterAppLogger{Writer: os.Stdout, Level: applogger.DEBUG},
				consolidateDbNumFiles: 1,
			},
			false,
		},
		{
			"Consolidate db error",
			fields{
				db: func() *nutsdb.DB {
					// Ensure previous execution data is cleaned
					os.RemoveAll("/tmp/tests-reliable-dbInitCleanup-consolidateError")

					// Open database
					opts := nutsdbOptionsWithDir("/tmp/tests-reliable-dbInitCleanup-consolidateError")
					r, err := nutsdb.Open(opts)
					if err != nil {
						panic(err)
					}

					return r
				}(),
				// appLogger: &applogger.NoLogAppLogger{},
				appLogger:             &applogger.WriterAppLogger{Writer: os.Stdout, Level: applogger.DEBUG},
				consolidateDbNumFiles: 0,
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsrc := &ReliableClient{
				db:                    tt.fields.db,
				dbInitCleanedup:       tt.fields.dbInitCleanedup,
				appLogger:             tt.fields.appLogger,
				consolidateDbNumFiles: tt.fields.consolidateDbNumFiles,
			}
			if err := dsrc.dbInitCleanup(); (err != nil) != tt.wantErr {
				t.Errorf("ReliableClient.dbInitCleanup() error = %+v, wantErr %+v", err, tt.wantErr)
			}
		})
	}

	os.RemoveAll("/tmp/tests-reliable-dbInitCleanup")
	os.RemoveAll("/tmp/tests-reliable-dbInitCleanup-conndata")
	os.RemoveAll("/tmp/tests-reliable-dbInitCleanup-truncKeysInOrder")
	os.RemoveAll("/tmp/tests-reliable-dbInitCleanup-missingKeysInOrder")
	os.RemoveAll("/tmp/tests-reliable-dbInitCleanup-consolidateError")
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
		DbPath("/tmp/devosedner-tests-ReliableClient_clientReconnectionDaemon").
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
		DbPath("/tmp/devosedner-tests-ReliableClient_clientReconnectionDaemon_recreate_error").
		ClientBuilder(
			NewClientBuilder().
				EntryPoint("NO_VALID_PROTOCOL://localhost")).
		EnableStandByModeTimeout(time.Millisecond * 50).
		RetryDaemonInitDelay(time.Minute).         // Prevent retry daemon runs during tests
		ConsolidateDbDaemonInitDelay(time.Minute). // Prevent retry daemon runs during tests
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
	want := `ERROR Error While create new client in Reconnection daemon: Error when create ` +
		`new DevoSender (Clear): Error when parse entrypoint NO_VALID_PROTOCOL://localhost: ` +
		`parse "NO_VALID_PROTOCOL://localhost": first path segment in URL cannot contain colon` + "\n" +
		`ERROR Error While create new client in Reconnection daemon: Error when create ` +
		`new DevoSender (Clear): Error when parse entrypoint NO_VALID_PROTOCOL://localhost: ` +
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

func TestReliableClient_consolidateDbDaemon__consolidate_error(t *testing.T) {
	// applogger to check errors
	var buf bytes.Buffer
	al := &applogger.WriterAppLogger{
		Writer: &buf,
		Level:  applogger.INFO,
	}
	// Create client with appLogger
	rc := &ReliableClient{
		consolidateDaemon: reliableClientDaemon{
			daemonOpts: daemonOpts{
				waitBtwChecks: time.Millisecond * 50,
			},
		},
		appLogger: al,
	}

	// starts daemon execution
	rc.consolidateDbDaemon()

	// wait and stop daemon
	time.Sleep(time.Millisecond * 50)
	rc.consolidateDaemon.stop = true

	// Checks that message is expected
	got := buf.String()
	wantPrefix := "ERROR Error While consolidate status db in consolidateDbDaemon: Status db is nil\n"
	if !strings.HasPrefix(got, wantPrefix) {
		t.Errorf("ReliableClient.clientReconnectionDaemon() ConsolidateStatusDb logger msg = %v, wantPrefix %v", got, wantPrefix)
	}
}

func TestReliableClient_consolidateDbDaemon__recreateDb(t *testing.T) {
	// applogger to check errors
	var buf bytes.Buffer
	al := &applogger.WriterAppLogger{
		Writer: &buf,
		Level:  applogger.DEBUG,
	}
	// status db with events to create files
	dbOpts := nutsdb.DefaultOptions
	dbOpts.SegmentSize = 64
	data := make(map[string][]byte, 12)
	val := []byte("1234567890123456") // 16 bytes lente
	for i := 0; i < 12; i++ {
		data[fmt.Sprintf("%d", i)] = val
	}
	path, db := newDbWithOpts("test", data, dbOpts)
	dbOpts.Dir = path

	// Crate client with appLogger
	rc := &ReliableClient{
		consolidateDaemon: reliableClientDaemon{
			daemonOpts: daemonOpts{
				waitBtwChecks: time.Millisecond * 100,
			},
		},
		appLogger:             al,
		db:                    db,
		dbOpts:                dbOpts,
		consolidateDbNumFiles: 2,
	}

	// starts daemon execution
	rc.consolidateDbDaemon()

	// wait for consolidation call stop daemon
	time.Sleep(time.Millisecond * 200)

	// Checks that message is expected
	got := buf.String()
	wantPrefix := "DEBUG consolidateDbDaemon working: { waitBtwChecks: 100ms, initDelay: 0s, stop: false}\n" +
		"DEBUG consolidateDbDaemon shot: { waitBtwChecks: 100ms, initDelay: 0s, stop: false}\n" +
		"INFO Starting status db files consolidation\n" +
		"DEBUG Recreating db as nutsdb memory leak Work-Arround in consolidateDbDaemon"
	if !strings.HasPrefix(got, wantPrefix) {
		t.Errorf("ReliableClient.clientReconnectionDaemon() logger msg = %v, wantPrefix %v", got, wantPrefix)
	}

	os.RemoveAll(path)
}

func TestReliableClient_consolidateDbDaemon(t *testing.T) {
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
				consolidateDaemon: reliableClientDaemon{
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
			if err := tt.dsrc.consolidateDbDaemon(); (err != nil) != tt.wantErr {
				t.Errorf("ReliableClient.consolidateDbDaemon() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestReliableClient_resendRecord(t *testing.T) {
	type args struct {
		r *reliableClientRecord
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
					DbPath("/tmp/tests-reliable-resendRecord").
					ClientBuilder(
						NewClientBuilder().EntryPoint("upd://localhost:13000")).
					Build()
				if err != nil {
					panic(err)
				}

				// Create record
				r.newRecord(
					&reliableClientRecord{
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
				&reliableClientRecord{
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
					DbPath("/tmp/tests-reliable-resendRecord-client-nil").
					ClientBuilder(
						NewClientBuilder().EntryPoint("udp://localhost:13000")).
					Build()
				if err != nil {
					panic(err)
				}

				// Create record
				r.newRecord(
					&reliableClientRecord{
						AsyncIDs: []string{nonConnIDPrefix + "11111"},
					},
				)

				// Force client to nil
				r.Client.Close()
				r.Client = nil
				return r
			}(),
			args{
				&reliableClientRecord{
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
					DbPath("/tmp/tests-reliable-resendRecord-usual-record").
					ClientBuilder(
						NewClientBuilder().EntryPoint("udp://localhost:13000")).
					Build()
				if err != nil {
					panic(err)
				}

				// Create record
				r.newRecord(
					&reliableClientRecord{
						AsyncIDs: []string{"11111"},
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
				&reliableClientRecord{
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
					DbPath("/tmp/tests-reliable-resendRecord-error-update").
					ClientBuilder(
						NewClientBuilder().EntryPoint("udp://localhost:13000")).
					Build()
				if err != nil {
					panic(err)
				}

				// Create record
				r.newRecord(
					&reliableClientRecord{
						AsyncIDs: []string{"11111"},
					},
				)

				// Close status database to force error
				r.db.Close()

				return r
			}(),
			args{
				&reliableClientRecord{
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
					DbPath("/tmp/tests-reliable-resendRecord-compressor").
					ClientBuilder(
						NewClientBuilder().EntryPoint("udp://localhost:13000")).
					Build()
				if err != nil {
					panic(err)
				}

				// Create record
				r.newRecord(
					&reliableClientRecord{
						AsyncIDs:   []string{"11111"},
						Compressor: &compressor.Compressor{Algorithm: compressor.CompressorGzip},
						Tag:        "test.keep.free",
						Msg:        "The message",
					},
				)
				return r
			}(),
			args{
				&reliableClientRecord{
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

func TestReliableClient_newRecord(t *testing.T) {
	type args struct {
		r *reliableClientRecord
	}
	tests := []struct {
		name           string
		reliableClient *ReliableClient
		args           args
		wantErr        bool
	}{
		{
			"Error updating new counter",
			func() *ReliableClient {
				os.RemoveAll("/tmp/tests-reliable-newRecord")

				// Create new counter with an invalid type to force error
				opts := nutsdbOptionsWithDir("/tmp/tests-reliable-newRecord")
				db, err := nutsdb.Open(opts)

				err = db.Update(func(tx *nutsdb.Tx) error {
					return tx.Put(statsBucket, countKey, []byte("tarari"), 0)
				})
				if err != nil {
					panic(err)
				}

				err = db.Close()
				if err != nil {
					panic(err)
				}

				r, _ := NewReliableClientBuilder().
					DbPath("/tmp/tests-reliable-newRecord").
					ClientBuilder(
						NewClientBuilder().EntryPoint("udp://localhost:13000")).
					Build()

				return r
			}(),
			args{
				&reliableClientRecord{
					AsyncIDs: []string{"11111"},
				},
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.reliableClient.newRecord(tt.args.r); (err != nil) != tt.wantErr {
				t.Errorf("ReliableClient.newRecord() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	os.RemoveAll("/tmp/tests-reliable-newRecord")
}

func Test_updateRecordInTx(t *testing.T) {
	type args struct {
		tx    *nutsdb.Tx
		r     *reliableClientRecord
		newID string
		ttl   uint32
	}
	tests := []struct {
		name              string
		args              args
		txActionAfterTest string
		wantErr           bool
	}{
		{
			"Error keysInOrder not found",
			args{
				tx: func() *nutsdb.Tx {
					os.RemoveAll("/tmp/tests-reliable-updateRecordInTx")

					opts := nutsdbOptionsWithDir("/tmp/tests-reliable-updateRecordInTx")
					db, err := nutsdb.Open(opts)
					if err != nil {
						panic(err)
					}
					tx, err := db.Begin(true)
					if err != nil {
						panic(err)
					}
					return tx
				}(),
				r: &reliableClientRecord{
					AsyncIDs:  []string{"1111111"},
					Timestamp: time.Now(),
				},
				newID: "2222222222",
				ttl:   3600,
			},
			"rollback",
			true,
		},
		{
			"Error tx closed",
			args{
				tx: func() *nutsdb.Tx {
					os.RemoveAll("/tmp/tests-reliable-updateRecordInTx-tx-closed")

					opts := nutsdbOptionsWithDir("/tmp/tests-reliable-updateRecordInTx-tx-closed")
					db, err := nutsdb.Open(opts)
					if err != nil {
						panic(err)
					}
					tx, err := db.Begin(true)
					if err != nil {
						panic(err)
					}
					tx.Commit()
					return tx
				}(),
				r: &reliableClientRecord{
					AsyncIDs:  []string{"1111111"},
					Timestamp: time.Now(),
				},
				newID: "2222222222",
				ttl:   3600,
			},
			"",
			true,
		},
		{
			"Error record not found",
			args{
				tx: func() *nutsdb.Tx {
					os.RemoveAll("/tmp/tests-reliable-updateRecordInTx-recordNotFound")

					opts := nutsdbOptionsWithDir("/tmp/tests-reliable-updateRecordInTx-recordNotFound")
					db, err := nutsdb.Open(opts)
					if err != nil {
						panic(err)
					}

					// Add necessary fields
					err = db.Update(func(tx *nutsdb.Tx) error {
						// Create empty id list
						return tx.RPush(ctrlBucket, keysInOrderKey, []byte("33333"))
					})
					if err != nil {
						panic(err)
					}

					tx, err := db.Begin(true)
					if err != nil {
						panic(err)
					}
					return tx
				}(),
				r: &reliableClientRecord{
					AsyncIDs:  []string{"1111111"},
					Timestamp: time.Now(),
				},
				newID: "2222222222",
				ttl:   3600,
			},
			"rollback",
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := updateRecordInTx(tt.args.tx, tt.args.r, tt.args.newID, tt.args.ttl); (err != nil) != tt.wantErr {
				t.Errorf("updateRecordInTx() error = %v, wantErr %v", err, tt.wantErr)
			}

			var err error
			switch tt.txActionAfterTest {
			case "rollback":
				err = tt.args.tx.Rollback()
			case "commit":
				err = tt.args.tx.Commit()
			}
			if err != nil {
				panic(err)
			}
		})
	}

	os.RemoveAll("/tmp/tests-reliable-updateRecordInTx")
	os.RemoveAll("/tmp/tests-reliable-updateRecordInTx-tx-closed")
	os.RemoveAll("/tmp/tests-reliable-updateRecordInTx-recordNotFound")
}

func Test_deleteRecordRawInTx(t *testing.T) {
	type args struct {
		tx        *nutsdb.Tx
		idAsBytes []byte
	}
	tests := []struct {
		name              string
		args              args
		txActionAfterTest string
		wantErr           bool
	}{
		{
			"Error tx closed",
			args{
				tx: func() *nutsdb.Tx {
					os.RemoveAll("/tmp/tests-reliable-deleteRecordRawInTx")

					opts := nutsdbOptionsWithDir("/tmp/tests-reliable-deleteRecordRawInTx")
					db, err := nutsdb.Open(opts)
					if err != nil {
						panic(err)
					}
					tx, err := db.Begin(true)
					if err != nil {
						panic(err)
					}
					tx.Commit()
					return tx
				}(),
			},
			"",
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := deleteRecordRawInTx(tt.args.tx, tt.args.idAsBytes); (err != nil) != tt.wantErr {
				t.Errorf("deleteRecordRawInTx() error = %v, wantErr %v", err, tt.wantErr)
			}

			var err error
			switch tt.txActionAfterTest {
			case "rollback":
				err = tt.args.tx.Rollback()
			case "commit":
				err = tt.args.tx.Commit()
			}
			if err != nil {
				panic(err)
			}
		})
	}

	os.RemoveAll("/tmp/tests-reliable-deleteRecordRawInTx")
}

func TestReliableClient_dropRecords(t *testing.T) {
	type args struct {
		n int
	}
	tests := []struct {
		name           string
		reliableClient *ReliableClient
		args           args
		wantErr        bool
	}{
		{
			"Error while call dropRecordsInTx",
			func() *ReliableClient {
				os.RemoveAll("/tmp/tests-reliable-dropRecords")

				r, err := NewReliableClientBuilder().
					DbPath("/tmp/tests-reliable-dropRecords").
					ClientBuilder(
						NewClientBuilder().EntryPoint("udp://localhost:13000")).
					Build()
				if err != nil {
					panic(err)
				}

				// Close db to force error
				r.db.Close()

				return r
			}(),
			args{},
			true,
		},
		{
			"0 records",
			func() *ReliableClient {
				os.RemoveAll("/tmp/tests-reliable-dropRecords-0records")

				r, err := NewReliableClientBuilder().
					DbPath("/tmp/tests-reliable-dropRecords-0records").
					ClientBuilder(
						NewClientBuilder().EntryPoint("udp://localhost:13000")).
					Build()
				if err != nil {
					panic(err)
				}
				return r
			}(),
			args{},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.reliableClient.dropRecords(tt.args.n); (err != nil) != tt.wantErr {
				t.Errorf("ReliableClient.dropRecords() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	os.RemoveAll("/tmp/tests-reliable-dropRecords")
	os.RemoveAll("/tmp/tests-reliable-dropRecords-0records")
}

func Test_dropRecordsInTx(t *testing.T) {
	type args struct {
		n int
	}
	tests := []struct {
		name            string
		existingRecords []*reliableClientRecord
		commitBeforeTx  bool // to force tx error
		args            args
		wantExistingIds []string
		wantErr         bool
	}{
		{
			"Empty",
			make([]*reliableClientRecord, 0),
			false,
			args{16},
			make([]string, 0),
			false,
		},
		{
			"Drop < size",
			[]*reliableClientRecord{
				{
					AsyncIDs:  []string{"ID-1"},
					Timestamp: time.Now().Add(time.Second * 3),
				},
				{
					AsyncIDs:  []string{"ID-2"},
					Timestamp: time.Now().Add(time.Second * 2),
				},
				{
					AsyncIDs:  []string{"ID-3"},
					Timestamp: time.Now().Add(time.Second),
				},
			},
			false,
			args{2},
			[]string{
				"ID-3",
			},
			false,
		},
		{
			"Drop == size",
			[]*reliableClientRecord{
				{
					AsyncIDs:  []string{"ID-1"},
					Timestamp: time.Now().Add(time.Second * 3),
				},
				{
					AsyncIDs:  []string{"ID-2"},
					Timestamp: time.Now().Add(time.Second * 2),
				},
				{
					AsyncIDs:  []string{"ID-3"},
					Timestamp: time.Now().Add(time.Second),
				},
			},
			false,
			args{3},
			[]string{},
			false,
		},
		{
			"Drop > size",
			[]*reliableClientRecord{
				{
					AsyncIDs:  []string{"ID-1"},
					Timestamp: time.Now().Add(time.Second * 3),
				},
				{
					AsyncIDs:  []string{"ID-2"},
					Timestamp: time.Now().Add(time.Second * 2),
				},
				{
					AsyncIDs:  []string{"ID-3"},
					Timestamp: time.Now().Add(time.Second),
				},
			},
			false,
			args{16},
			[]string{},
			false,
		},
		{
			"Error in tx",
			make([]*reliableClientRecord, 0),
			true,
			args{16},
			[]string{},
			true,
		},
	}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			path, db := newDb(ctrlBucket, make(map[string][]byte, 0))

			dsrc := &ReliableClient{
				db:         db,
				bufferSize: uint(len(tt.existingRecords)), // We need to ensure size to maintain exisitng records
			}

			if len(tt.existingRecords) > 0 {
				for _, record := range tt.existingRecords {
					err := dsrc.newRecord(record)

					if err != nil {
						panic(fmt.Errorf("Error when warm up records: %w", err))
					}
				}
			}

			err := db.Update(func(tx *nutsdb.Tx) error {
				if tt.commitBeforeTx {
					tx.Commit()
				}
				return dropRecordsInTx(tx, tt.args.n)
			})

			if (err != nil) != tt.wantErr {
				t.Errorf("dropRecordsInTx() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			got, err := dsrc.findAllRecordsID()
			if !reflect.DeepEqual(got, tt.wantExistingIds) {
				t.Errorf("dropRecordsInTx() n=%d remainingRecordIds = %v, want %v", tt.args.n, got, tt.wantExistingIds)
			}

			destroyDb(path, db)
		})
	}
}

func Test_findAllRecordsID(t *testing.T) {
	tests := []struct {
		name         string
		existingKeys map[string][]byte
		existingSets map[string][][]byte
		closedDb     bool
		want         []string
		wantErr      bool
	}{
		{
			"Empty",
			make(map[string][]byte, 0),
			make(map[string][][]byte, 0),
			false,
			make([]string, 0),
			false,
		},
		{
			"Key value format invalid",
			map[string][]byte{
				string(keysKey): []byte("should not be valid"),
			},
			make(map[string][][]byte, 0),
			false,
			make([]string, 0),
			false,
		},
		{
			"Get values",
			make(map[string][]byte, 0),
			map[string][][]byte{
				string(keysKey): {
					[]byte("ID-1"),
					[]byte("ID-2"),
				},
			},
			false,
			[]string{
				"ID-1",
				"ID-2",
			},
			false,
		},
		{
			"Error DB closed",
			make(map[string][]byte, 0),
			make(map[string][][]byte, 0),
			true,
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, db := newDb(ctrlBucket, tt.existingKeys)

			if len(tt.existingSets) > 0 {
				db.Update(func(tx *nutsdb.Tx) error {

					for k, vs := range tt.existingSets {
						for _, v := range vs {
							err := tx.SAdd(ctrlBucket, []byte(k), v)
							if err != nil {
								panic(fmt.Errorf("Error when warm up sets: %w", err))
							}
						}
					}

					return nil
				})
			}

			if tt.closedDb {
				db.Close()
			}

			dsrc := &ReliableClient{
				db: db,
			}
			got, err := dsrc.findAllRecordsID()

			if (err != nil) != tt.wantErr {
				t.Errorf("findAllRecordsIDRaw() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// order is not important we transform to map to prevent order
			gotMap := map[string]interface{}{}
			wantMap := map[string]interface{}{}
			for _, s := range got {
				gotMap[s] = nil
			}
			for _, s := range tt.want {
				wantMap[s] = nil
			}
			if !reflect.DeepEqual(gotMap, wantMap) {
				t.Errorf("findAllRecordsIDRaw() = %v, want %v", got, tt.want)
			}

			destroyDb(path, db)
		})
	}
}

func Test_findAllRecordsIDRaw(t *testing.T) {
	tests := []struct {
		name         string
		existingKeys map[string][]byte
		existingSets map[string][][]byte
		closedDb     bool
		want         [][]byte
		wantErr      bool
	}{
		{
			"Empty",
			make(map[string][]byte, 0),
			make(map[string][][]byte, 0),
			false,
			nil,
			false,
		},
		{
			"Key value format invalid",
			map[string][]byte{
				string(keysKey): []byte("should not be valid"),
			},
			make(map[string][][]byte, 0),
			false,
			nil,
			false,
		},
		{
			"Get values",
			make(map[string][]byte, 0),
			map[string][][]byte{
				string(keysKey): {
					[]byte("ID-1"),
					[]byte("ID-2"),
				},
			},
			false,
			[][]byte{
				[]byte("ID-1"),
				[]byte("ID-2"),
			},
			false,
		},
		{
			"Error DB closed",
			make(map[string][]byte, 0),
			make(map[string][][]byte, 0),
			true,
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, db := newDb(ctrlBucket, tt.existingKeys)

			if len(tt.existingSets) > 0 {
				db.Update(func(tx *nutsdb.Tx) error {

					for k, vs := range tt.existingSets {
						for _, v := range vs {
							err := tx.SAdd(ctrlBucket, []byte(k), v)
							if err != nil {
								panic(fmt.Errorf("Error when warm up sets: %w", err))
							}
						}
					}

					return nil
				})
			}

			if tt.closedDb {
				db.Close()
			}

			dsrc := &ReliableClient{
				db: db,
			}
			got, err := dsrc.findAllRecordsIDRaw()

			if (err != nil) != tt.wantErr {
				t.Errorf("findAllRecordsIDRaw() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// order is not important we transform to map to prevent order
			gotMap := map[string]interface{}{}
			wantMap := map[string]interface{}{}
			for _, s := range got {
				gotMap[string(s)] = nil
			}
			for _, s := range tt.want {
				wantMap[string(s)] = nil
			}
			if !reflect.DeepEqual(gotMap, wantMap) {
				t.Errorf("findAllRecordsIDRaw() = %v, want %v", got, tt.want)
			}

			destroyDb(path, db)
		})
	}
}

func Test_findAllRecordsIDRawInTx(t *testing.T) {
	tests := []struct {
		name         string
		existingKeys map[string][]byte
		existingSets map[string][][]byte
		closedDb     bool
		want         [][]byte
		wantErr      bool
	}{
		{
			"Empty",
			make(map[string][]byte, 0),
			make(map[string][][]byte, 0),
			false,
			nil,
			false,
		},
		{
			"Key value format invalid",
			map[string][]byte{
				string(keysKey): []byte("should not be valid"),
			},
			make(map[string][][]byte, 0),
			false,
			nil,
			false,
		},
		{
			"Get values",
			make(map[string][]byte, 0),
			map[string][][]byte{
				string(keysKey): {
					[]byte("ID-1"),
					[]byte("ID-2"),
				},
			},
			false,
			[][]byte{
				[]byte("ID-1"),
				[]byte("ID-2"),
			},
			false,
		},
		{
			"Error DB closed",
			make(map[string][]byte, 0),
			make(map[string][][]byte, 0),
			true,
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, db := newDb(ctrlBucket, tt.existingKeys)

			if len(tt.existingSets) > 0 {
				db.Update(func(tx *nutsdb.Tx) error {

					for k, vs := range tt.existingSets {
						for _, v := range vs {
							err := tx.SAdd(ctrlBucket, []byte(k), v)
							if err != nil {
								panic(fmt.Errorf("Error when warm up sets: %w", err))
							}
						}
					}

					return nil
				})
			}

			if tt.closedDb {
				db.Close()
			}

			var got [][]byte
			err := db.View(func(tx *nutsdb.Tx) error {
				var err error
				got, err = findAllRecordsIDRawInTx(tx)
				return err
			})

			if (err != nil) != tt.wantErr {
				t.Errorf("findAllRecordsIDRawInTx() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// order is not important we transform to map to prevent order
			gotMap := map[string]interface{}{}
			wantMap := map[string]interface{}{}
			for _, s := range got {
				gotMap[string(s)] = nil
			}
			for _, s := range tt.want {
				wantMap[string(s)] = nil
			}

			if !reflect.DeepEqual(gotMap, wantMap) {
				t.Errorf("findAllRecordsIDRawInTx() = %v, want %v", got, tt.want)
			}

			destroyDb(path, db)
		})
	}
}

func TestReliableClient_getRecord(t *testing.T) {
	type args struct {
		id string
	}
	tests := []struct {
		name         string
		existingKeys map[string][]byte
		closedDb     bool
		args         args
		want         *reliableClientRecord
		wantErr      bool
	}{
		{
			"ID does not exist",
			make(map[string][]byte, 0),
			false,
			args{"id-1"},
			nil,
			false,
		},
		{
			"ID find",
			map[string][]byte{
				"id-1": func() []byte {
					rd := &reliableClientRecord{
						AsyncIDs:  []string{"id-1"},
						Msg:       "the message",
						Tag:       "the tag",
						Timestamp: time.Unix(0, 0),
					}
					r, err := rd.Serialize()
					if err != nil {
						panic(err)
					}
					return r
				}(),
			},
			false,
			args{"id-1"},
			&reliableClientRecord{
				AsyncIDs:  []string{"id-1"},
				Msg:       "the message",
				Tag:       "the tag",
				Timestamp: time.Unix(0, 0),
			},
			false,
		},
		{
			"DB error",
			make(map[string][]byte, 0),
			true,
			args{"id-1"},
			nil,
			true,
		},
	}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			path, db := newDb(dataBucket, tt.existingKeys)

			if tt.closedDb {
				db.Close()
			}
			dsrc := &ReliableClient{
				db: db,
			}
			got, err := dsrc.getRecord(tt.args.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReliableClient.getRecordRaw() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReliableClient.getRecordRaw() = %v, want %v", got, tt.want)
			}

			destroyDb(path, db)
		})

	}
}

func TestReliableClient_getRecordRaw(t *testing.T) {
	type args struct {
		idAsBytes []byte
	}
	tests := []struct {
		name         string
		existingKeys map[string][]byte
		closedDb     bool
		args         args
		want         *reliableClientRecord
		wantErr      bool
	}{
		{
			"ID does not exist",
			make(map[string][]byte, 0),
			false,
			args{[]byte("id-1")},
			nil,
			false,
		},
		{
			"ID find",
			map[string][]byte{
				"id-1": func() []byte {
					rd := &reliableClientRecord{
						AsyncIDs:  []string{"id-1"},
						Msg:       "the message",
						Tag:       "the tag",
						Timestamp: time.Unix(0, 0),
					}
					r, err := rd.Serialize()
					if err != nil {
						panic(err)
					}
					return r
				}(),
			},
			false,
			args{[]byte("id-1")},
			&reliableClientRecord{
				AsyncIDs:  []string{"id-1"},
				Msg:       "the message",
				Tag:       "the tag",
				Timestamp: time.Unix(0, 0),
			},
			false,
		},
		{
			"DB error",
			make(map[string][]byte, 0),
			true,
			args{[]byte("id-1")},
			nil,
			true,
		},
	}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			path, db := newDb(dataBucket, tt.existingKeys)

			if tt.closedDb {
				db.Close()
			}
			dsrc := &ReliableClient{
				db: db,
			}
			got, err := dsrc.getRecordRaw(tt.args.idAsBytes)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReliableClient.getRecordRaw() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReliableClient.getRecordRaw() = %v, want %v", got, tt.want)
			}

			destroyDb(path, db)
		})

	}
}

func Test_getRecordRawInTx(t *testing.T) {
	type args struct {
		key []byte
	}
	tests := []struct {
		name         string
		existingKeys map[string][]byte
		closedTx     bool
		args         args
		want         *reliableClientRecord
		wantErr      bool
	}{
		{
			"ID does not exist",
			make(map[string][]byte, 0),
			false,
			args{
				[]byte("id-1"),
			},
			nil,
			false,
		},
		{
			"ID exists",
			map[string][]byte{
				"id-1": func() []byte {
					rd := &reliableClientRecord{
						AsyncIDs:  []string{"id-1"},
						Msg:       "the message",
						Tag:       "the tag",
						Timestamp: time.Unix(0, 0),
					}
					r, err := rd.Serialize()
					if err != nil {
						panic(err)
					}
					return r
				}(),
			},
			false,
			args{
				[]byte("id-1"),
			},
			&reliableClientRecord{
				AsyncIDs:  []string{"id-1"},
				Msg:       "the message",
				Tag:       "the tag",
				Timestamp: time.Unix(0, 0),
			},
			false,
		},
		{
			"Unmarshal error",
			map[string][]byte{
				"id-fail": []byte("tarari que te vi"),
			},
			false,
			args{
				[]byte("id-fail"),
			},
			nil,
			true,
		},
		{
			"DB Error",
			make(map[string][]byte, 0),
			true,
			args{
				[]byte("id-ok"),
			},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		path, db := newDb(dataBucket, tt.existingKeys)

		t.Run(tt.name, func(t *testing.T) {
			err := db.View(func(tx *nutsdb.Tx) error {
				if tt.closedTx {
					tx.Commit()
				}

				got, err := getRecordRawInTx(tx, tt.args.key)
				if (err != nil) != tt.wantErr {
					t.Errorf("getRecordRawInTx() error = %v, wantErr %v", err, tt.wantErr)
				}

				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("getRecordRawInTx() got = %+v, want %+v", got, tt.want)
					return errors.New("Test failed")
				}
				return nil
			})

			if err != nil {
				return
			}
		})

		destroyDb(path, db)
	}
}

func Test_dec(t *testing.T) {
	type args struct {
		bucket          string
		key             []byte
		v               int
		errorIfNotFound bool
	}
	tests := []struct {
		name         string
		existingKeys map[string][]byte
		args         args
		want         string
		wantErr      bool
	}{
		{
			"Key does not exist ignored",
			make(map[string][]byte, 0),
			args{
				"test",
				[]byte("new_key"),
				1,
				false,
			},
			"-1",
			false,
		},
		{
			"Key does not exist error",
			make(map[string][]byte, 0),
			args{
				"test",
				[]byte("new_key"),
				22,
				true,
			},
			"0",
			true,
		},
		{
			"Key exists",
			map[string][]byte{
				"test-key": []byte("22"),
			},
			args{
				"test",
				[]byte("test-key"),
				2,
				true,
			},
			"20",
			false,
		},
		{
			"Key exists other format",
			map[string][]byte{
				"test-key": []byte("tarari que te vi"),
			},
			args{
				"test",
				[]byte("test-key"),
				12,
				true,
			},
			"-12",
			false,
		},
		{
			"dec 0 key does not exists",
			make(map[string][]byte, 0),
			args{
				"test",
				[]byte("new_key"),
				0,
				true,
			},
			"0",
			false,
		},
		{
			"dec 0 other format",
			map[string][]byte{
				"test-key": []byte("tarari"),
			},
			args{
				"test",
				[]byte("new_key"),
				0,
				true,
			},
			"tarari",
			false,
		},
	}
	for _, tt := range tests {
		path, db := newDb(tt.args.bucket, tt.existingKeys)

		t.Run(tt.name, func(t *testing.T) {
			db.Update(func(tx *nutsdb.Tx) error {
				err := dec(tx, tt.args.bucket, tt.args.key, tt.args.v, tt.args.errorIfNotFound)
				if (err != nil) != tt.wantErr {
					t.Errorf("dec() error = %v, wantErr %v", err, tt.wantErr)
				}
				return nil
			})

			if !tt.args.errorIfNotFound {
				expectedValue := []byte(tt.want)
				if !assertKeyVal(db, tt.args.bucket, tt.args.key, expectedValue) {
					t.Errorf("dec() for key %s want value %s", string(tt.args.key), tt.want)
				}
			}
		})

		destroyDb(path, db)
	}
}

func Test_inc(t *testing.T) {
	type args struct {
		bucket          string
		key             []byte
		v               int
		errorIfNotFound bool
	}
	tests := []struct {
		name         string
		existingKeys map[string][]byte
		args         args
		want         string
		wantErr      bool
	}{
		{
			"Key does not exist ignored",
			make(map[string][]byte, 0),
			args{
				"test",
				[]byte("new_key"),
				1,
				false,
			},
			"1",
			false,
		},
		{
			"Key does not exist error",
			make(map[string][]byte, 0),
			args{
				"test",
				[]byte("new_key"),
				22,
				true,
			},
			"0",
			true,
		},
		{
			"Key exists",
			map[string][]byte{
				"test-key": []byte("22"),
			},
			args{
				"test",
				[]byte("test-key"),
				2,
				true,
			},
			"24",
			false,
		},
		{
			"Key exists other format",
			map[string][]byte{
				"test-key": []byte("tarari que te vi"),
			},
			args{
				"test",
				[]byte("test-key"),
				12,
				true,
			},
			"12",
			false,
		},
		{
			"inc 0 key does not exists",
			make(map[string][]byte, 0),
			args{
				"test",
				[]byte("new_key"),
				0,
				true,
			},
			"0",
			false,
		},
		{
			"inc 0 other format",
			map[string][]byte{
				"test-key": []byte("tarari"),
			},
			args{
				"test",
				[]byte("new_key"),
				0,
				true,
			},
			"tarari",
			false,
		},
	}
	for _, tt := range tests {
		path, db := newDb(tt.args.bucket, tt.existingKeys)

		t.Run(tt.name, func(t *testing.T) {
			db.Update(func(tx *nutsdb.Tx) error {
				err := inc(tx, tt.args.bucket, tt.args.key, tt.args.v, tt.args.errorIfNotFound)
				if (err != nil) != tt.wantErr {
					t.Errorf("inc() error = %v, wantErr %v", err, tt.wantErr)
				}
				return nil
			})

			if !tt.args.errorIfNotFound {
				expectedValue := []byte(tt.want)
				if !assertKeyVal(db, tt.args.bucket, tt.args.key, expectedValue) {
					t.Errorf("inc() for key %s want value %s", string(tt.args.key), tt.want)
				}
			}
		})

		destroyDb(path, db)
	}
}

func Test_cont(t *testing.T) {
	type args struct {
		bucket          string
		key             []byte
		errorIfNotFound bool
	}
	tests := []struct {
		name         string
		existingKeys map[string][]byte
		closedTx     bool
		args         args
		want         int
		wantErr      bool
	}{
		{
			"Key does not exist ignored",
			make(map[string][]byte, 0),
			false,
			args{
				"test",
				[]byte("new_key"),
				false,
			},
			0,
			false,
		},
		{
			"Key does not exist error",
			make(map[string][]byte, 0),
			false,
			args{
				"test",
				[]byte("new_key"),
				true,
			},
			0,
			true,
		},
		{
			"Key exists",
			map[string][]byte{
				"test-key": []byte("22"),
			},
			false,
			args{
				"test",
				[]byte("test-key"),
				true,
			},
			22,
			false,
		},
		{
			"Key exists other format",
			map[string][]byte{
				"test-key": []byte("tarari que te vi"),
			},
			false,
			args{
				"test",
				[]byte("test-key"),
				true,
			},
			0,
			true,
		},
		{
			"DB error",
			make(map[string][]byte, 0),
			true,
			args{
				"test",
				[]byte("test-key"),
				true,
			},
			0,
			true,
		},
	}
	for _, tt := range tests {
		path, db := newDb(tt.args.bucket, tt.existingKeys)

		t.Run(tt.name, func(t *testing.T) {
			err := db.View(func(tx *nutsdb.Tx) error {
				if tt.closedTx {
					tx.Commit()
				}

				got, err := cont(tx, tt.args.bucket, tt.args.key, tt.args.errorIfNotFound)
				if (err != nil) != tt.wantErr {
					t.Errorf("cont() error = %v, wantErr %v", err, tt.wantErr)
				}

				if got != tt.want {
					t.Errorf("cont() got = %v, want %v", got, tt.want)
					return errors.New("Test failed")
				}
				return nil
			})

			if err != nil {
				return
			}
		})

		destroyDb(path, db)
	}
}

func Test_set(t *testing.T) {
	type args struct {
		bucket string
		key    []byte
		v      int
	}
	tests := []struct {
		name         string
		existingKeys map[string][]byte
		args         args
		wantErr      bool
	}{
		{
			"Key does not exist",
			make(map[string][]byte, 0),
			args{
				"test",
				[]byte("new_key"),
				22,
			},
			false,
		},
		{
			"Key exists",
			map[string][]byte{
				"test-key": []byte("22"),
			},
			args{
				"test",
				[]byte("test-key"),
				12,
			},
			false,
		},
		{
			"Key exists other format",
			map[string][]byte{
				"test-key": []byte("tarari que te vi"),
			},
			args{
				"test",
				[]byte("test-key"),
				12,
			},
			false,
		},
	}
	for _, tt := range tests {
		path, db := newDb(tt.args.bucket, tt.existingKeys)

		t.Run(tt.name, func(t *testing.T) {
			db.Update(func(tx *nutsdb.Tx) error {
				if err := set(tx, tt.args.bucket, tt.args.key, tt.args.v); (err != nil) != tt.wantErr {
					t.Errorf("set() error = %v, wantErr %v", err, tt.wantErr)
				}
				return nil
			})

			expectedValue := []byte(strconv.Itoa(tt.args.v))
			if !assertKeyVal(db, tt.args.bucket, tt.args.key, expectedValue) {
				t.Errorf("set() for key %s want value %d", string(tt.args.key), tt.args.v)
			}
		})

		destroyDb(path, db)
	}
}

func Test_del(t *testing.T) {

	type args struct {
		bucket       string
		key          []byte
		existingKeys map[string][]byte
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"Key does not exist",
			args{
				"test",
				[]byte("Does not exists"),
				make(map[string][]byte, 0),
			},
			false,
		},
		{
			"Key exists",
			args{
				"test",
				[]byte("test-key"),
				map[string][]byte{
					"test-key": []byte("test-value"),
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		path, db := newDb(tt.args.bucket, tt.args.existingKeys)

		t.Run(tt.name, func(t *testing.T) {
			db.Update(func(tx *nutsdb.Tx) error {
				if err := del(tx, tt.args.bucket, tt.args.key); (err != nil) != tt.wantErr {
					t.Errorf("del() error = %v, wantErr %v", err, tt.wantErr)
				}
				return nil
			})

			if existKey(db, tt.args.bucket, tt.args.key) {
				t.Errorf("del() key %s exists in db", string(tt.args.key))
			}
		})

		destroyDb(path, db)
	}
}

func Test_nutsdbIsNotFoundError(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			"Nil error",
			args{},
			false,
		},
		{
			"ErrBucketNotFound",
			args{nutsdb.ErrBucketNotFound},
			true,
		},
		{
			"ErrBucketEmpty",
			args{nutsdb.ErrBucketEmpty},
			true,
		},
		{
			"ErrNotFoundKey",
			args{nutsdb.ErrNotFoundKey},
			true,
		},
		{
			"ErrKeyNotFound",
			args{nutsdb.ErrKeyNotFound},
			true,
		},
		{
			"key not exits",
			args{errors.New("key not exits")},
			true,
		},
		{
			"item not exits",
			args{errors.New("item not exits")},
			true,
		},
		{
			"not found bucket:FOO,key:BAR",
			args{errors.New("not found bucket:FOOfoo,key:BARbarBAR")},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nutsdbIsNotFoundError(tt.args.err); got != tt.want {
				t.Errorf("nutsdbIsNotFoundError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_numberOfFiles(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			"No existing path",
			args{"/this_path_should_not_exist"},
			0,
		},
		{
			"Empty path",
			args{
				func() string {
					// Create path
					r := "/tmp/test-devosender_reliable-numberOfFiles"
					os.RemoveAll(r)
					err := os.Mkdir(r, 0600)
					if err != nil {
						panic(err)
					}
					return r
				}(),
			},
			0,
		},
		{
			"Path with file",
			args{
				func() string {
					// Create path
					r := "/tmp/test-devosender_reliable-numberOfFiles-files"
					os.RemoveAll(r)
					err := os.Mkdir(r, 0700)
					if err != nil {
						panic(err)
					}

					// Create directory and file
					err = os.Mkdir(r+"/otherdir", 0600)
					if err != nil {
						panic(err)
					}

					f, err := os.Create(r + "/file.txt")
					if err != nil {
						panic(err)
					}
					f.Close()

					return r
				}(),
			},
			2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := numberOfFiles(tt.args.path); got != tt.want {
				t.Errorf("numberOfFiles() = %v, want %v", got, tt.want)
			}
		})
	}

	os.RemoveAll("/tmp/test-devosender_reliable-numberOfFiles")
	os.RemoveAll("/tmp/test-devosender_reliable-numberOfFiles-files")
}

func TestIsOldIDNotFoundErr(t *testing.T) {
	type args struct {
		e error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			"Nil error",
			args{nil},
			false,
		},
		{
			"No match error",
			args{errors.New("Other error")},
			false,
		},
		{
			"Match error",
			args{errors.New("Old id 1jdwir-9820832-dslljfd-10920834 did not find in tarari_bucket.tarari_Key")},
			true,
		},
		{
			"Unsupported key name",
			args{errors.New("Old id 1jdwir-9820832-dslljfd-10920834 did not find in tarari_bucket.tarari-Key")},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsOldIDNotFoundErr(tt.args.e); got != tt.want {
				t.Errorf("IsOldIDNotFoundErr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func newDb(initValBucket string, initVals map[string][]byte) (string, *nutsdb.DB) {
	return newDbWithOpts(initValBucket, initVals, nutsdb.DefaultOptions)
}

func newDbWithOpts(initValBucket string, initVals map[string][]byte, opts nutsdb.Options) (string, *nutsdb.DB) {
	path := fmt.Sprintf("%s%creliable-test-%d", os.TempDir(), os.PathSeparator, rand.Int())

	opts.Dir = path
	db, err := nutsdb.Open(opts)
	if err != nil {
		panic(err)
	}

	// Add data
	if initValBucket != "" && len(initVals) > 0 {
		err := db.Update(func(tx *nutsdb.Tx) error {
			for k, v := range initVals {
				err := tx.Put(initValBucket, []byte(k), v, 0)
				if err != nil {
					return err
				}
			}
			return nil
		})

		if err != nil {
			panic(err)
		}
	}

	return path, db
}

func assertKeyVal(db *nutsdb.DB, bucket string, key []byte, val []byte) bool {
	r := true
	err := db.View(func(tx *nutsdb.Tx) error {
		v, err := tx.Get(bucket, key)
		if err != nil {
			return err
		}
		if !bytes.Equal(val, v.Value) {
			return errors.New("val and v are not equal")
		}
		return nil
	})
	if err != nil {
		r = false
	}

	return r
}

func existKey(db *nutsdb.DB, bucket string, key []byte) bool {
	r := true
	err := db.View(func(tx *nutsdb.Tx) error {
		entries, err := tx.GetAll(bucket)
		if err != nil {
			return err
		}

		for _, entry := range entries {
			if bytes.Equal(entry.Key, key) {
				return nil
			}
		}
		return errors.New("key not found")
	})
	if err != nil {
		r = false
	}

	return r
}

func destroyDb(path string, db *nutsdb.DB) {
	db.Close()
	os.RemoveAll(path)
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

func nutsdbOptionsWithDir(d string) nutsdb.Options {
	r := nutsdb.DefaultOptions
	r.Dir = d
	return r
}
