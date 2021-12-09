package devosender

import (
	"fmt"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/cyberluisda/devo-go/applogger"
)

func TestLazyClient_StandBy(t *testing.T) {
	tests := []struct {
		name                string
		lazyClient          *LazyClient
		wantErr             bool
		wantLogEventPattern *regexp.Regexp
	}{
		{
			"Error waiting for pending msgs",
			func() *LazyClient {
				r, err := NewLazyClientBuilder().
					ClientBuilder(NewClientBuilder().EntryPoint("tcp://example.com:80")). // We need a real connection
					EnableStandByModeTimeout(time.Microsecond).                           // To force timeout
					Build()
				if err != nil {
					panic(err)
				}
				return r
			}(),
			true,
			nil,
		},
		{
			"Log error while close connection",
			func() *LazyClient {
				r, err := NewLazyClientBuilder().
					ClientBuilder(NewClientBuilder().EntryPoint("tcp://example.com:80")). // We need a real connection
					AppLogger(&MemoryAppLogger{}).
					Build()
				if err != nil {
					panic(err)
				}

				// Close connection to force error
				r.Client.conn.Close()
				r.Client.conn = nil
				return r
			}(),
			false,
			regexp.MustCompile(`^WARN: Error while close inner client. Uninstantiate client anyway:`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.lazyClient.StandBy(); (err != nil) != tt.wantErr {
				t.Errorf("LazyClient.StandBy() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.wantLogEventPattern != nil {
				mal, ok := tt.lazyClient.appLogger.(*MemoryAppLogger)
				if ok {
					logEvent := mal.Events[len(mal.Events)-1]
					if !tt.wantLogEventPattern.MatchString(logEvent) {
						t.Errorf("LazyClient.StandBy() wantLogEventPatter = %v, last log msg := %s", tt.wantLogEventPattern, logEvent)
					}

				} else {
					t.Errorf("LazyClient.StandBy() wantLogEventPatter = %v, but logger MemoryAppLogger instance: %T", tt.wantLogEventPattern, tt.lazyClient.appLogger)
				}
			}
		})
	}
}

func TestLazyClient_WakeUp(t *testing.T) {
	tests := []struct {
		name       string
		lazyClient *LazyClient
		wantErr    bool
	}{
		{
			"Error while recreate client",
			func() *LazyClient {
				r, err := NewLazyClientBuilder().
					ClientBuilder(NewClientBuilder().EntryPoint("udp://localhost:13000")).
					Build()
				if err != nil {
					panic(err)
				}

				// Pass to stand-by
				err = r.StandBy()
				if err != nil {
					panic(err)
				}

				// Change ClientBuilder to force error
				r.clientBuilder.entrypoint = ""

				return r
			}(),
			true,
		},
		{
			"Error while flush pending events",
			func() *LazyClient {
				r, err := NewLazyClientBuilder().
					ClientBuilder(NewClientBuilder().EntryPoint("tcp://example.com:80")). // We need a real connection
					FlushTimeout(time.Microsecond).                                       // to force flush timeout error
					Build()
				if err != nil {
					panic(err)
				}

				// Pass to stand-by
				err = r.StandBy()
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
			if err := tt.lazyClient.WakeUp(); (err != nil) != tt.wantErr {
				t.Errorf("LazyClient.WakeUp() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLazyClient_Close(t *testing.T) {
	tests := []struct {
		name       string
		lazyClient *LazyClient
		wantErr    bool
	}{
		{
			"WakeUp error",
			func() *LazyClient {
				r, err := NewLazyClientBuilder().
					ClientBuilder(NewClientBuilder().EntryPoint("udp://localhost:13000")).
					Build()
				if err != nil {
					panic(err)
				}

				// Pass to stand-by
				err = r.StandBy()
				if err != nil {
					panic(err)
				}

				// Change ClientBuilder to force error
				r.clientBuilder.entrypoint = ""

				return r
			}(),
			true,
		},
		{
			"Flush error",
			func() *LazyClient {
				r, err := NewLazyClientBuilder().
					ClientBuilder(NewClientBuilder().EntryPoint("tcp://example.com:80")). // We need a real connection
					FlushTimeout(time.Microsecond).                                       // to force flush timeout error
					Build()
				if err != nil {
					panic(err)
				}

				return r
			}(),
			true,
		},
		{
			"StandBy error",
			func() *LazyClient {
				r, err := NewLazyClientBuilder().
					ClientBuilder(NewClientBuilder().EntryPoint("tcp://example.com:80")). // We need a real connection
					EnableStandByModeTimeout(time.Microsecond).                           // To force timeout
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
			if err := tt.lazyClient.Close(); (err != nil) != tt.wantErr {
				t.Errorf("LazyClient.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLazyClient_Wakeup_Close_unblocked(t *testing.T) {
	// Unit tests to reproduce https://github.com/cyberluisda/devo-go/issues/26

	// Create client with  very insane flush timeout
	c, err := NewLazyClientBuilder().
		ClientBuilder(NewClientBuilder().EntryPoint("udp://localhost:13000")).
		FlushTimeout(time.Microsecond).
		Build()
	if err != nil {
		t.FailNow()
	}

	// pass to standby and send async msg
	err = c.StandBy()
	if err != nil {
		t.FailNow()
	}
	c.SendWTagAsync("test.keep.free", "msg")

	// Call wake up that should return error:
	err = c.WakeUp()
	if err == nil {
		t.Errorf("LazyClient.Wakeup and then LazyClient.Close, Wakeup want error bug got nil")
	}

	// Call Close() and waiting for timeout
	w := make(chan error, 1)
	go func() {
		w <- c.Close()
	}()

	select {
	case err = <-w:
		if err == nil {
			t.Errorf("LazyClient.Wakeup and then LazyClient.Close, Close want error bug got nil")
		}
	case <-time.After(time.Millisecond * 200):
		t.Errorf("LazyClient.Wakeup and then LazyClient.Close timeout reached while wait for Close")
	}
}

func TestLazyClient_popBuffer(t *testing.T) {
	type fields struct {
		buffer []*lazyClientRecord
	}
	tests := []struct {
		name       string
		fields     fields
		want       *lazyClientRecord
		want1      bool
		wantBuffer []*lazyClientRecord
	}{
		{
			"Empty buffer",
			fields{},
			nil,
			false,
			nil,
		},
		{
			"One element buffer",
			fields{
				[]*lazyClientRecord{
					{
						AsyncID: "async id",
						Msg:     "msg",
						Tag:     "tag",
					},
				},
			},
			&lazyClientRecord{
				AsyncID: "async id",
				Msg:     "msg",
				Tag:     "tag",
			},
			true,
			nil,
		},
		{
			"Two elements buffer",
			fields{
				[]*lazyClientRecord{
					{
						AsyncID: "async id 1",
						Msg:     "msg 1",
						Tag:     "tag 1",
					},
					{
						AsyncID: "async id 2",
						Msg:     "msg 2",
						Tag:     "tag 2",
					},
				},
			},
			&lazyClientRecord{
				AsyncID: "async id 1",
				Msg:     "msg 1",
				Tag:     "tag 1",
			},
			true,
			[]*lazyClientRecord{
				{
					AsyncID: "async id 2",
					Msg:     "msg 2",
					Tag:     "tag 2",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc := &LazyClient{
				buffer: tt.fields.buffer,
			}
			got, got1 := lc.popBuffer()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LazyClient.popBuffer() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("LazyClient.popBuffer() got1 = %v, want %v", got1, tt.want1)
			}
			if !reflect.DeepEqual(lc.buffer, tt.wantBuffer) {
				t.Errorf("LazyClient.popBuffer() remaining buffer got = %#v, want %#v", lc.buffer, tt.wantBuffer)
			}
		})
	}
}

func TestLazyClient_undoPopBuffer(t *testing.T) {
	type fields struct {
		bufferSize uint32
		buffer     []*lazyClientRecord
	}
	type args struct {
		r *lazyClientRecord
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantErr    bool
		wantBuffer []*lazyClientRecord
	}{
		{
			"Nil record",
			fields{},
			args{},
			false,
			nil,
		},
		{
			"Zero buffer size",
			fields{},
			args{
				&lazyClientRecord{
					AsyncID: "async id 1",
					Msg:     "msg 1",
					Tag:     "tag 1",
				},
			},
			true,
			nil,
		},
		{
			"Empty buffer",
			fields{
				bufferSize: 1,
			},
			args{
				&lazyClientRecord{
					AsyncID: "async id 1",
					Msg:     "msg 1",
					Tag:     "tag 1",
				},
			},
			false,
			[]*lazyClientRecord{
				{
					AsyncID: "async id 1",
					Msg:     "msg 1",
					Tag:     "tag 1",
				},
			},
		},
		{
			"Full buffer",
			fields{
				bufferSize: 1,
				buffer: []*lazyClientRecord{
					{
						AsyncID: "async id 1",
						Msg:     "msg 1",
						Tag:     "tag 1",
					},
				},
			},
			args{
				&lazyClientRecord{
					AsyncID: "async id 2",
					Msg:     "msg 2",
					Tag:     "tag 2",
				},
			},
			true,
			[]*lazyClientRecord{
				{
					AsyncID: "async id 1",
					Msg:     "msg 1",
					Tag:     "tag 1",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc := &LazyClient{
				bufferSize: tt.fields.bufferSize,
				buffer:     tt.fields.buffer,
			}
			if err := lc.undoPopBuffer(tt.args.r); (err != nil) != tt.wantErr {
				t.Errorf("LazyClient.undoPopBuffer() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(lc.buffer, tt.wantBuffer) {
				t.Errorf("LazyClient.undoPopBuffer() remaining buffer got = %#v, want %#v", lc.buffer, tt.wantBuffer)
			}
		})
	}
}

type MemoryAppLogger struct {
	Events []string
	Level  applogger.Level
}

func (mal *MemoryAppLogger) IsLevelEnabled(l applogger.Level) bool {
	return l <= mal.Level
}

func (mal *MemoryAppLogger) Log(l applogger.Level, a ...interface{}) {
	mal.Events = append(mal.Events, fmt.Sprint(l, a))
}

func (mal *MemoryAppLogger) Logf(l applogger.Level, format string, a ...interface{}) {
	mal.Events = append(mal.Events, fmt.Sprintf("%s: "+format, applogger.LevelString(l), a))
}
