package devosender

import (
	"testing"
	"time"

	"github.com/xujiajun/nutsdb"
)

func TestReliableClient_String(t *testing.T) {
	type fields struct {
		Client                   *Client
		clientBuilder            *ClientBuilder
		db                       *nutsdb.DB
		bufferSize               uint
		eventTTLSeconds          uint32
		retryWait                time.Duration
		reconnWait               time.Duration
		retryStop                bool
		reconnStop               bool
		retryInitDelay           time.Duration
		reconnInitDelay          time.Duration
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
			"Client: {<nil>}, db: <nil>, bufferSize: 0, eventTTLSeconds: 0, retryWait: 0s, " +
				"reconnWait: 0s, retryStop: false, reconnStop: false, retryInitDelay: 0s, " +
				"reconnInitDelay: 0s, daemonStopTimeout: 0s, standByMode: false, enableStandByModeTimeout: 0s, " +
				"dbInitCleanedup: false, daemonStopped: <nil>, flushTimeout: 0s",
		},
		{
			"With some values",
			fields{
				Client: &Client{
					entryPoint: "udp://example.com:80",
				},
				db:                       &nutsdb.DB{},
				bufferSize:               123,
				eventTTLSeconds:          20,
				retryWait:                time.Minute,
				reconnWait:               time.Second * 10,
				retryStop:                true,
				reconnStop:               true,
				retryInitDelay:           time.Second * 2,
				reconnInitDelay:          time.Second,
				daemonStopTimeout:        time.Second * 5,
				standByMode:              true,
				enableStandByModeTimeout: time.Second * 3,
				dbInitCleanedup:          true,
				flushTimeout:             time.Minute * 2,
			},
			"Client: {entryPoint: 'udp://example.com:80', syslogHostname: '', defaultTag: '', " +
				"connAddr: '<nil>', ReplaceSequences: map[], tls: <nil>, #asyncErrors: 0, tcp: {<nil>}, " +
				"connectionUsedTimestamp: '0001-01-01 00:00:00 +0000 UTC', maxTimeConnActive: '0s', " +
				"#asyncItems: 0, lastSendCallTimestamp: '0001-01-01 00:00:00 +0000 UTC'}, db: {KeyCount: 0, ListIdx: map[]}, " +
				"bufferSize: 123, eventTTLSeconds: 20, retryWait: 1m0s, reconnWait: 10s, retryStop: true, " +
				"reconnStop: true, retryInitDelay: 2s, reconnInitDelay: 1s, daemonStopTimeout: 5s, " +
				"standByMode: true, enableStandByModeTimeout: 3s, dbInitCleanedup: true, " +
				"daemonStopped: <nil>, flushTimeout: 2m0s",
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
				retryWait:                tt.fields.retryWait,
				reconnWait:               tt.fields.reconnWait,
				retryStop:                tt.fields.retryStop,
				reconnStop:               tt.fields.reconnStop,
				retryInitDelay:           tt.fields.retryInitDelay,
				reconnInitDelay:          tt.fields.reconnInitDelay,
				daemonStopTimeout:        tt.fields.daemonStopTimeout,
				standByMode:              tt.fields.standByMode,
				enableStandByModeTimeout: tt.fields.enableStandByModeTimeout,
				dbInitCleanedup:          tt.fields.dbInitCleanedup,
				daemonStopped:            tt.fields.daemonStopped,
				flushTimeout:             tt.fields.flushTimeout,
			}
			if got := dsrc.String(); got != tt.want {
				t.Errorf("ReliableClient.String() = %v, want %v", got, tt.want)
			}
		})
	}
}
