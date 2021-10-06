package devosender

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
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

func Test_inc(t *testing.T) {
	type args struct {
		bucket          string
		key             []byte
		v               int
		errorIfNotFound bool
		existingKeys    map[string][]byte
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			"Key does not exist ignored",
			args{
				"test",
				[]byte("new_key"),
				1,
				false,
				make(map[string][]byte, 0),
			},
			"1",
			false,
		},
		{
			"Key does not exist error",
			args{
				"test",
				[]byte("new_key"),
				22,
				true,
				make(map[string][]byte, 0),
			},
			"0",
			true,
		},
		{
			"Key exists",
			args{
				"test",
				[]byte("test-key"),
				2,
				true,
				map[string][]byte{
					"test-key": []byte("22"),
				},
			},
			"24",
			false,
		},
		{
			"Key exists other format",
			args{
				"test",
				[]byte("test-key"),
				12,
				true,
				map[string][]byte{
					"test-key": []byte("tarari que te vi"),
				},
			},
			"12",
			false,
		},
		{
			"inc 0 key does not exists",
			args{
				"test",
				[]byte("new_key"),
				0,
				true,
				make(map[string][]byte, 0),
			},
			"0",
			false,
		},
		{
			"inc 0 other format",
			args{
				"test",
				[]byte("new_key"),
				0,
				true,
				map[string][]byte{
					"test-key": []byte("tarari"),
				},
			},
			"tarari",
			false,
		},
	}
	for _, tt := range tests {
		path, db := newDb(tt.args.bucket, tt.args.existingKeys)

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
		existingKeys    map[string][]byte
	}
	tests := []struct {
		name    string
		args    args
		want    int
		wantErr bool
	}{
		{
			"Key does not exist ignored",
			args{
				"test",
				[]byte("new_key"),
				false,
				make(map[string][]byte, 0),
			},
			0,
			false,
		},
		{
			"Key does not exist error",
			args{
				"test",
				[]byte("new_key"),
				true,
				make(map[string][]byte, 0),
			},
			0,
			true,
		},
		{
			"Key exists",
			args{
				"test",
				[]byte("test-key"),
				true,
				map[string][]byte{
					"test-key": []byte("22"),
				},
			},
			22,
			false,
		},
		{
			"Key exists other format",
			args{
				"test",
				[]byte("test-key"),
				true,
				map[string][]byte{
					"test-key": []byte("tarari que te vi"),
				},
			},
			0,
			true,
		},
	}
	for _, tt := range tests {
		path, db := newDb(tt.args.bucket, tt.args.existingKeys)

		t.Run(tt.name, func(t *testing.T) {
			err := db.View(func(tx *nutsdb.Tx) error {
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
		bucket       string
		key          []byte
		v            int
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
				[]byte("new_key"),
				22,
				make(map[string][]byte, 0),
			},
			false,
		},
		{
			"Key exists",
			args{
				"test",
				[]byte("test-key"),
				12,
				map[string][]byte{
					"test-key": []byte("22"),
				},
			},
			false,
		},
		{
			"Key exists other format",
			args{
				"test",
				[]byte("test-key"),
				12,
				map[string][]byte{
					"test-key": []byte("tarari que te vi"),
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		path, db := newDb(tt.args.bucket, tt.args.existingKeys)

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

func newDb(initValBucket string, initVals map[string][]byte) (string, *nutsdb.DB) {
	path := fmt.Sprintf("%s%creliable-test-%d", os.TempDir(), os.PathSeparator, rand.Int())

	opts := nutsdb.DefaultOptions
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
