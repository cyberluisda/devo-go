package devosender

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
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
			if !reflect.DeepEqual(got, tt.want) {
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
			if !reflect.DeepEqual(got, tt.want) {
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
			if !reflect.DeepEqual(got, tt.want) {
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
