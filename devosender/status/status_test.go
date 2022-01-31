package status

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/cyberluisda/devo-go/devosender"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/xujiajun/nutsdb"
)

func TestNutsDBStatusBuilder_DbSegmentSize(t *testing.T) {
	type args struct {
		size int64
	}
	tests := []struct {
		name string
		nsb  *NutsDBStatusBuilder
		args args
		want *NutsDBStatusBuilder
	}{
		{
			"Set value",
			&NutsDBStatusBuilder{},
			args{1024},
			&NutsDBStatusBuilder{
				dbOpts: nutsdb.Options{
					SegmentSize: 1024,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.nsb.DbSegmentSize(tt.args.size); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NutsDBStatusBuilder.DbSegmentSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNutsDBStatusBuilder_DbEntryIdxMode(t *testing.T) {
	type args struct {
		mode nutsdb.EntryIdxMode
	}
	tests := []struct {
		name string
		nsb  *NutsDBStatusBuilder
		args args
		want *NutsDBStatusBuilder
	}{
		{
			"Set value",
			&NutsDBStatusBuilder{},
			args{nutsdb.HintKeyAndRAMIdxMode},
			&NutsDBStatusBuilder{
				dbOpts: nutsdb.Options{
					EntryIdxMode: nutsdb.HintKeyAndRAMIdxMode,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.nsb.DbEntryIdxMode(tt.args.mode); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NutsDBStatusBuilder.DbEntryIdxMode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNutsDBStatusBuilder_DbRWMode(t *testing.T) {
	type args struct {
		mode nutsdb.RWMode
	}
	tests := []struct {
		name string
		nsb  *NutsDBStatusBuilder
		args args
		want *NutsDBStatusBuilder
	}{
		{
			"Set value",
			&NutsDBStatusBuilder{},
			args{nutsdb.MMap},
			&NutsDBStatusBuilder{
				dbOpts: nutsdb.Options{
					RWMode:               nutsdb.MMap,
					StartFileLoadingMode: nutsdb.MMap,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.nsb.DbRWMode(tt.args.mode); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NutsDBStatusBuilder.DbRWMode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNutsDBStatusBuilder_BufferSize(t *testing.T) {
	type args struct {
		size uint
	}
	tests := []struct {
		name string
		nsb  *NutsDBStatusBuilder
		args args
		want *NutsDBStatusBuilder
	}{
		{
			"Set value",
			&NutsDBStatusBuilder{},
			args{1234},
			&NutsDBStatusBuilder{bufferSize: 1234},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.nsb.BufferSize(tt.args.size); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NutsDBStatusBuilder.BufferSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNutsDBStatusBuilder_FilesToConsolidateDb(t *testing.T) {
	type args struct {
		files int
	}
	tests := []struct {
		name string
		nsb  *NutsDBStatusBuilder
		args args
		want *NutsDBStatusBuilder
	}{
		{
			"Set value",
			&NutsDBStatusBuilder{},
			args{5},
			&NutsDBStatusBuilder{filesToConsolidateDb: 5},
		},
		{
			"Ignore invalild value",
			&NutsDBStatusBuilder{filesToConsolidateDb: 4},
			args{1},
			&NutsDBStatusBuilder{filesToConsolidateDb: 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.nsb.FilesToConsolidateDb(tt.args.files); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NutsDBStatusBuilder.FilesToConsolidateDb() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNutsDBStatusBuilder_RecreateDbClientAfterConsolidation(t *testing.T) {
	type args struct {
		b bool
	}
	tests := []struct {
		name string
		nsb  *NutsDBStatusBuilder
		args args
		want *NutsDBStatusBuilder
	}{
		{
			"Set value",
			&NutsDBStatusBuilder{},
			args{true},
			&NutsDBStatusBuilder{recreateDbClientAfterConsolidation: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.nsb.RecreateDbClientAfterConsolidation(tt.args.b); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NutsDBStatusBuilder.RecreateDbClientAfterConsolidation() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNutsDBStatusBuilder_Build(t *testing.T) {
	tests := []struct {
		name            string
		nsb             *NutsDBStatusBuilder
		wantNilDBStatus bool
		wantErr         bool
	}{
		{
			"Error dbOpts.Dir empty",
			&NutsDBStatusBuilder{},
			true,
			true,
		},
		{
			"Error opent nutsdb",
			&NutsDBStatusBuilder{
				dbOpts: nutsdb.Options{
					Dir: "/only/root/should/create/this/dir",
				},
			},
			true,
			true,
		},
		{
			"Error while initialize",
			&NutsDBStatusBuilder{
				dbOpts: func() nutsdb.Options {
					r := nutsdb.DefaultOptions
					r.Dir = toolsTestNewTempDirName()
					return r
				}(),
				filesToConsolidateDb: 1,
			},
			false,
			true,
		},
		{
			"Open db",
			&NutsDBStatusBuilder{
				dbOpts: func() nutsdb.Options {
					r := nutsdb.DefaultOptions
					r.Dir = toolsTestNewTempDirName()
					return r
				}(),
				filesToConsolidateDb: 3,
			},
			false,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.nsb.Build()
			if (err != nil) != tt.wantErr {
				t.Errorf("NutsDBStatusBuilder.Build() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if (got == nil) != tt.wantNilDBStatus {
				t.Errorf("NutsDBStatusBuilder.Build() got = %v, want nil %v", got, tt.wantNilDBStatus)
			}

			// Clean
			if got != nil && got.dbOpts.Dir != "" {
				got.Close()
				err := os.RemoveAll(got.dbOpts.Dir)
				if err != nil {
					panic(err)
				}
			}
		})
	}
}

func TestNutsDBStatus_New(t *testing.T) {
	type setup struct {
		initialOrderIdx   *orderIdx
		initialDataBucket map[string][]byte
	}
	type args struct {
		er *EventRecord
	}

	now := time.Now()

	tests := []struct {
		name                 string
		setup                setup
		ns                   *NutsDBStatus
		args                 args
		wantErr              bool
		wantERInStatus       map[string]*EventRecord
		wantOrderIdxInStatus *orderIdx
	}{
		{
			"Nil input",
			setup{},
			&NutsDBStatus{},
			args{nil},
			true,
			nil,
			nil,
		},
		{
			"Error idx not found",
			setup{},
			&NutsDBStatus{
				eventTTL: 120, // expiration time is 2 minutes
			},
			args{
				&EventRecord{
					AsyncIDs:  []string{"id-1"},
					Timestamp: now,
				},
			},
			true,
			nil,
			nil,
		},
		{
			"Error event exists",
			setup{
				initialOrderIdx: &orderIdx{
					Order: []string{"id-1"},
					Refs:  map[string]string{"id-1": "id-1"},
				},
				initialDataBucket: map[string][]byte{
					"id-1": func() []byte {
						er := &EventRecord{AsyncIDs: []string{"id-1"}}
						r, err := er.Serialize()
						if err != nil {
							panic(err)
						}
						return r
					}(),
				},
			},
			&NutsDBStatus{},
			args{&EventRecord{AsyncIDs: []string{"id-1"}}},
			true,
			map[string]*EventRecord{
				"id-1": {AsyncIDs: []string{"id-1"}},
			},
			&orderIdx{
				Order: []string{"id-1"},
				Refs: map[string]string{
					"id-1": "id-1",
				},
			},
		},
		{
			"Event expired",
			setup{
				initialOrderIdx: &orderIdx{
					Order: []string{"id-1"},
					Refs:  map[string]string{"id-1": "id-1"},
				},
				initialDataBucket: map[string][]byte{
					"id-1": func() []byte {
						er := &EventRecord{AsyncIDs: []string{"id-1"}}
						r, err := er.Serialize()
						if err != nil {
							panic(err)
						}
						return r
					}(),
				},
			},
			&NutsDBStatus{
				eventTTL: 0, // all events expired
			},
			args{
				&EventRecord{
					AsyncIDs:  []string{"id-2"},
					Timestamp: now,
				},
			},
			false,
			map[string]*EventRecord{
				"id-1": {AsyncIDs: []string{"id-1"}},
			},
			&orderIdx{
				Order: []string{"id-1"},
				Refs: map[string]string{
					"id-1": "id-1",
				},
			},
		},
		{
			"Drop event by buffer full reason",
			setup{
				initialOrderIdx: &orderIdx{
					Order: []string{"id-1"},
					Refs:  map[string]string{"id-1": "id-1"},
				},
				initialDataBucket: map[string][]byte{
					"id-1": func() []byte {
						er := &EventRecord{AsyncIDs: []string{"id-1"}}
						r, err := er.Serialize()
						if err != nil {
							panic(err)
						}
						return r
					}(),
				},
			},
			&NutsDBStatus{
				eventTTL:   120, // expiration time is 2 minutes
				bufferSize: 1,
			},
			args{
				&EventRecord{
					AsyncIDs:  []string{"id-2"},
					Timestamp: now,
				},
			},
			false,
			map[string]*EventRecord{
				"id-2": {AsyncIDs: []string{"id-2"}, Timestamp: now},
			},
			&orderIdx{
				Order: []string{"id-2"},
				Refs: map[string]string{
					"id-2": "id-2",
				},
			},
		},
		{
			"New event",
			setup{
				initialOrderIdx: &orderIdx{
					Order: []string{"id-1"},
					Refs:  map[string]string{"id-1": "id-1"},
				},
				initialDataBucket: map[string][]byte{
					"id-1": func() []byte {
						er := &EventRecord{AsyncIDs: []string{"id-1"}}
						r, err := er.Serialize()
						if err != nil {
							panic(err)
						}
						return r
					}(),
				},
			},
			&NutsDBStatus{
				eventTTL:   120, // expiration time is 2 minutes
				bufferSize: 10,
			},
			args{
				&EventRecord{
					AsyncIDs:  []string{"id-2"},
					Timestamp: now,
				},
			},
			false,
			map[string]*EventRecord{
				// Index is updated when we search EventRecord using ns.Get
				"id-1": {AsyncIDs: []string{"id-1"}},
				"id-2": {AsyncIDs: []string{"id-2"}, Timestamp: now},
			},
			&orderIdx{
				Order: []string{"id-1", "id-2"},
				Refs: map[string]string{
					"id-1": "id-1",
					"id-2": "id-2",
				},
			},
		},
	}
	for _, tt := range tests {

		// Intialize status db
		path, db := toolTestNewDb(dataBucket, tt.setup.initialDataBucket)
		tt.ns.db = db
		tt.ns.dbOpts = nutsdb.DefaultOptions
		tt.ns.dbOpts.Dir = path

		if tt.setup.initialOrderIdx != nil {
			err := db.Update(func(tx *nutsdb.Tx) error {
				return saveOrderIdxInTx(tx, tt.setup.initialOrderIdx)
			})
			if err != nil {
				panic(err)
			}
		}

		t.Run(tt.name, func(t *testing.T) {
			if err := tt.ns.New(tt.args.er); (err != nil) != tt.wantErr {
				t.Errorf("NutsDBStatus.New() error = %+v, want %+v", err, tt.wantErr)
			}

			if len(tt.wantERInStatus) > 0 {
				for k, v := range tt.wantERInStatus {
					got, _, err := tt.ns.Get(k)
					if err != nil {
						t.Errorf("NutsDBStatus.New() status EventRecord unexpected derror: %v", err)
					} else {
						if got != nil && got.Timestamp.Sub(v.Timestamp) == 0 {
							v.Timestamp = got.Timestamp
						}
						if !reflect.DeepEqual(got, v) {
							// Fix timestamp comparation
							t.Errorf("NutsDBStatus.New() status EventRecord got = %+v, want %+v", got, v)
						}
					}
				}
			}

			if tt.wantOrderIdxInStatus != nil {
				var got *orderIdx
				err := tt.ns.db.View(func(tx *nutsdb.Tx) error {
					var err error
					got, err = getOrderIdxInTx(tx)
					return err
				})
				if err != nil {
					t.Errorf("NutsDBStatus.New() status orderIdx unexpected derror: %v", err)
				} else if !reflect.DeepEqual(got, tt.wantOrderIdxInStatus) {
					t.Errorf("NutsDBStatus.New() status orderIdx got = %+v, want %+v", got, tt.wantOrderIdxInStatus)
				}
			}
		})

		// Clean
		tt.ns.Close()
		toolTestDestroyDb(path, db)
	}
}

func TestNutsDBStatus_Update(t *testing.T) {
	type setup struct {
		initialOrderIdx   *orderIdx
		initialDataBucket map[string][]byte
	}
	type args struct {
		oldID string
		newID string
	}
	tests := []struct {
		name                 string
		setup                setup
		ns                   *NutsDBStatus
		args                 args
		wantErr              bool
		wantERInStatus       map[string]*EventRecord
		wantOrderIdxInStatus *orderIdx
	}{
		{
			"Error load event",
			setup{},
			&NutsDBStatus{},
			args{"id-1", "id-2"},
			true,
			nil,
			nil,
		},
		{
			"ID not found in index",
			setup{
				initialOrderIdx: &orderIdx{
					Order: []string{"id-1"},
					Refs:  map[string]string{"id-1": "id-1"},
				},
				initialDataBucket: map[string][]byte{
					"id-1": func() []byte {
						er := &EventRecord{AsyncIDs: []string{"id-1"}}
						r, err := er.Serialize()
						if err != nil {
							panic(err)
						}
						return r
					}(),
				},
			},
			&NutsDBStatus{},
			args{"id-4", "id-5"},
			true,
			nil,
			&orderIdx{
				Order: []string{"id-1"},
				Refs:  map[string]string{"id-1": "id-1"},
			},
		},
		{
			"Update event",
			setup{
				initialOrderIdx: &orderIdx{
					Order: []string{"id-1"},
					Refs:  map[string]string{"id-1": "id-1"},
				},
				initialDataBucket: map[string][]byte{
					"id-1": func() []byte {
						er := &EventRecord{AsyncIDs: []string{"id-1"}}
						r, err := er.Serialize()
						if err != nil {
							panic(err)
						}
						return r
					}(),
				},
			},
			&NutsDBStatus{},
			args{"id-1", "id-2"},
			false,
			map[string]*EventRecord{
				// Index is updated when we search EventRecord using ns.Get
				"id-2": {AsyncIDs: []string{"id-1", "id-2"}},
			},
			&orderIdx{
				Order: []string{"id-2"},
				Refs:  map[string]string{"id-2": "id-1"},
			},
		},
	}
	for _, tt := range tests {

		// Intialize status db
		path, db := toolTestNewDb(dataBucket, tt.setup.initialDataBucket)
		tt.ns.db = db
		tt.ns.dbOpts = nutsdb.DefaultOptions
		tt.ns.dbOpts.Dir = path

		if tt.setup.initialOrderIdx != nil {
			err := db.Update(func(tx *nutsdb.Tx) error {
				return saveOrderIdxInTx(tx, tt.setup.initialOrderIdx)
			})
			if err != nil {
				panic(err)
			}
		}

		t.Run(tt.name, func(t *testing.T) {
			if err := tt.ns.Update(tt.args.oldID, tt.args.newID); (err != nil) != tt.wantErr {
				t.Errorf("NutsDBStatus.Update() error = %+v, want %+v", err, tt.wantErr)
			}

			if len(tt.wantERInStatus) > 0 {
				for k, v := range tt.wantERInStatus {
					got, _, err := tt.ns.Get(k)
					if err != nil {
						t.Errorf("NutsDBStatus.Update() status EventRecord unexpected derror: %v", err)
					} else {
						if got != nil && got.Timestamp.Sub(v.Timestamp) == 0 {
							v.Timestamp = got.Timestamp
						}
						if !reflect.DeepEqual(got, v) {
							// Fix timestamp comparation
							t.Errorf("NutsDBStatus.Update() status EventRecord got = %+v, want %+v", got, v)
						}
					}
				}
			}

			if tt.wantOrderIdxInStatus != nil {
				var got *orderIdx
				err := tt.ns.db.View(func(tx *nutsdb.Tx) error {
					var err error
					got, err = getOrderIdxInTx(tx)
					return err
				})
				if err != nil {
					t.Errorf("NutsDBStatus.Update() status orderIdx unexpected derror: %v", err)
				} else if !reflect.DeepEqual(got, tt.wantOrderIdxInStatus) {
					t.Errorf("NutsDBStatus.Update() status orderIdx got = %+v, want %+v", got, tt.wantOrderIdxInStatus)
				}
			}
		})

		// Clean
		tt.ns.Close()
		toolTestDestroyDb(path, db)
	}
}

func TestNutsDBStatus_Get(t *testing.T) {
	type setup struct {
		initialOrderIdx    *orderIdx
		initialDataBucket  map[string][]byte
		initialCounCounter int
	}
	type args struct {
		ID string
	}
	tests := []struct {
		name    string
		setup   setup
		ns      *NutsDBStatus
		args    args
		want    *EventRecord
		wantPos int
		wantErr bool
	}{
		{
			"Idx not found error",
			setup{},
			&NutsDBStatus{},
			args{"not found"},
			nil,
			0,
			true,
		},
		{
			"ID not found in index",
			setup{
				initialOrderIdx: &orderIdx{
					Order: []string{"id-1"},
					Refs:  map[string]string{"id-1": "id-1"},
				},
			},
			&NutsDBStatus{},
			args{"id-4"},
			nil,
			-1,
			true,
		},
		{
			"Error count counter",
			setup{
				initialOrderIdx: &orderIdx{
					Order: []string{"id-1"},
					Refs:  map[string]string{"id-1": "id-1"},
				},
			},
			&NutsDBStatus{},
			args{"id-1"},
			nil,
			0,
			true,
		},
		{
			"Error event expired",
			setup{
				initialOrderIdx: &orderIdx{
					Order: []string{"id-1"},
					Refs:  map[string]string{"id-1": "id-1"},
				},
				initialCounCounter: 1,
			},
			&NutsDBStatus{},
			args{"id-1"},
			nil,
			-1,
			true,
		},
		{
			"Error get event",
			setup{
				initialOrderIdx: &orderIdx{
					Order: []string{"id-1"},
					Refs:  map[string]string{"id-1": "id-1"},
				},
				initialCounCounter: 1,
				initialDataBucket: map[string][]byte{
					"id-1": []byte("unmarshall error"),
				},
			},
			&NutsDBStatus{},
			args{"id-1"},
			nil,
			0,
			true,
		},
		{
			"Get event",
			setup{
				initialOrderIdx: &orderIdx{
					Order: []string{"id-1"},
					Refs:  map[string]string{"id-1": "id-1"},
				},
				initialCounCounter: 1,
				initialDataBucket: map[string][]byte{
					"id-1": func() []byte {
						er := &EventRecord{
							AsyncIDs: []string{"id-1"},
						}
						r, _ := er.Serialize()
						return r
					}(),
				},
			},
			&NutsDBStatus{},
			args{"id-1"},
			&EventRecord{
				AsyncIDs: []string{"id-1"},
			},
			0,
			false,
		},
	}
	for _, tt := range tests {

		// Intialize status db
		path, db := toolTestNewDb(dataBucket, tt.setup.initialDataBucket)
		tt.ns.db = db
		tt.ns.dbOpts = nutsdb.DefaultOptions
		tt.ns.dbOpts.Dir = path

		if tt.setup.initialOrderIdx != nil {
			err := db.Update(func(tx *nutsdb.Tx) error {
				return saveOrderIdxInTx(tx, tt.setup.initialOrderIdx)
			})
			if err != nil {
				panic(err)
			}
		}
		if tt.setup.initialCounCounter > 0 {
			err := db.Update(func(tx *nutsdb.Tx) error {
				return set(tx, statsBucket, countKey, tt.setup.initialCounCounter)
			})
			if err != nil {
				panic(err)
			}
		}

		t.Run(tt.name, func(t *testing.T) {
			got, gotPos, err := tt.ns.Get(tt.args.ID)
			if (err != nil) != tt.wantErr {
				t.Errorf("NutsDBStatus.FinishRecord() error = %+v, want %+v", err, tt.wantErr)
			}

			if tt.want != nil && got != nil {
				if tt.want.Timestamp.Sub(got.Timestamp) == 0 {
					// Fixing timestamp to prevent timezone error when compare using DeepEqual
					tt.want.Timestamp = got.Timestamp
				}
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NutsDBStatus.FinishRecord() got = %+v, want %+v", got, tt.want)
			}

			if gotPos != tt.wantPos {
				t.Errorf("NutsDBStatus.FinishRecord() pos = %v, want %v", gotPos, tt.wantPos)
			}
		})

		// Clean
		tt.ns.Close()
		toolTestDestroyDb(path, db)
	}
}

func TestNutsDBStatus_FinishRecord(t *testing.T) {
	type setup struct {
		initialOrderIdx      *orderIdx
		initialDataBucket    map[string][]byte
		initialCounCounter   int
		initialFinishCounter int
	}
	type args struct {
		ID string
	}
	tests := []struct {
		name    string
		setup   setup
		ns      *NutsDBStatus
		args    args
		wantErr bool
	}{
		{
			"Idx not found error",
			setup{},
			&NutsDBStatus{},
			args{"not found"},
			true,
		},
		{
			"ID not found in index",
			setup{
				initialOrderIdx: &orderIdx{},
			},
			&NutsDBStatus{},
			args{"id-1"},
			true,
		},
		{
			"Error decrement count counter",
			setup{
				initialOrderIdx: &orderIdx{
					Order: []string{"id-1"},
					Refs: map[string]string{
						"id-1": "id-1",
					},
				},
			},
			&NutsDBStatus{},
			args{"id-1"},
			true,
		},
		{
			"ID only in idx",
			setup{
				initialOrderIdx: &orderIdx{
					Order: []string{"id-1"},
					Refs: map[string]string{
						"id-1": "id-1",
					},
				},
				initialCounCounter: 1,
			},
			&NutsDBStatus{},
			args{"id-1"},
			false,
		},
		{
			"Delete",
			setup{
				initialOrderIdx: &orderIdx{
					Order: []string{"id-1"},
					Refs: map[string]string{
						"id-1": "id-1",
					},
				},
				initialCounCounter: 1,
				initialDataBucket: map[string][]byte{
					"id-1": []byte("fake id-1"),
				},
			},
			&NutsDBStatus{},
			args{"id-1"},
			false,
		},
	}
	for _, tt := range tests {

		// Intialize status db
		path, db := toolTestNewDb(dataBucket, tt.setup.initialDataBucket)
		tt.ns.db = db
		tt.ns.dbOpts = nutsdb.DefaultOptions
		tt.ns.dbOpts.Dir = path

		if tt.setup.initialOrderIdx != nil {
			err := db.Update(func(tx *nutsdb.Tx) error {
				return saveOrderIdxInTx(tx, tt.setup.initialOrderIdx)
			})
			if err != nil {
				panic(err)
			}
		}
		if tt.setup.initialCounCounter > 0 {
			err := db.Update(func(tx *nutsdb.Tx) error {
				return set(tx, statsBucket, countKey, tt.setup.initialCounCounter)
			})
			if err != nil {
				panic(err)
			}
		}
		if tt.setup.initialFinishCounter > -1 {
			err := db.Update(func(tx *nutsdb.Tx) error {
				return set(tx, statsBucket, finishedKey, tt.setup.initialFinishCounter)
			})
			if err != nil {
				panic(err)
			}
		}

		t.Run(tt.name, func(t *testing.T) {
			err := tt.ns.FinishRecord(tt.args.ID)
			if (err != nil) != tt.wantErr {
				t.Errorf("NutsDBStatus.FinishRecord() error = %+v, want %+v", err, tt.wantErr)

			}
		})

		// Clean
		tt.ns.Close()
		toolTestDestroyDb(path, db)
	}
}

func TestNutsDBStatus_AllIDs(t *testing.T) {
	type setup struct {
		initialOrderIdx   *orderIdx
		initialDataBucket map[string][]byte
	}
	tests := []struct {
		name    string
		setup   setup
		ns      *NutsDBStatus
		want    []string
		wantErr bool
	}{
		{
			"Idx not found error",
			setup{},
			&NutsDBStatus{},
			nil,
			true,
		},
		{
			"Evicted event",
			setup{
				initialOrderIdx: &orderIdx{
					Order: []string{"id-1"},
					Refs: map[string]string{
						"id-1": "id-1",
					},
				},
			},
			&NutsDBStatus{},
			[]string{},
			false,
		},
		{
			"Return IDs",
			setup{
				initialOrderIdx: &orderIdx{
					Order: []string{"id-1", "id-3"},
					Refs: map[string]string{
						"id-1": "id-1",
						"id-3": "id-2",
					},
				},
				initialDataBucket: map[string][]byte{
					"id-1": []byte("fake id-1"),
					"id-2": []byte("fake id-2"),
					"id-9": []byte("ignored because is missing in index"),
				},
			},
			&NutsDBStatus{},
			[]string{"id-1", "id-3"},
			false,
		},
	}
	for _, tt := range tests {

		// Intialize status db
		path, db := toolTestNewDb(dataBucket, tt.setup.initialDataBucket)
		tt.ns.db = db
		tt.ns.dbOpts = nutsdb.DefaultOptions
		tt.ns.dbOpts.Dir = path

		if tt.setup.initialOrderIdx != nil {
			err := db.Update(func(tx *nutsdb.Tx) error {
				return saveOrderIdxInTx(tx, tt.setup.initialOrderIdx)
			})
			if err != nil {
				panic(err)
			}
		}

		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.ns.AllIDs()
			if (err != nil) != tt.wantErr {
				t.Errorf("NutsDBStatus.AllIDs() error = %+v, want %+v", err, tt.wantErr)

			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NutsDBStatus.AllIDs() got = %+v, want %+v", got, tt.want)
			}
		})

		// Clean
		tt.ns.Close()
		toolTestDestroyDb(path, db)
	}
}

func TestNutsDBStatus_Stats(t *testing.T) {
	type setup struct {
		initialCounters   map[string]int
		initialOrderIdx   *orderIdx
		initialDataBucket map[string][]byte
		enableDebugEnvVar bool
	}
	tests := []struct {
		name  string
		setup setup
		ns    *NutsDBStatus
		want  Stats
	}{
		{
			"Empty stats",
			setup{},
			&NutsDBStatus{},
			Stats{
				DbDataEntries: -1,
			},
		},
		{
			"Stats with values",
			setup{
				initialCounters: map[string]int{
					string(countKey):    5,
					string(droppedKey):  6,
					string(evictedKey):  7,
					string(finishedKey): 8,
					string(updatedKey):  9,
				},
				initialOrderIdx: &orderIdx{
					Order: []string{"id-1", "id-2"},
					Refs: map[string]string{
						"id-1": "id-1",
						"id-2": "id-2",
					},
				},
				initialDataBucket: map[string][]byte{
					"id-1": []byte("fake id-1"),
					"id-2": []byte("fake id-2"),
					"id-3": []byte("fake id-3"),
				},
			},
			&NutsDBStatus{},
			Stats{
				BufferCount:   5,
				Updated:       9,
				Finished:      8,
				Dropped:       6,
				Evicted:       7,
				DbIdxSize:     2,
				DbMaxFileID:   0,
				DbDataEntries: -1, // This is resolved only when DEBUG flag is enabled
			},
		},
		{
			"Debug stats",
			setup{
				enableDebugEnvVar: true,
				initialCounters: map[string]int{
					string(countKey):    5,
					string(droppedKey):  6,
					string(evictedKey):  7,
					string(finishedKey): 8,
					string(updatedKey):  9,
				},
				initialOrderIdx: &orderIdx{
					Order: []string{"id-1", "id-2"},
					Refs: map[string]string{
						"id-1": "id-1",
						"id-2": "id-2",
					},
				},
				initialDataBucket: map[string][]byte{
					"id-1": []byte("fake id-1"),
					"id-2": []byte("fake id-2"),
					"id-3": []byte("fake id-3"),
				},
			},
			&NutsDBStatus{},
			Stats{
				BufferCount:   5,
				Updated:       9,
				Finished:      8,
				Dropped:       6,
				Evicted:       7,
				DbIdxSize:     2,
				DbMaxFileID:   0,
				DbDataEntries: 3,
			},
		},
	}
	for _, tt := range tests {

		// Intialize status db
		path, db := toolTestNewDb(dataBucket, tt.setup.initialDataBucket)
		tt.ns.db = db
		tt.ns.dbOpts = nutsdb.DefaultOptions
		tt.ns.dbOpts.Dir = path

		if len(tt.setup.initialCounters) > 0 {
			err := db.Update(func(tx *nutsdb.Tx) error {
				for k, v := range tt.setup.initialCounters {
					err := set(tx, statsBucket, []byte(k), v)
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

		if tt.setup.initialOrderIdx != nil {
			err := db.Update(func(tx *nutsdb.Tx) error {
				return saveOrderIdxInTx(tx, tt.setup.initialOrderIdx)
			})
			if err != nil {
				panic(err)
			}
		}

		if tt.setup.enableDebugEnvVar {
			os.Setenv("DEVOGO_DEBUG_SENDER_STATS_COUNT_DATA", "yes")
		}

		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ns.Stats(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NutsDBStatus.Stats() got = %+v, want %+v", got, tt.want)
			}
		})

		// Clean
		tt.ns.Close()
		toolTestDestroyDb(path, db)
		if tt.setup.enableDebugEnvVar {
			os.Unsetenv("DEVOGO_DEBUG_SENDER_STATS_COUNT_DATA")
		}
	}
}

func TestNutsDBStatus_HouseKeeping(t *testing.T) {
	type setup struct {
		createDb     bool
		opts         *nutsdb.Options
		noFillNsOpts bool
		initialData  map[string][]byte
		deleteData   []string
	}
	tests := []struct {
		name              string
		setup             setup
		ns                *NutsDBStatus
		wantErr           bool
		wantNumberOfFiles int
	}{
		{
			"status db nil",
			setup{},
			&NutsDBStatus{},
			true,
			-1,
		},
		{
			"Error mergefiles",
			setup{createDb: true},
			&NutsDBStatus{},
			true,
			-1,
		},
		{
			"Error merge recreate client",
			setup{
				createDb: true,
				opts: func() *nutsdb.Options {
					r := nutsdb.DefaultOptions
					r.SegmentSize = 82 // To force generate more than two files
					return &r
				}(),
				noFillNsOpts: true,
				initialData: map[string][]byte{
					"111": []byte("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"),
					"222": []byte("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"),
					"333": []byte("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"),
				},
			},
			&NutsDBStatus{
				filesToConsolidateDb:               2,
				recreateDbClientAfterConsolidation: true,
			},
			true,
			-1,
		},
		{
			"Merge",
			setup{
				createDb: true,
				opts: func() *nutsdb.Options {
					r := nutsdb.DefaultOptions
					r.SegmentSize = 82 // To force generate more than two files
					return &r
				}(),
				initialData: map[string][]byte{
					"111": []byte("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"),
					"222": []byte("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"),
					"333": []byte("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"),
					"444": []byte("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"),
					"555": []byte("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"),
					"666": []byte("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"),
					"777": []byte("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"),
					"888": []byte("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"),
					"999": []byte("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"),
					"000": []byte("11111111111111111111111111111111"),
				},
				deleteData: []string{
					"111",
					"222",
					"333",
					"444",
					"555",
					"666",
					"777",
					"888",
					"999",
				},
			},
			&NutsDBStatus{
				filesToConsolidateDb: 2,
				dbOpts: nutsdb.Options{
					Dir: "/tmp/does/not/exits",
				},
			},
			false,
			1,
		},
	}
	for _, tt := range tests {
		var path string
		var db *nutsdb.DB
		if tt.setup.createDb {
			if tt.setup.opts == nil {
				path, db = toolTestNewDb("test", tt.setup.initialData)
				if !tt.setup.noFillNsOpts {
					tt.ns.dbOpts = nutsdb.DefaultOptions
				}
				tt.ns.dbOpts.Dir = path
			} else {
				path, db = toolTestNewDbWithOpts("test", tt.setup.initialData, *tt.setup.opts)
				if !tt.setup.noFillNsOpts {
					tt.ns.dbOpts = *tt.setup.opts
				}
				tt.ns.dbOpts.Dir = path
			}
			tt.ns.db = db

			err := db.Update(func(tx *nutsdb.Tx) error {
				for _, ID := range tt.setup.deleteData {
					err := tx.Delete("test", []byte(ID))
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

		t.Run(tt.name, func(t *testing.T) {
			if err := tt.ns.HouseKeeping(); (err != nil) != tt.wantErr {
				t.Errorf("NutsDBStatus.HouseKeeping() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.wantNumberOfFiles >= 0 && tt.wantNumberOfFiles != NumberOfFiles(path) {
				t.Errorf("NutsDBStatus.HouseKeeping() files got = %v, want %v", NumberOfFiles(path), tt.wantNumberOfFiles)
			}
		})

		if tt.setup.createDb {
			tt.ns.Close()
			toolTestDestroyDb(path, db)
		}
	}
}

func TestNutsDBStatus_Close(t *testing.T) {
	type setup struct {
		createDb bool
		closeDb  bool
	}
	tests := []struct {
		name    string
		setup   setup
		ns      *NutsDBStatus
		wantErr bool
	}{
		{
			"nil db",
			setup{},
			&NutsDBStatus{},
			false,
		},
		{
			"Close",
			setup{createDb: true},
			&NutsDBStatus{},
			false,
		},
		{
			"Previously closed",
			setup{createDb: true, closeDb: true},
			&NutsDBStatus{},
			true,
		},
	}
	for _, tt := range tests {
		var path string
		var db *nutsdb.DB
		if tt.setup.createDb {
			path, db = toolTestNewDb("", nil)
			tt.ns.db = db
			tt.ns.dbOpts = nutsdb.DefaultOptions
			tt.ns.dbOpts.Dir = path

			if tt.setup.closeDb {
				db.Close()
			}
		}

		t.Run(tt.name, func(t *testing.T) {
			if err := tt.ns.Close(); (err != nil) != tt.wantErr {
				t.Errorf("NutsDBStatus.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})

		if tt.setup.createDb {
			toolTestDestroyDb(path, db)
		}
	}
}

func TestNutsDBStatus_Initialize(t *testing.T) {
	type setup struct {
		createDb bool
	}
	tests := []struct {
		name           string
		setup          setup
		ns             *NutsDBStatus
		wantErr        bool
		wantIntialized bool
	}{
		{
			"Previously initialized",
			setup{},
			&NutsDBStatus{initialized: true},
			false,
			true,
		},
		{
			"House keeping error",
			setup{},
			&NutsDBStatus{},
			true,
			false,
		},
		{
			"Empty idx",
			setup{createDb: true},
			&NutsDBStatus{
				filesToConsolidateDb: 2,
			},
			false,
			true,
		},
	}
	for _, tt := range tests {
		var path string
		var db *nutsdb.DB
		if tt.setup.createDb {
			path, db = toolTestNewDb("", nil)
			tt.ns.db = db
			tt.ns.dbOpts = nutsdb.DefaultOptions
			tt.ns.dbOpts.Dir = path
		}

		t.Run(tt.name, func(t *testing.T) {
			if err := tt.ns.Initialize(); (err != nil) != tt.wantErr {
				t.Errorf("NutsDBStatus.Initialize() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.ns.initialized != tt.wantIntialized {
				t.Errorf("NutsDBStatus.Initialize() intiealized got = %v, want %v", tt.ns.initialized, tt.wantIntialized)
			}
		})

		if tt.setup.createDb {
			tt.ns.Close()
			toolTestDestroyDb(path, db)
		}
	}
}

func Test_recreateIdxInTx(t *testing.T) {
	type setup struct {
		initVals []*EventRecord
		closedTx bool
	}
	type args struct {
		idx *orderIdx
	}
	tests := []struct {
		name    string
		setup   setup
		args    args
		want    *orderIdx
		wantErr bool
	}{
		{
			"Error idx is nil",
			setup{},
			args{},
			nil,
			true,
		},
		{
			"Error loading records",
			setup{closedTx: true},
			args{idx: &orderIdx{}},
			nil,
			true,
		},
		{
			"Reset idx",
			setup{},
			args{
				idx: &orderIdx{
					Order: []string{"id-1"},
					Refs: map[string]string{
						"id-1": "id-1",
					},
				},
			},
			&orderIdx{Refs: make(map[string]string, 0)},
			false,
		},
		{
			"Error entry without valid EffectiveID",
			setup{
				initVals: []*EventRecord{
					{},
				},
			},
			args{idx: &orderIdx{}},
			nil,
			true,
		},
		{
			"Fix orderId with same number of elements",
			setup{
				initVals: []*EventRecord{
					{
						AsyncIDs: []string{
							"id-1",
							"id-2",
						},
					},
				},
			},
			args{
				idx: &orderIdx{
					Order: []string{"id-4"},
					Refs: map[string]string{
						"id-4": "id-4",
					},
				},
			},
			&orderIdx{
				Order: []string{"id-2"},
				Refs: map[string]string{
					"id-2": "id-1",
				},
			},
			false,
		},
		{
			"Fix orderId",
			setup{
				initVals: []*EventRecord{
					{
						AsyncIDs: []string{
							"id-1",
							"id-2",
						},
					},
				},
			},
			args{
				idx: &orderIdx{
					Order: []string{"id-4"},
					Refs: map[string]string{
						"id-4": "id-4",
						"id-5": "id-5",
					},
				},
			},
			&orderIdx{
				Order: []string{"id-2"},
				Refs: map[string]string{
					"id-2": "id-1",
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		path, db := toolTestNewDb("", nil)
		if len(tt.setup.initVals) > 0 {
			data := map[string]map[string][]byte{
				dataBucket: make(map[string][]byte, len(tt.setup.initVals)),
			}

			for _, initVal := range tt.setup.initVals {
				bs, _ := initVal.Serialize()
				key := "no-key"
				if len(initVal.AsyncIDs) > 0 {
					key = initVal.AsyncIDs[0]
				}
				data[dataBucket][key] = bs
			}

			toolTestDbInitialData(db, data)
		}

		t.Run(tt.name, func(t *testing.T) {

			err := db.Update(func(tx *nutsdb.Tx) error {
				if tt.setup.closedTx {
					tx.Commit()
				}

				err := recreateIdxInTx(tx, tt.args.idx)
				if (err != nil) != tt.wantErr {
					t.Errorf("recreateIdxInTx() error = %v, wantErr %v", err, tt.wantErr)
					return err
				}

				return nil
			})

			if err != nil {
				return
			}

			if tt.want != nil {
				if !reflect.DeepEqual(tt.want, tt.args.idx) {
					t.Errorf("recreateIdxInTx() args.idx got = %+v, want %+v", tt.args.idx, tt.want)
				}
			}
		})

		toolTestDestroyDb(path, db)
	}
}

func Test_saveDataRecordInTx(t *testing.T) {
	now := time.Now()
	type setup struct {
		initVal  *EventRecord
		closedTx bool
	}
	type args struct {
		er  *EventRecord
		ttl uint32
	}
	tests := []struct {
		name    string
		setup   setup
		args    args
		want    *EventRecord
		wantErr bool
	}{
		{
			"Error save data",
			setup{
				closedTx: true,
			},
			args{
				er: &EventRecord{AsyncIDs: []string{"id-1"}},
			},
			nil,
			true,
		},
		{
			"New record",
			setup{},
			args{
				er: &EventRecord{
					AsyncIDs:  []string{"id-1"},
					Timestamp: now,
				},
			},
			&EventRecord{
				AsyncIDs:  []string{"id-1"},
				Timestamp: now,
			},
			false,
		},
		{
			"Replacing record",
			setup{
				initVal: &EventRecord{AsyncIDs: []string{"id-1"}},
			},
			args{
				er: &EventRecord{AsyncIDs: []string{"id-1", "id-2"}},
			},
			&EventRecord{AsyncIDs: []string{"id-1", "id-2"}},
			false,
		},
	}
	for _, tt := range tests {
		var bucket string
		initStatusKeyVal := make(map[string][]byte, 1)
		if tt.setup.initVal != nil {
			bucket = dataBucket
			bs, _ := tt.setup.initVal.Serialize()
			initStatusKeyVal[tt.setup.initVal.AsyncIDs[0]] = bs
		}
		path, db := toolTestNewDb(bucket, initStatusKeyVal)

		t.Run(tt.name, func(t *testing.T) {

			err := db.Update(func(tx *nutsdb.Tx) error {
				if tt.setup.closedTx {
					tx.Commit()
				}

				err := saveDataRecordInTx(tx, tt.args.er, tt.args.ttl)
				if (err != nil) != tt.wantErr {
					t.Errorf("saveDataRecordInTx() error = %v, wantErr %v", err, tt.wantErr)
					return err
				}

				return nil
			})

			if err != nil {
				return
			}

			if tt.want != nil {
				var got *EventRecord
				err = db.View(func(tx *nutsdb.Tx) error {
					ID := tt.want.AsyncIDs[0]
					var errInView error
					got, errInView = getDataRecordInTx(tx, []byte(ID))
					return errInView
				})
				if err != nil {
					t.Errorf("saveDataRecordInTx() while compare data saved error = %v", err)
				}
				if !tt.want.Timestamp.Equal(got.Timestamp) {
					t.Errorf("saveDataRecordInTx() saved er.Timestamp got = %+v, want %+v", got.Timestamp, tt.want.Timestamp)
				}
				tt.want.Timestamp = got.Timestamp // fixing reflect.DeepEqual comparation
				if !reflect.DeepEqual(tt.want, got) {
					t.Errorf("saveDataRecordInTx() saved er got = %+v, want %+v", got, tt.want)
				}
			}
		})

		toolTestDestroyDb(path, db)
	}
}

func Test_getOrderIdxInTx(t *testing.T) {
	type setup struct {
		initVal  *orderIdx
		closedTx bool
	}
	tests := []struct {
		name    string
		setup   setup
		want    *orderIdx
		wantErr bool
	}{
		{
			"Missing idx in db",
			setup{},
			nil,
			true,
		},
		{
			"Error load idx",
			setup{
				closedTx: true,
			},
			nil,
			true,
		},
		{
			"Empty idx",
			setup{
				initVal: &orderIdx{},
			},
			&orderIdx{
				Refs: map[string]string{},
			},
			false,
		},
		{
			"Load idx",
			setup{
				initVal: &orderIdx{
					Order: []string{"id-1"},
					Refs:  map[string]string{"id-1": "id-1"},
				},
			},
			&orderIdx{
				Order: []string{"id-1"},
				Refs:  map[string]string{"id-1": "id-1"},
			},
			false,
		},
	}
	for _, tt := range tests {
		var bucket string
		initStatusKeyVal := make(map[string][]byte, 1)
		if tt.setup.initVal != nil {
			bucket = idxBucket
			bs, _ := tt.setup.initVal.serialize()
			initStatusKeyVal[string(idxKey)] = bs
		}
		path, db := toolTestNewDb(bucket, initStatusKeyVal)

		t.Run(tt.name, func(t *testing.T) {

			err := db.Update(func(tx *nutsdb.Tx) error {
				if tt.setup.closedTx {
					tx.Commit()
				}

				got, err := getOrderIdxInTx(tx)
				if (err != nil) != tt.wantErr {
					t.Errorf("getOrderIdxInTx() error = %v, wantErr %v", err, tt.wantErr)
					return err
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("getOrderIdxInTx() = %v, want %v", got, tt.want)
				}

				return nil
			})

			if err != nil {
				return
			}
		})

		toolTestDestroyDb(path, db)
	}
}

func Test_removeFromIdxInTx(t *testing.T) {
	type setup struct {
		initVal  *orderIdx
		closedTx bool
	}
	type args struct {
		oi                *orderIdx
		pos               int
		metricToIncrement []byte
	}
	tests := []struct {
		name    string
		setup   setup
		args    args
		want    *orderIdx
		wantErr bool
	}{
		{
			"Pos less than 0",
			setup{},
			args{
				oi:                nil,
				pos:               -1,
				metricToIncrement: nil,
			},
			nil,
			true,
		},
		{
			"Pos out of bounds",
			setup{},
			args{
				oi:                &orderIdx{},
				pos:               2,
				metricToIncrement: nil,
			},
			nil,
			true,
		},
		{
			"Error saving metric out of bounds",
			setup{
				closedTx: true,
			},
			args{
				oi: &orderIdx{
					Order: []string{"id-1"},
					Refs:  map[string]string{"id-1": "id-1"},
				},
				pos:               0,
				metricToIncrement: []byte("test-increment-value"),
			},
			nil,
			true,
		},
		{
			"Error saving idx",
			setup{
				closedTx: true,
			},
			args{
				oi: &orderIdx{
					Order: []string{"id-1"},
					Refs:  map[string]string{"id-1": "id-1"},
				},
				pos:               0,
				metricToIncrement: nil,
			},
			nil,
			true,
		},
		{
			"Update idx",
			setup{
				initVal: &orderIdx{
					Order: []string{"id-3"},
					Refs:  map[string]string{"id-3": "id-3"},
				},
			},
			args{
				oi: &orderIdx{
					Order: []string{"id-1", "id-2"},
					Refs: map[string]string{
						"id-1": "id-1",
						"id-2": "id-2",
					},
				},
				pos:               0,
				metricToIncrement: nil,
			},
			&orderIdx{
				Order: []string{"id-2"},
				Refs:  map[string]string{"id-2": "id-2"},
			},
			false,
		},
	}
	for _, tt := range tests {
		var bucket string
		initStatusKeyVal := make(map[string][]byte, 1)
		if tt.setup.initVal != nil {
			bucket = idxBucket
			bs, _ := tt.setup.initVal.serialize()
			initStatusKeyVal[string(idxKey)] = bs
		}
		path, db := toolTestNewDb(bucket, initStatusKeyVal)

		t.Run(tt.name, func(t *testing.T) {

			err := db.Update(func(tx *nutsdb.Tx) error {
				if tt.setup.closedTx {
					tx.Commit()
				}

				if err := removeFromIdxInTx(tx, tt.args.oi, tt.args.pos, tt.args.metricToIncrement); (err != nil) != tt.wantErr {
					t.Errorf("removeFromIdxInTx() error = %v, wantErr %v", err, tt.wantErr)
				}

				return nil
			})

			if err != nil {
				return
			}

			if tt.want != nil {
				var idxInDb *orderIdx
				err := db.View(func(tx *nutsdb.Tx) error {
					var errTx error
					idxInDb, errTx = getOrderIdxInTx(tx)
					return errTx
				})
				if err != nil {
					t.Errorf("removeFromIdxInTx() error when load saved idx to validate value: %v", err)
					return
				}
				if !reflect.DeepEqual(idxInDb, tt.want) {
					t.Errorf("removeFromIdxInTx() idx saved = %+v, want %+v", idxInDb, tt.want)
				}
			}

		})

		toolTestDestroyDb(path, db)
	}
}

func Test_removeFirstRecordInTx(t *testing.T) {
	type args struct {
		oi                *orderIdx
		metricToIncrement []byte
	}
	type setup struct {
		closedTx bool
		initVals map[string]map[string][]byte
	}
	tests := []struct {
		name  string
		setup setup
		args  args
		want  *orderIdx
		// noWantKeys is a list of bucket, key tuples that should not be present after apply
		// removeLastRecordInTx. If len() is equal to 0 any check will be done
		noWantKeys [][]string
		// noWantKeys is a list of bucket, key tuples that should be present after apply
		// removeLastRecordInTx. If len() is equal to 0 any check will be done
		wantKeys [][]string
		wantErr  bool
	}{
		{
			"Empty index",
			setup{},
			args{&orderIdx{}, nil},
			nil,
			nil,
			nil,
			false,
		},
		{
			"Error delete record",
			setup{
				closedTx: true,
			},
			args{
				&orderIdx{
					Order: []string{"id-1"},
					Refs:  map[string]string{"id-1": "id-1"},
				},
				nil,
			},
			nil,
			nil,
			nil,
			true,
		},
		{
			"Delete the one",
			setup{
				closedTx: false,
				initVals: map[string]map[string][]byte{
					dataBucket: {
						"id-1": func() []byte {
							var er *EventRecord
							r, _ := er.Serialize()
							return r
						}(),
					},
				},
			},
			args{
				&orderIdx{
					Order: []string{"id-1"},
					Refs:  map[string]string{"id-1": "id-1"},
				},
				nil,
			},
			&orderIdx{Order: make([]string, 0), Refs: make(map[string]string, 0)},
			[][]string{
				{dataBucket, "id-1"},
			},
			nil,
			false,
		},
		{
			"Delete first",
			setup{
				closedTx: false,
				initVals: map[string]map[string][]byte{
					dataBucket: {
						"id-1": func() []byte {
							var er *EventRecord
							r, _ := er.Serialize()
							return r
						}(),
						"id-2": func() []byte {
							var er *EventRecord
							r, _ := er.Serialize()
							return r
						}(),
					},
				},
			},
			args{
				&orderIdx{
					Order: []string{"id-1", "id-2"},
					Refs: map[string]string{
						"id-1": "id-1",
						"id-2": "id-2",
					},
				},
				nil,
			},
			&orderIdx{Order: []string{"id-2"}, Refs: map[string]string{"id-2": "id-2"}},
			[][]string{
				{dataBucket, "id-1"},
			},
			[][]string{
				{dataBucket, "id-2"},
			},
			false,
		},
		{
			"Missing id in data",
			setup{
				closedTx: false,
				initVals: map[string]map[string][]byte{
					dataBucket: {
						"id-2": func() []byte {
							var er *EventRecord
							r, _ := er.Serialize()
							return r
						}(),
					},
				},
			},
			args{
				&orderIdx{
					Order: []string{"id-1", "id-2"},
					Refs: map[string]string{
						"id-1": "id-1",
						"id-2": "id-2",
					},
				},
				nil,
			},
			&orderIdx{
				Order: []string{"id-2"},
				Refs:  map[string]string{"id-2": "id-2"},
			},
			nil,
			[][]string{
				{dataBucket, "id-2"},
			},
			false,
		},
		{
			"Missing id with inconsistent index",
			setup{
				closedTx: false,
				initVals: map[string]map[string][]byte{
					dataBucket: {
						"id-2": func() []byte {
							var er *EventRecord
							r, _ := er.Serialize()
							return r
						}(),
					},
					idxBucket: {
						string(idxKey): func() []byte {
							idx := &orderIdx{
								Order: []string{"id-2"},
								Refs:  map[string]string{"id-2": "id-2"},
							}
							r, _ := idx.serialize()
							return r
						}(),
					},
				},
			},
			args{
				&orderIdx{
					Order: []string{"id-1"},
					Refs:  map[string]string{"id-1": "id-1"},
				},
				nil,
			},
			// Previous state in status is ignored because it was not aligned
			// with idx passed as parameter
			&orderIdx{
				Order: []string{},
				Refs:  map[string]string{},
			},
			nil,
			[][]string{
				{dataBucket, "id-2"},
			},
			false,
		},
	}
	for _, tt := range tests {

		path, db := toolTestNewDb("", nil)
		toolTestDbInitialData(db, tt.setup.initVals)

		t.Run(tt.name, func(t *testing.T) {

			err := db.Update(func(tx *nutsdb.Tx) error {
				if tt.setup.closedTx {
					tx.Commit()
				}

				if err := removeFirstRecordInTx(tx, tt.args.oi, tt.args.metricToIncrement); (err != nil) != tt.wantErr {
					t.Errorf("removeLastRecordInTx() error = %v, wantErr %v", err, tt.wantErr)
					return err
				}

				return nil
			})

			if err != nil {
				return
			}

			if tt.want != nil {
				var idxInDb *orderIdx
				err = db.View(func(tx *nutsdb.Tx) error {
					var errTx error
					idxInDb, errTx = getOrderIdxInTx(tx)
					return errTx
				})
				if err != nil {
					t.Errorf("removeLastRecordInTx() error when load saved idx to validate value: %v", err)
					return
				}
				if !reflect.DeepEqual(idxInDb, tt.want) {
					t.Errorf("removeLastRecordInTx() idx saved = %+v, want %+v", idxInDb, tt.want)
				}
			}

			if len(tt.noWantKeys) > 0 {
				for _, vals := range tt.noWantKeys {
					bucket, key := vals[0], []byte(vals[1])
					if toolTestExistKey(db, bucket, key) {
						t.Errorf("removeLastRecordInTx() key %s should not be in bucket %s but still exists", vals[1], bucket)
					}
				}
			}

			if len(tt.wantKeys) > 0 {
				for _, vals := range tt.wantKeys {
					bucket, key := vals[0], []byte(vals[1])
					if !toolTestExistKey(db, bucket, key) {
						t.Errorf("removeLastRecordInTx() key %s should be present in bucket %s but was not found", vals[1], bucket)
					}
				}
			}

		})

		toolTestDestroyDb(path, db)
	}
}

func Test_orderIdx_remove(t *testing.T) {
	type args struct {
		pos int
	}
	tests := []struct {
		name string
		oi   *orderIdx
		args args
		want *orderIdx
	}{
		{
			"Empty",
			&orderIdx{},
			args{128},
			&orderIdx{},
		},
		{
			"Pos less than 0",
			&orderIdx{
				Order: []string{"id-1"},
				Refs: map[string]string{
					"id-1": "id-1",
				},
			},
			args{-1},
			&orderIdx{
				Order: []string{"id-1"},
				Refs: map[string]string{
					"id-1": "id-1",
				},
			},
		},
		{
			"Pos greater than elements number",
			&orderIdx{
				Order: []string{"id-1"},
				Refs: map[string]string{
					"id-1": "id-1",
				},
			},
			args{23},
			&orderIdx{
				Order: []string{"id-1"},
				Refs: map[string]string{
					"id-1": "id-1",
				},
			},
		},
		{
			"Deleting",
			&orderIdx{
				Order: []string{"id-1"},
				Refs: map[string]string{
					"id-1": "id-2",
				},
			},
			args{0},
			&orderIdx{
				Order: []string{},
				Refs:  map[string]string{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.oi.remove(tt.args.pos)
			if !reflect.DeepEqual(tt.oi, tt.want) {
				t.Errorf("oriderIdx.remove(): got %v, want %v", tt.oi, tt.want)
			}
		})
	}
}

func Test_orderIdx_set(t *testing.T) {
	type args struct {
		oldID string
		newID string
	}
	tests := []struct {
		name string
		oi   *orderIdx
		args args
		want *orderIdx
	}{
		{
			"Empty",
			&orderIdx{},
			args{"old-one", "new-one"},
			&orderIdx{},
		},
		{
			"OldID does not exist",
			&orderIdx{
				Order: []string{"id-1"},
				Refs: map[string]string{
					"id-1": "id-1",
				},
			},
			args{"old-one", "new-one"},
			&orderIdx{
				Order: []string{"id-1"},
				Refs: map[string]string{
					"id-1": "id-1",
				},
			},
		},
		{
			"Replacing",
			&orderIdx{
				Order: []string{"id-1"},
				Refs: map[string]string{
					"id-1": "id-2",
				},
			},
			args{"id-1", "new-one"},
			&orderIdx{
				Order: []string{"new-one"},
				Refs: map[string]string{
					"new-one": "id-2",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.oi.set(tt.args.oldID, tt.args.newID)
			if !reflect.DeepEqual(tt.oi, tt.want) {
				t.Errorf("oriderIdx.set(): got %v, want %v", tt.oi, tt.want)
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
		path, db := toolTestNewDb(tt.args.bucket, tt.existingKeys)

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
				if !toolTestAssertKeyVal(db, tt.args.bucket, tt.args.key, expectedValue) {
					t.Errorf("inc() for key %s want value %s", string(tt.args.key), tt.want)
				}
			}
		})

		toolTestDestroyDb(path, db)
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
		path, db := toolTestNewDb(tt.args.bucket, tt.existingKeys)

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

		toolTestDestroyDb(path, db)
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
		path, db := toolTestNewDb(tt.args.bucket, tt.args.existingKeys)

		t.Run(tt.name, func(t *testing.T) {
			db.Update(func(tx *nutsdb.Tx) error {
				if err := del(tx, tt.args.bucket, tt.args.key); (err != nil) != tt.wantErr {
					t.Errorf("del() error = %v, wantErr %v", err, tt.wantErr)
				}
				return nil
			})

			if toolTestExistKey(db, tt.args.bucket, tt.args.key) {
				t.Errorf("del() key %s exists in db", string(tt.args.key))
			}
		})

		toolTestDestroyDb(path, db)
	}
}

func TestEventRecord_Serialize(t *testing.T) {
	tests := []struct {
		name    string
		er      *EventRecord
		want    []byte
		wantErr bool
	}{
		{
			"Nil",
			nil,
			[]byte{192},
			false,
		},
		{
			"Empty",
			&EventRecord{},
			func() []byte {
				r, _ := msgpack.Marshal(&EventRecord{})
				return r
			}(),
			false,
		},
		{
			"Full",
			&EventRecord{
				AsyncIDs:   []string{"id-1", "id-2"},
				Timestamp:  time.Time{}.Add(time.Second),
				Tag:        "test.keep.free",
				Msg:        "the event content",
				Compressor: &devosender.Compressor{Algorithm: devosender.CompressorGzip},
				LastError:  errors.New("Test error"),
			},
			func() []byte {
				er := &EventRecord{
					AsyncIDs:   []string{"id-1", "id-2"},
					Timestamp:  time.Time{}.Add(time.Second),
					Tag:        "test.keep.free",
					Msg:        "the event content",
					Compressor: &devosender.Compressor{Algorithm: devosender.CompressorGzip},
					LastError:  errors.New("Test error"),
				}
				r, _ := msgpack.Marshal(er)
				return r
			}(),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.er.Serialize()
			if (err != nil) != tt.wantErr {
				t.Errorf("EventRecord.Serialize() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EventRecord.Serialize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEventRecord_EffectiveID(t *testing.T) {
	tests := []struct {
		name string
		er   *EventRecord
		want string
	}{
		{
			"Nil AsyncIDs",
			&EventRecord{},
			"",
		},
		{
			"Empty AsyncIDs",
			&EventRecord{AsyncIDs: []string{}},
			"",
		},
		{
			"With AsyncIDs",
			&EventRecord{AsyncIDs: []string{"id-1", "id-2"}},
			"id-2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.er.EffectiveID(); got != tt.want {
				t.Errorf("EventRecord.EffectiveID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_IsNotFoundErr(t *testing.T) {
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
			"err bucket",
			args{errors.New("err bucket")},
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
			if got := IsNotFoundErr(tt.args.err); got != tt.want {
				t.Errorf("IsNotFoundErr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func toolTestNewDb(initValBucket string, initVals map[string][]byte) (string, *nutsdb.DB) {
	return toolTestNewDbWithOpts(initValBucket, initVals, nutsdb.DefaultOptions)
}

func toolTestNewDbWithOpts(initValBucket string, initVals map[string][]byte, opts nutsdb.Options) (string, *nutsdb.DB) {
	path := toolsTestNewTempDirName()

	opts.Dir = path
	db, err := nutsdb.Open(opts)
	if err != nil {
		panic(err)
	}

	// Add data
	if initValBucket != "" && len(initVals) > 0 {
		toolTestDbInitialData(
			db,
			map[string]map[string][]byte{
				initValBucket: initVals,
			},
		)
	}

	return path, db
}

func toolsTestNewTempDirName() string {
	return fmt.Sprintf("%s%cdevosender_status-test-%d", os.TempDir(), os.PathSeparator, rand.Int())
}

// Add data to opened nutsdb.
// data format is a map which key is the bucket and the value is the second level data map
// these second data map format is: key is the key to save and value ([]byte) is de value
// to save in parent bucket
func toolTestDbInitialData(db *nutsdb.DB, data map[string]map[string][]byte) {
	if len(data) > 0 {
		err := db.Update(func(tx *nutsdb.Tx) error {
			for bucket, vals := range data {
				for k, v := range vals {
					err := tx.Put(bucket, []byte(k), v, 0)
					if err != nil {
						return err
					}
				}
			}
			return nil
		})

		if err != nil {
			panic(err)
		}
	}
}

func toolTestAssertKeyVal(db *nutsdb.DB, bucket string, key []byte, val []byte) bool {
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

func toolTestExistKey(db *nutsdb.DB, bucket string, key []byte) bool {
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

func toolTestDestroyDb(path string, db *nutsdb.DB) {
	db.Close()
	os.RemoveAll(path)
}

func TestSorteableStringTime_Swap(t *testing.T) {
	type fields struct {
		Values     []string
		Timestamps []time.Time
	}
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *SorteableStringTime
	}{
		{
			"Swap elements minor first",
			fields{
				[]string{"b", "a"},
				func() []time.Time {
					r := make([]time.Time, 2)
					var t time.Time
					r[0] = t.Add(time.Second * 2)
					r[1] = t.Add(time.Second)
					return r
				}(),
			},
			args{0, 1},
			&SorteableStringTime{
				Values: []string{"a", "b"},
				Timestamps: func() []time.Time {
					r := make([]time.Time, 2)
					var t time.Time
					r[0] = t.Add(time.Second)
					r[1] = t.Add(time.Second * 2)
					return r
				}(),
			},
		},
		{
			"Swap elements major first",
			fields{
				[]string{"b", "a"},
				func() []time.Time {
					r := make([]time.Time, 2)
					var t time.Time
					r[0] = t.Add(time.Second * 2)
					r[1] = t.Add(time.Second)
					return r
				}(),
			},
			args{1, 0},
			&SorteableStringTime{
				Values: []string{"a", "b"},
				Timestamps: func() []time.Time {
					r := make([]time.Time, 2)
					var t time.Time
					r[0] = t.Add(time.Second)
					r[1] = t.Add(time.Second * 2)
					return r
				}(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sst := &SorteableStringTime{
				Values:     tt.fields.Values,
				Timestamps: tt.fields.Timestamps,
			}
			sst.Swap(tt.args.i, tt.args.j)
			if !reflect.DeepEqual(sst, tt.want) {
				t.Errorf("SorteableStringTime.Swap() = %+v, want %+v", sst, tt.want)
			}
		})
	}
}
