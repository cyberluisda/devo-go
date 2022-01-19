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
	path := fmt.Sprintf("%s%cdevosender_status-test-%d", os.TempDir(), os.PathSeparator, rand.Int())

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
