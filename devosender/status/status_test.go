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
