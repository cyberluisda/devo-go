package status

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/cyberluisda/devo-go/devosender"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/xujiajun/nutsdb"
)

// Builder is the abstrastraction of any Status Builder implementation
type Builder interface {
	Build() (Status, error)
}

// NutsDBStatusBuilder represents the Builder for NutsDBstatus
type NutsDBStatusBuilder struct {
	dbOpts                             nutsdb.Options
	eventsTTLSeconds                   uint32
	bufferSize                         uint
	filesToConsolidateDb               int
	recreateDbClientAfterConsolidation bool
}

const (
	// DefaultEventsTTLSeconds is the default value for EventsTTLSeconds used by NewSNutsDBStatusBuilder
	DefaultEventsTTLSeconds uint32 = 3600
	// DefaultBufferSize is the default size of the total events buffer manged by Status
	// implementation
	DefaultBufferSize uint = 5000000
	// DefaultFilesToConsolidateDb is the default value used when NutsDBStatusBuilder.FilesToConsolidateDb
	// is not called or not set
	DefaultFilesToConsolidateDb = 5
	// DefaultRecreateDbClientAfterConsolidation is the default value used when
	// NutsDBStatusBuilder.RecreateDbClientAfterConsolidation is not called
	DefaultRecreateDbClientAfterConsolidation = true
)

// NewSNutsDBStatusBuilder is the factory method for instantiate *NutsDBStatusBuilder with
// default values
func NewSNutsDBStatusBuilder() *NutsDBStatusBuilder {
	return &NutsDBStatusBuilder{
		nutsdb.DefaultOptions,
		DefaultEventsTTLSeconds,
		DefaultBufferSize,
		DefaultFilesToConsolidateDb,
		DefaultRecreateDbClientAfterConsolidation,
	}
}

// EventsTTLSeconds sets the EventsTTLSeconds builder value
func (nsb *NutsDBStatusBuilder) EventsTTLSeconds(ttl uint32) *NutsDBStatusBuilder {
	nsb.eventsTTLSeconds = ttl
	return nsb
}

// NutsDBOptions sets the nutsdb.Options builder value
func (nsb *NutsDBStatusBuilder) NutsDBOptions(opts nutsdb.Options) *NutsDBStatusBuilder {
	nsb.dbOpts = opts
	return nsb
}

// DbPath sets the Dir property in nutsdb.Options builder value
func (nsb *NutsDBStatusBuilder) DbPath(path string) *NutsDBStatusBuilder {
	nsb.dbOpts.Dir = path
	return nsb
}

// DbSegmentSize sets the SegmentSize (Maximum size for each status persisted file)
// property in nutsdb.Options builder value:
func (nsb *NutsDBStatusBuilder) DbSegmentSize(size int64) *NutsDBStatusBuilder {
	nsb.dbOpts.SegmentSize = size
	return nsb
}

// DbEntryIdxMode sets the EntryIdxMode (See https://pkg.go.dev/github.com/xujiajun/nutsdb@v0.6.0#section-readme)
// property in nutsdb.Options builder value
func (nsb *NutsDBStatusBuilder) DbEntryIdxMode(mode nutsdb.EntryIdxMode) *NutsDBStatusBuilder {
	nsb.dbOpts.EntryIdxMode = mode
	return nsb
}

// DbRWMode sets the RWMode and StartFileLoadingMode (Database read-write mode,
// see https://pkg.go.dev/github.com/xujiajun/nutsdb@v0.6.0#section-readme) properties
// in nutsdb.Options builder value
func (nsb *NutsDBStatusBuilder) DbRWMode(mode nutsdb.RWMode) *NutsDBStatusBuilder {
	nsb.dbOpts.RWMode = mode
	nsb.dbOpts.StartFileLoadingMode = mode
	return nsb
}

// BufferSize sets the BufferSize builder value
func (nsb *NutsDBStatusBuilder) BufferSize(size uint) *NutsDBStatusBuilder {
	nsb.bufferSize = size
	return nsb
}

// FilesToConsolidateDb sets the minimum number of files threshold to run consolidate
// nutsdb files (Merge) when HouseKeeping func is called.
// files value will be internally assigned only if it is greater than 1
func (nsb *NutsDBStatusBuilder) FilesToConsolidateDb(files int) *NutsDBStatusBuilder {
	if files >= 2 {
		nsb.filesToConsolidateDb = files
	}
	return nsb
}

// RecreateDbClientAfterConsolidation enable the close and open nutsdb client when a
// consolidation operation (Merge) is done during HouseKeeping call. This feature
// prevents some memory leaks related with internal nutsdb objects management.
func (nsb *NutsDBStatusBuilder) RecreateDbClientAfterConsolidation(b bool) *NutsDBStatusBuilder {
	nsb.recreateDbClientAfterConsolidation = b
	return nsb
}

// Build builds NutsDBStatus instance based on Builder config.
// NutsDBStatus.Initialize is called just after build connection
func (nsb *NutsDBStatusBuilder) Build() (*NutsDBStatus, error) {
	if nsb.dbOpts.Dir == "" {
		return nil, fmt.Errorf("Empty Dir in nutsdb.Opts")
	}

	// Open nutsdb
	db, err := nutsdb.Open(nsb.dbOpts)
	if err != nil {
		return nil, fmt.Errorf("While open nutsdb: %w", err)
	}

	r := &NutsDBStatus{
		db:                                 db,
		dbOpts:                             nsb.dbOpts,
		eventTTL:                           nsb.eventsTTLSeconds,
		bufferSize:                         nsb.bufferSize,
		filesToConsolidateDb:               nsb.filesToConsolidateDb,
		recreateDbClientAfterConsolidation: nsb.recreateDbClientAfterConsolidation,
	}

	err = r.Initialize()
	if err != nil {
		return r, fmt.Errorf("While initialize NutsDBStatus: %w", err)
	}
	return r, nil
}

// Status is the engine abstranction to save and retrieve EventRecord instances
// from status implementation.
type Status interface {
	New(er *EventRecord) error
	Update(oldID, newID string) error
	Get(ID string) (*EventRecord, int, error)
	FinishRecord(ID string) error
	AllIDs() ([]string, error)
	FindAll() ([]*EventRecord, error)
	Stats() Stats
	HouseKeeping() error
	Close() error
}

// NutsDBStatus is the nutsdb implementation of Status interface
type NutsDBStatus struct {
	db                                 *nutsdb.DB
	dbOpts                             nutsdb.Options
	dbMtx                              sync.Mutex
	initialized                        bool
	eventTTL                           uint32
	bufferSize                         uint
	filesToConsolidateDb               int
	recreateDbClientAfterConsolidation bool
}

type orderIdx struct {
	Order []string
	Refs  map[string]string
}

func getOrderIdxInTx(tx *nutsdb.Tx) (*orderIdx, error) {
	raw, err := tx.Get(idxBucket, idxKey)
	if err != nil {
		return nil, fmt.Errorf("While load %s.%s: %w", idxBucket, string(idxKey), err)
	}

	r, err := unSerialzeOrderIdx(raw.Value)
	if r.Refs == nil {
		r.Refs = map[string]string{}
	}
	if err != nil {
		return r, fmt.Errorf("While unserialize value of %s.%s: %w", idxBucket, string(idxKey), err)
	}

	return r, nil
}

func saveOrderIdxInTx(tx *nutsdb.Tx, od *orderIdx) error {
	raw, err := od.serialize()
	if err != nil {
		return fmt.Errorf("While serialize orderIdx: %w", err)
	}

	err = tx.Put(idxBucket, idxKey, raw, 0)
	if err != nil {
		return fmt.Errorf("While save value to %s.%s: %w", idxBucket, string(idxKey), err)
	}

	return nil
}

func removeFromIdxInTx(tx *nutsdb.Tx, oi *orderIdx, pos int, metricToIncrement []byte) error {
	if pos < 0 || pos > len(oi.Order)-1 {
		return fmt.Errorf("Pos is out of bounds")
	}

	if metricToIncrement != nil {
		err := inc(tx, statsBucket, metricToIncrement, 1, false)
		if err != nil {
			return fmt.Errorf("While increment %s.%s: %w", statsBucket, string(metricToIncrement), err)
		}
	}
	oi.remove(pos)

	err := saveOrderIdxInTx(tx, oi)
	if err != nil {
		return fmt.Errorf("While save index: %w", err)
	}
	return nil
}

func removeLastRecordInTx(tx *nutsdb.Tx, oi *orderIdx, metricToIncrement []byte) error {
	if len(oi.Order) == 0 {
		return nil
	}

	// Get last ID
	ID := oi.Order[len(oi.Order)-1]
	err := deleteDataRecordInTx(tx, []byte(ID))
	if err != nil {
		return fmt.Errorf("While delete data record with id %s: %w", ID, err)
	}

	err = removeFromIdxInTx(tx, oi, len(oi.Order)-1, metricToIncrement)
	if err != nil {
		return fmt.Errorf("While remove references of %s id from index: %w", ID, err)
	}
	return nil
}

func unSerialzeOrderIdx(raw []byte) (*orderIdx, error) {
	r := orderIdx{}
	err := msgpack.Unmarshal(raw, &r)
	return &r, err
}

func (oi *orderIdx) serialize() ([]byte, error) {
	v, err := msgpack.Marshal(oi)

	return v, err
}

func (oi *orderIdx) indexOf(s string) int {
	r := -1
	if len(oi.Order) == 0 {
		return r
	}
	for i, v := range oi.Order {
		if v == s {
			r = i
			break
		}
	}
	return r
}

func (oi *orderIdx) remove(pos int) {
	if len(oi.Order) == 0 || pos < 0 || pos > len(oi.Order)-1 {
		return
	}

	// Delte references
	ID := oi.Order[pos]
	toRemove := make([]string, 1)
	toRemove[0] = ID

	// Inverse references
	for k, v := range oi.Refs {
		if v == ID {
			toRemove = append(toRemove, k)
		}
	}

	for _, v := range toRemove {
		delete(oi.Refs, v)
	}

	// Remove from order idx
	tmp := oi.Order[:pos]
	tmp = append(tmp, oi.Order[pos+1:]...)
	oi.Order = nil
	oi.Order = tmp
}

func (oi *orderIdx) add(ID string) {
	oi.Order = append(oi.Order, ID)
	oi.Refs[ID] = ID
}

func (oi *orderIdx) set(oldID, newID string) {
	pos := oi.indexOf(oldID)
	if pos == -1 {
		return
	}

	oi.Order[pos] = newID
	// Pointing newID to original ID
	oi.Refs[newID] = oi.Refs[oldID]
	// Removing old reference
	delete(oi.Refs, oldID)
}

func (oi *orderIdx) reset(capacity int) {
	if capacity < 1 {
		oi.Order = nil
		oi.Refs = map[string]string{}
	} else {
		oi.Order = make([]string, capacity)
		oi.Refs = make(map[string]string, capacity)
	}
}

// inc increments the integer value of a key. If key exists value should be transformed using
// strvconv.Atoi(string(value)) before increments the value. If key does not exist it will create
// with n value unless errorIfNotFound parameter is true. On this case it returns an error.
// WARNING. inc is not prepare to be called more than one time on same transaction. To solve this
// problem use set func instead.
func inc(tx *nutsdb.Tx, bucket string, key []byte, n int, errorIfNotFound bool) error {
	if n == 0 {
		return nil
	}

	ve, err := tx.Get(bucket, key)
	if IsNotFoundErr(err) {
		if errorIfNotFound {
			return err
		}
		err = tx.Put(bucket, key, []byte(fmt.Sprint(n)), 0)
	} else {
		v, _ := strconv.Atoi(string(ve.Value))
		v = v + n
		err = tx.Put(bucket, key, []byte(strconv.Itoa(v)), 0)
	}

	return err
}

// cont returns the interger value of a key. If key exists value should be transformed using
// strvconv.Atoi(string(value)). If key does not exist it will return 0 value unless errorIfNotFound
// parameter is true. On this case it returns an error.
// WARNING. con is not prepare to be called more than one time in the same transaction. You should
// solve it loading the value at the begining of the transcation and maintain internally updated.
func cont(tx *nutsdb.Tx, bucket string, key []byte, errorIfNotFound bool) (int, error) {
	ve, err := tx.Get(bucket, key)
	if IsNotFoundErr(err) {
		if errorIfNotFound {
			return 0, err
		}
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(string(ve.Value))
}

// set sets the integer value of a key. Value is transformed using strvconv.Atoi(string(value)).
// This method should be used when you need inc or dec one value more than one time in
// same transacion. For example
// Use:
// n,_ := cont(tx, "bucket", []byte("key"))
// ...
// n++
// n++
// ...
// n--
// set(tx, "bucket", []byte("key"), n)
// Instead of:
// n,_ := cont(tx, "bucket", []byte("key"))
// ...
// inc(tx, "bucket", []byte("key"), 1, false)
// inc(tx, "bucket", []byte("key"), 1, false)
// ...
// dec(tx, "bucket", []byte("key"), 1, false)
func set(tx *nutsdb.Tx, bucket string, key []byte, v int) error {
	vStr := strconv.Itoa(v)
	return tx.Put(bucket, key, []byte(vStr), 0)
}

// del remove a key. Usefull alias of tx.Delete when you are working with inc, dec, cont and set
func del(tx *nutsdb.Tx, bucket string, key []byte) error {
	return tx.Delete(bucket, key)
}

// Stats represent the counter ando other metric values extracted from status db
// implementation
type Stats struct {
	// Number of events in buffer
	BufferCount int
	// UPdaTotal events that are in buffer and daemon was tried to re-send
	Updated int
	// Finished is the total number of events that were processed (out of buffer)
	Finished int
	// Dropped is the total number of events that were removed from buffer without send because
	// limit of the buffer size was reached
	Dropped int
	// Evicted is the total number of events that were removed from buffer because they were expired
	// before stablish connection
	Evicted int

	// DbIdxSize is the number of IDs saved in the ordered index
	DbIdxSize int
	// DbMaxFileID is the file number(id) used by status db
	DbMaxFileID int64
	// DbDataEntries is the number of status records saved on the status db, or -1 if this metric was
	// not solved. Resolution of this metric seriously affects the performance. For this reason this metric
	// will only resolve it if value of DEVOGO_DEBUG_SENDER_STATS_COUNT_DATA environment varaiblable is "yes"
	DbDataEntries int
}

// EventRecord is the record to save and retrieve events in/from status
// The events are the messages that can be send used the client to Devo
type EventRecord struct {
	AsyncIDs   []string
	Timestamp  time.Time
	Tag        string
	Msg        string
	Compressor *devosender.Compressor
	LastError  error
}

// Serialize transforms EventRecord to byte slice
func (er *EventRecord) Serialize() ([]byte, error) {
	r, err := msgpack.Marshal(er)
	if err != nil {
		return nil, fmt.Errorf("While serialize record: %w", err)
	}
	return r, nil
}

// EffectiveID return the last value of ID, that is the updated value of the ID
// in EventRecord
func (er *EventRecord) EffectiveID() string {
	if len(er.AsyncIDs) == 0 {
		return ""
	}
	return er.AsyncIDs[len(er.AsyncIDs)-1]
}

var reNotFoundError = regexp.MustCompile(`^not found bucket:.*,key:.*$`)

// IsNotFoundErr check and return if error parameter is one of the "Not found"
// recognized errors returned by nutsdb operations.
func IsNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	if err == nutsdb.ErrBucketNotFound {
		return true
	}
	if err == nutsdb.ErrBucketEmpty {
		return true
	}
	if err == nutsdb.ErrNotFoundKey {
		return true
	}
	if err == nutsdb.ErrKeyNotFound {
		return true
	}
	errStr := err.Error()
	if errStr == "err bucket" {
		return true
	}
	if errStr == "key not exits" {
		return true
	}
	if errStr == "item not exits" {
		return true
	}

	return reNotFoundError.MatchString(errStr)
}

// NumberOfFiles return the number of files, really non directories elements, in
// a directory path without entering in sub-directories
func NumberOfFiles(path string) int {
	files, _ := ioutil.ReadDir(path)
	r := 0
	for _, f := range files {
		if !f.IsDir() {
			r++
		}
	}
	return r
}

// SorteableStringTime represent a slice of string, time.Time tuples that can be
// sorted by time.Time value using sort.Sort() method
type SorteableStringTime struct {
	Values     []string
	Timestamps []time.Time
}

// Add add new (string, time) tuple
func (sst *SorteableStringTime) Add(v string, t time.Time) {
	sst.Values = append(sst.Values, v)
	sst.Timestamps = append(sst.Timestamps, t)
}

// Len is part of by sort.Interface
func (sst *SorteableStringTime) Len() int {
	return len(sst.Values)
}

// Less is part of by sort.Interface
func (sst *SorteableStringTime) Less(i, j int) bool {
	return sst.Timestamps[i].Before(sst.Timestamps[j])
}

// Swap is part of by sort.Interface
func (sst *SorteableStringTime) Swap(i, j int) {
	sst.Values[i], sst.Values[j] = sst.Values[j], sst.Values[i]
	sst.Timestamps[i], sst.Timestamps[j] = sst.Timestamps[j], sst.Timestamps[i]
}
