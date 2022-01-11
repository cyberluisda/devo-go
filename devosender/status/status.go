package status

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
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

const (
	dataBucket  = "data"
	idxBucket   = "idx"
	statsBucket = "stats"
)

var (
	idxKey      = []byte("idx")
	countKey    = []byte("count")
	updatedKey  = []byte("updated")
	evictedKey  = []byte("evicted")
	finishedKey = []byte("finished")
	droppedKey  = []byte("dropped")
)

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

var (
	// ErrRecordEvicted is the error returned when EventRecord was expired
	ErrRecordEvicted error = errors.New("EventRecord evicted")
	// ErrRecordNotFoundInIdx is the error returned when EventRecord was not found in the index
	ErrRecordNotFoundInIdx error = errors.New("EventRecord not found in index")
)

// Get is the Status.Get implementation for NutsDBStatus: Returns EventRecord based on ID
func (ns *NutsDBStatus) Get(ID string) (*EventRecord, int, error) {
	ns.dbMtx.Lock()
	defer ns.dbMtx.Unlock()

	var r *EventRecord
	var pos int
	evictedRecord := false
	err := ns.db.Update(func(tx *nutsdb.Tx) error {
		idx, err := getOrderIdxInTx(tx)
		if err != nil {
			return fmt.Errorf("While load order index: %w", err)
		}

		// Load position in order idx
		pos = idx.indexOf(ID)
		if pos == -1 {
			return ErrRecordNotFoundInIdx
		}

		// Get reference in idx
		origID := idx.Refs[ID]

		// Get register raw
		r, err = getDataRecordInTx(tx, []byte(origID))
		if IsNotFoundErr(err) {
			// Assuming record was expired
			err = removeFromIdxInTx(tx, idx, pos, evictedKey)
			if err != nil {
				return fmt.Errorf("While mark %s as evicted: %w", ID, err)
			}

			// Decrement buffer counter
			err = inc(tx, statsBucket, countKey, -1, true)
			if err != nil {
				return fmt.Errorf("While decrement buffer counter: %w", err)
			}

			r = nil
			pos = -1
			evictedRecord = true // Instaed of return return ErrRecordEvicted to prevent nutsdb rollback
		} else if err != nil {
			return fmt.Errorf("While load record: %w", err)
		}
		return nil
	})

	// Fix RecordEvicted error
	if evictedRecord && err == nil {
		err = ErrRecordEvicted
	}

	return r, pos, err
}

// FinishRecord is the Status.FinishRecord implementation for NutsDBStatus: Mark
// as record as finished and remove it from status
func (ns *NutsDBStatus) FinishRecord(ID string) error {
	ns.dbMtx.Lock()
	defer ns.dbMtx.Unlock()

	notFound := false
	err := ns.db.Update(func(tx *nutsdb.Tx) error {
		idx, err := getOrderIdxInTx(tx)
		if err != nil {
			return fmt.Errorf("While load index: %w", err)
		}

		// Check if record exists.
		pos := idx.indexOf(ID)
		notFound = pos == -1

		// Ensure is deleted from data
		err = deleteDataRecordInTx(tx, []byte(ID))
		if err != nil {
			return fmt.Errorf("While ensure record is removed from data")
		}
		if notFound {
			return nil
		}

		err = removeFromIdxInTx(tx, idx, pos, finishedKey)
		if err != nil {
			return fmt.Errorf("While remove form index: %w", err)
		}

		err = inc(tx, statsBucket, countKey, -1, true)
		if err != nil {
			return fmt.Errorf("While decrement %s.%s counter: %w", statsBucket, string(countKey), err)
		}

		return nil
	})

	if err == nil && notFound {
		err = ErrRecordNotFoundInIdx
	}
	return err
}

// AllIDs is the Status.AllIDs implementation for NutsDBStatus: Return all ids based
// on index information only
func (ns *NutsDBStatus) AllIDs() ([]string, error) {
	ns.dbMtx.Lock()
	defer ns.dbMtx.Unlock()

	// Load index
	var idx *orderIdx
	err := ns.db.View(func(tx *nutsdb.Tx) error {
		var err error
		idx, err = getOrderIdxInTx(tx)

		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return idx.Order, fmt.Errorf("While load order index: %w", err)
	}

	// Evicted events
	err = ns.db.Update(func(tx *nutsdb.Tx) error {

		// Look for expired
		posToRemove := make([]int, 0)
		for i, ID := range idx.Order {
			origID := []byte(idx.Refs[ID])
			_, err := tx.Get(dataBucket, origID)
			if IsNotFoundErr(err) {
				// Assuming was expired
				posToRemove = append(posToRemove, i)
			} else if err != nil {
				return fmt.Errorf("While look for %s.%s: %w", dataBucket, ID, err)
			}
		}

		// Clean expired
		if len(posToRemove) > 0 {
			for _, pos := range posToRemove {
				idx.remove(pos)
			}
			err := saveOrderIdxInTx(tx, idx)
			if err != nil {
				return fmt.Errorf("While update order index after clean expired events: %w", err)
			}

			err = inc(tx, statsBucket, evictedKey, len(posToRemove), false)
			if err != nil {
				return fmt.Errorf("While update evicted stat: %w", err)
			}
		}

		return nil
	})

	return idx.Order, err
}

// FindAll is the Status.FindAll implementation for NutsDBStatus: Return all EventRecords from status
// Bear in mind that this operation is heavy use resources.
// Order is not WARRANTIED, Use AllIDs with Get to get all recores in order
func (ns *NutsDBStatus) FindAll() ([]*EventRecord, error) {
	ns.dbMtx.Lock()
	defer ns.dbMtx.Unlock()

	var r []*EventRecord
	// First get all keys
	err := ns.db.Update(func(tx *nutsdb.Tx) error {
		entries, err := tx.GetAll(dataBucket)
		if err != nil && !IsNotFoundErr(err) {
			return fmt.Errorf("While load all entries from %s: %w", dataBucket, err)
		}

		// Un marshall all records
		r = make([]*EventRecord, len(entries))
		updatedIDsInRecords := make(map[string]interface{}, len(entries))
		errors := make([]string, 0)
		for i, raw := range entries {
			er := &EventRecord{}
			err = msgpack.Unmarshal(raw.Value, er)
			if err != nil {
				errors = append(errors, err.Error())
			}

			// save in result variable
			r[i] = er

			updatedID := er.EffectiveID()
			updatedIDsInRecords[updatedID] = nil
		}
		entries = nil // easy gc

		if len(errors) > 0 {
			return fmt.Errorf("Event record unseriallize errors: %s", strings.Join(errors, ", "))
		}

		// Fix expired in index
		idx, err := getOrderIdxInTx(tx)
		if err != nil {
			return fmt.Errorf("While load index: %w", err)
		}
		expiredIDs := make(map[string]interface{}, 0)
		for _, v := range idx.Order {
			if _, ok := updatedIDsInRecords[v]; !ok {
				// Assuming event was expired because it was not foudn in ids from records
				expiredIDs[v] = nil
			}
		}
		if len(expiredIDs) > 0 {
			// Load and update evicted stat value because we can not increment more than one
			// time in the same transaction. We will set evicted stats value later
			evictedStat, err := cont(tx, statsBucket, evictedKey, false)
			if err != nil {
				return fmt.Errorf("While load %s.%s: %w", statsBucket, string(evictedKey), err)
			}
			countStat, err := cont(tx, statsBucket, countKey, false)
			if err != nil {
				return fmt.Errorf("While load %s.%s: %w", statsBucket, string(countKey), err)
			}

			// Remove expired events from idx one by one
			for ID := range expiredIDs {
				i := idx.indexOf(ID)
				err = removeFromIdxInTx(tx, idx, i, nil)
				if err != nil {
					return fmt.Errorf("While remove expired %s ID (%d) from index: %w", ID, i, err)
				}
			}

			// Update stats
			err = set(tx, statsBucket, evictedKey, evictedStat+len(expiredIDs))
			if err != nil {
				return fmt.Errorf("While update %s.%s: %w", statsBucket, string(evictedKey), err)
			}
			err = set(tx, statsBucket, countKey, countStat-len(expiredIDs))
			if err != nil {
				return fmt.Errorf("While update %s.%s: %w", statsBucket, string(countKey), err)
			}
		}

		return nil
	})

	return r, err
}

// Stats return Stas generated by NutsDBStatus
func (ns *NutsDBStatus) Stats() Stats {
	ns.dbMtx.Lock()
	defer ns.dbMtx.Unlock()
	r := Stats{}

	// metrics from status db
	ns.db.View(func(tx *nutsdb.Tx) error {
		v, _ := cont(tx, statsBucket, countKey, true)
		r.BufferCount = v
		v, _ = cont(tx, statsBucket, droppedKey, true)
		r.Dropped = v
		v, _ = cont(tx, statsBucket, evictedKey, true)
		r.Evicted = v
		v, _ = cont(tx, statsBucket, finishedKey, true)
		r.Finished = v
		v, _ = cont(tx, statsBucket, updatedKey, true)
		r.Updated = v

		od, _ := getOrderIdxInTx(tx)
		r.DbIdxSize = len(od.Order)

		// high resources stats
		if v, ok := os.LookupEnv("DEVOGO_DEBUG_SENDER_STATS_COUNT_DATA"); ok && strings.ToLower(v) == "yes" {
			e, _ := tx.GetAll(dataBucket)
			r.DbDataEntries = len(e)
			e = nil // easy gc
		} else {
			r.DbDataEntries = -1
		}

		return nil
	})

	// other stats
	r.DbMaxFileID = ns.db.MaxFileID

	return r
}

// HouseKeeping the implementation of Status.HouseKeeping, It runs a set of tasks
// like consolidate status db, fix and regenerat index if needed, etc.
func (ns *NutsDBStatus) HouseKeeping() error {
	ns.dbMtx.Lock()
	defer ns.dbMtx.Unlock()

	// Consolidate nutsdb files
	merged := false
	nFiles := NumberOfFiles(ns.dbOpts.Dir)
	if nFiles >= ns.filesToConsolidateDb {
		err := ns.db.Merge()
		if err != nil {
			return fmt.Errorf("While consolidate (Merge) status files, # files=%d: %w", nFiles, err)
		}
		merged = true
	}

	if ns.recreateDbClientAfterConsolidation && merged {
		ns.db.Close()
		var err error
		ns.db, err = nutsdb.Open(ns.dbOpts)
		if err != nil {
			return fmt.Errorf("While recreate db client after consolidate status files: %w", err)
		}
	}

	return nil
}

// Close is the implementation of Status.Close,
func (ns *NutsDBStatus) Close() error {
	if ns.db == nil {
		return nil
	}

	ns.dbMtx.Lock()
	defer ns.dbMtx.Unlock()
	return ns.db.Close()
}

// Initialize is the Intialize implementation of Status interface for NutsDBStatus.
// Checks and ensure that required buckets and keys exists, and try to fix problems on
// internal data structures and references.
func (ns *NutsDBStatus) Initialize() error {
	if ns.initialized {
		return nil
	}

	// Consolidate database
	err := ns.HouseKeeping()
	if err != nil {
		return fmt.Errorf("While perform initial HouseKeeping: %w", err)
	}

	ns.dbMtx.Lock()
	defer ns.dbMtx.Unlock()

	err = ns.db.Update(func(tx *nutsdb.Tx) error {
		// Load index from status db
		idx, err := getOrderIdxInTx(tx)
		if IsNotFoundErr(errors.Unwrap(err)) {
			idx = &orderIdx{}
			err := saveOrderIdxInTx(tx, idx)
			if err != nil {
				return fmt.Errorf("While save empty index: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("While load index: %w", err)
		}

		// Reindex if needed
		err = recreateIdxInTx(tx, idx)
		if err != nil {
			return fmt.Errorf("While recreate index: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("While initialize internal db: %w", err)
	}

	ns.initialized = true
	return nil
}

func recreateIdxInTx(tx *nutsdb.Tx, idx *orderIdx) error {
	if idx == nil {
		return fmt.Errorf("idx is nil")
	}

	// Fix index issues
	entries, err := tx.GetAll(dataBucket)
	if IsNotFoundErr(err) || len(entries) == 0 {
		// We should set empty idx
		idx.reset(0)
		err := saveOrderIdxInTx(tx, idx)
		if err != nil {
			return fmt.Errorf("While save empty index after checks that %s bucket is empty: %w", dataBucket, err)
		}
	} else if err != nil {
		return fmt.Errorf("While load instances to do initial index check: %w", err)
	} else {
		fixRequired := false

		// Should I rebuild index?
		if len(idx.Order) != len(entries) {
			fixRequired = true
		}
		if len(idx.Refs) != len(entries) {
			fixRequired = true
		}

		if fixRequired {
			// Loop over all entries to get id and timestamp. This will be used by order
			idsTs := &SorteableStringTime{}

			for _, entry := range entries {
				er := &EventRecord{}
				err = msgpack.Unmarshal(entry.Value, er)
				if err != nil {
					return fmt.Errorf("While unmarshall record from raw %v: %w", entry.Value, err)
				}

				ID := er.EffectiveID()
				if ID == "" {
					return fmt.Errorf("Loaded record , %+v, without any value in ASyncIds", er)
				}
				idsTs.Add(ID, er.Timestamp)
			}

			// Sort IDs to be added to new idx
			sort.Sort(idsTs)

			// Reset and build index with idsTs data
			idx.reset(idsTs.Len())
			for i, k := range idsTs.Values {
				idx.Order[i] = k
				idx.Refs[k] = k
			}

			// Save new verison of idx
			err = saveOrderIdxInTx(tx, idx)
			if err != nil {
				return fmt.Errorf("While save rebuilt index: %w", err)
			}
		}
	}

	return nil
}

func getDataRecordInTx(tx *nutsdb.Tx, ID []byte) (*EventRecord, error) {
	raw, err := tx.Get(dataBucket, ID)
	if err != nil {
		return nil, err
	}

	r := &EventRecord{}
	err = msgpack.Unmarshal(raw.Value, r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func saveDataRecordInTx(tx *nutsdb.Tx, er *EventRecord, ttl uint32) error {

	ID := []byte(er.AsyncIDs[0])
	ts := uint64(er.Timestamp.Unix())
	raw, err := er.Serialize()
	if err != nil {
		return fmt.Errorf("While create new record in TX: %w", err)
	}

	err = tx.PutWithTimestamp(dataBucket, ID, raw, ttl, ts)
	if err != nil {
		return fmt.Errorf("While save new record in %s.%s in TX: %w", dataBucket, string(ID), err)
	}

	return nil
}

func deleteDataRecordInTx(tx *nutsdb.Tx, ID []byte) error {
	return tx.Delete(dataBucket, ID)
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
