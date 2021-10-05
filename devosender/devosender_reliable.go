package devosender

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/xujiajun/nutsdb"
)

// ReliableClientBuilder defines the Builder for build ReliableClient
type ReliableClientBuilder struct {
	clientBuilder            *ClientBuilder
	dbOpts                   nutsdb.Options
	retryDaemonOpts          daemonOpts
	clientReconnOpts         daemonOpts
	daemonStopTimeout        time.Duration
	bufferEventsSize         uint
	eventTimeToLive          uint32
	enableStandByModeTimeout time.Duration
	flushTimeout             time.Duration
}

type daemonOpts struct {
	waitBtwChecks time.Duration
	initDelay     time.Duration
}

const (
	// DefaultDaemonWaitBtwChecks is the default time that daemons must wait between
	// run checks or works
	DefaultDaemonWaitBtwChecks = time.Second
	// DefaultDaemonInitDelay is the default delay time that daemons must wait before
	// start to work
	DefaultDaemonInitDelay = time.Millisecond * 500
	// DefaultBufferEventsSize is the default size of the total events buffer managed
	// by Reliable client to save events
	DefaultBufferEventsSize uint = 5000000
	// DefaultEventTimeToLive is the expiration time in secods for each event before
	//be evicted from the buffer by Reliable client
	DefaultEventTimeToLive = 60 * 60
	// DefaultEnableStandByModeTimeout is the Default timeout to wait for all pending
	// async messages managed byclient when StandBy func is called . If timeout is
	// reached then error will be send
	DefaultEnableStandByModeTimeout = time.Second
	// DefaultDaemonStopTimeout is the default timeout to wait when stopping (Close) each daemon
	DefaultDaemonStopTimeout = time.Second * 2
	// DefaultFlushAsyncTimeout is the Default timeout to wait for all pending async
	// messages managed by client when Flush func is called. If timeout is reached
	// then error will be send
	DefaultFlushAsyncTimeout = time.Millisecond * 500
)

// NewReliableClientBuilder return ReliableClientBuilder with intialized to default values
func NewReliableClientBuilder() *ReliableClientBuilder {

	r := &ReliableClientBuilder{
		retryDaemonOpts:          daemonOpts{DefaultDaemonWaitBtwChecks, DefaultDaemonInitDelay},
		clientReconnOpts:         daemonOpts{DefaultDaemonWaitBtwChecks, DefaultDaemonInitDelay},
		daemonStopTimeout:        DefaultDaemonStopTimeout,
		bufferEventsSize:         DefaultBufferEventsSize,
		eventTimeToLive:          DefaultEventTimeToLive,
		enableStandByModeTimeout: DefaultEnableStandByModeTimeout,
		dbOpts:                   nutsdb.DefaultOptions,
		flushTimeout:             DefaultFlushAsyncTimeout,
	}

	return r
}

// DbPath sets the Database status path in the filesystem
func (dsrcb *ReliableClientBuilder) DbPath(path string) *ReliableClientBuilder {
	dsrcb.dbOpts.Dir = path
	return dsrcb
}

// RetryDaemonWaitBtwChecks sets the time wait interval between checks for retry send events daemon
// value is set only if d value is greater than 0
func (dsrcb *ReliableClientBuilder) RetryDaemonWaitBtwChecks(d time.Duration) *ReliableClientBuilder {
	if d > 0 {
		dsrcb.retryDaemonOpts.waitBtwChecks = d
	}
	return dsrcb
}

// ClientReconnDaemonWaitBtwChecks sets the time wait interval between checks for reconnect daemon
// value is set only if d value is greater than 0
func (dsrcb *ReliableClientBuilder) ClientReconnDaemonWaitBtwChecks(d time.Duration) *ReliableClientBuilder {
	if d > 0 {
		dsrcb.clientReconnOpts.waitBtwChecks = d
	}
	return dsrcb
}

// RetryDaemonInitDelay sets the initial time delay when retry send events daemon is started
// value is set only if d value is greater than 0
func (dsrcb *ReliableClientBuilder) RetryDaemonInitDelay(d time.Duration) *ReliableClientBuilder {
	if d > 0 {
		dsrcb.retryDaemonOpts.initDelay = d
	}
	return dsrcb
}

// ClientReconnDaemonInitDelay sets the initial time delay when reconnect daemon is started
// value is set only if d value is greater than 0
func (dsrcb *ReliableClientBuilder) ClientReconnDaemonInitDelay(d time.Duration) *ReliableClientBuilder {
	if d > 0 {
		dsrcb.clientReconnOpts.initDelay = d
	}
	return dsrcb
}

// DaemonStopTimeout sets the timeout to wait for each daemon when ReliableClient is closed
// value is set only if d value is greater than 0
func (dsrcb *ReliableClientBuilder) DaemonStopTimeout(d time.Duration) *ReliableClientBuilder {
	if d > 0 {
		dsrcb.daemonStopTimeout = d
	}
	return dsrcb
}

// BufferEventsSize sets the maximun number of events to get in the buffer.
// Be carefully when set this value, because some operations requires to load all keys in
// memory
// Value is only if size less or equal than math.MaxInt64
func (dsrcb *ReliableClientBuilder) BufferEventsSize(size uint) *ReliableClientBuilder {
	if size <= math.MaxInt64 {
		dsrcb.bufferEventsSize = size
	}
	return dsrcb
}

// EventTimeToLiveInSeconds sets the time to live per each event in seconds.
// If d value is zero then no expiration will be active
func (dsrcb *ReliableClientBuilder) EventTimeToLiveInSeconds(d uint32) *ReliableClientBuilder {
	dsrcb.eventTimeToLive = d
	return dsrcb
}

// EnableStandByModeTimeout sets and enable if value is greter than 0, the timeout to wait
// for pending async events in client when StandBy() func is called
func (dsrcb *ReliableClientBuilder) EnableStandByModeTimeout(d time.Duration) *ReliableClientBuilder {
	dsrcb.enableStandByModeTimeout = d
	return dsrcb
}

// FlushTimeout sets the timeout when wait for pending async envents in clien when
// Flush() func is called
func (dsrcb *ReliableClientBuilder) FlushTimeout(d time.Duration) *ReliableClientBuilder {
	if d >= 0 {
		dsrcb.flushTimeout = d
	}

	return dsrcb
}

// ClientBuilder sets the ClientBuilder needed to build the underhood client. This is required
// to initial setup and it is used by reconnect daemon too.
func (dsrcb *ReliableClientBuilder) ClientBuilder(cb *ClientBuilder) *ReliableClientBuilder {
	dsrcb.clientBuilder = cb
	return dsrcb
}

// ReliableClient defines a Client with Reliable capatilities for Async operations only
type ReliableClient struct {
	*Client
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
	clientMtx                sync.Mutex
	standByMode              bool
	enableStandByModeTimeout time.Duration
	dbInitCleanedup          bool
	daemonStopped            chan bool
	flushTimeout             time.Duration
}


// reliableClientRecord is the internal structure to save and manage the status of the event
// and allow operations like resend.
type reliableClientRecord struct {
	AsyncIDs   []string
	Timestamp  time.Time
	Tag        string
	Msg        string
	Compressor *Compressor
	LastError  error
}

// Serialize returns the serialized value of a reliableClientRecord
func (rcr *reliableClientRecord) Serialize() []byte {
	r, err := msgpack.Marshal(rcr)
	if err != nil {
		fmt.Println("ERROR uncontrolled in reliableClientRecord.Serialize", err)
		panic(err)
	}
	return r
}

// mustUnserialize returns the assoicated reliableClientRecord to a previously serialized value.
func mustUnserialize(bs []byte, dst *reliableClientRecord) {
	err := msgpack.Unmarshal(bs, dst)
	if err != nil {
		fmt.Println("ERROR uncontrolled in Unserialize of reliableClientRecord type", err)
		panic(err)
	}
}

const (
	dataBucket      = "data"
	ctrlBucket      = "ctrl"
	statsBucket     = "stats"
	nonConnIDPrefix = "non-conn-"
)

var (
	keysKey              = []byte("keys")
	countKey             = []byte("count")
	keysInOrderKey       = []byte("keys_in_order")
	updatedKey           = []byte("updated")
	evictedKey           = []byte("evicted")
	finishedKey          = []byte("finished")
	droppedKey           = []byte("dropped")
	nonConnIDPrefixBytes = []byte(nonConnIDPrefix)
)

// newRecord saves in status and persist new record updating counters at same time
func (dsrc *ReliableClient) newRecord(r *reliableClientRecord) error {
	id := r.AsyncIDs[len(r.AsyncIDs)-1]
	idAsBytes := []byte(id)

	err := dsrc.db.Update(func(tx *nutsdb.Tx) error {
		totalEvents, err := cont(tx, statsBucket, countKey, false)
		if err != nil {
			return err
		}

		droppedEvents := false
		if uint(totalEvents) >= dsrc.bufferSize {
			// We need to remove elements
			removeNum := totalEvents - int(dsrc.bufferSize) + 1 // plus one to break free space for current event

			err := dropRecordsInTx(tx, removeNum)
			if err != nil {
				return err
			}
			droppedEvents = true
		}

		err = tx.PutWithTimestamp(dataBucket, idAsBytes, r.Serialize(), dsrc.eventTTLSeconds, uint64(r.Timestamp.Unix()))
		if err != nil {
			return err
		}

		err = tx.SAdd(ctrlBucket, keysKey, idAsBytes)
		if err != nil {
			return err
		}

		err = tx.RPush(ctrlBucket, keysInOrderKey, idAsBytes)
		if err != nil {
			return err
		}

		if droppedEvents {
			// Ensure we have the counter just buffer size
			err = set(tx, statsBucket, countKey, int(dsrc.bufferSize))
		} else {
			// We only update counter if we did not drop events by full buffer reason.
			// Counter was properly updated by dropRecords
			err = inc(tx, statsBucket, countKey, 1, false)
		}
		//}

		return err
	})

	if err != nil {
		return fmt.Errorf("Error when create new record with %s id: %w", id, err)
	}

	return nil
}

// updateRecord updates the status of a reliableClientRecord with new ID updating counters at same time
func (dsrc *ReliableClient) updateRecord(r *reliableClientRecord, newID string) {
	oldID := r.AsyncIDs[len(r.AsyncIDs)-1] // Only for debug purpose

	err := dsrc.db.Update(func(tx *nutsdb.Tx) error {
		return updateRecordInTx(tx, r, newID, dsrc.eventTTLSeconds)
	})

	if err != nil {
		fmt.Printf("ERROR uncontrolled when updateRecord newID %s, oldID %s: %v\n", newID, oldID, err)
		panic(err)
	}
}

// updateRecordInTx updates the status of a reliableClientRecord with new ID updating counters
// at same time, using a provided sttus db transaction
func updateRecordInTx(tx *nutsdb.Tx, r *reliableClientRecord, newID string, ttl uint32) error {
	now := time.Now()
	oldID := r.AsyncIDs[len(r.AsyncIDs)-1]

	// Check for expiration
	expiration := r.Timestamp.Add(time.Duration(ttl) * time.Second)
	if expiration.Before(now) {
		// Event was expired. We directly remove it
		oldIDAsBytes := []byte(oldID)
		return deleteRecordRawInTx(tx, oldIDAsBytes) // Last ID is the used to delete
	}

	// Update Id create new key in data and ctrl in database and remove old ones
	r.AsyncIDs = append(r.AsyncIDs, newID)
	newIDAsBytes := []byte(newID)
	oldIDAsBytes := []byte(oldID)

	// Add new elements
	err := tx.SAdd(ctrlBucket, keysKey, newIDAsBytes)
	if err != nil {
		return err
	}
	err = tx.PutWithTimestamp(dataBucket, newIDAsBytes, r.Serialize(), ttl, uint64(r.Timestamp.Unix()))
	if err != nil {
		return err
	}

	// Remove old elements
	err = tx.Delete(dataBucket, oldIDAsBytes)
	if err != nil {
		return err
	}
	err = tx.SRem(ctrlBucket, keysKey, oldIDAsBytes)
	if err != nil {
		return err
	}

	// Update keysInOrder element
	n, err := tx.LSize(ctrlBucket, keysInOrderKey)
	if err != nil {
		return err
	}

	// Load all keys in memory.
	ls, err := tx.LRange(ctrlBucket, keysInOrderKey, 0, n-1)
	if err != nil {
		return err
	}

	found := false
	for idx, vs := range ls {
		if bytes.Equal(vs, oldIDAsBytes) {
			err := tx.LSet(ctrlBucket, keysInOrderKey, idx, newIDAsBytes)
			if err != nil {
				return err
			}
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("Error old id %s did not find in %s.%s", oldID, ctrlBucket, string(keysInOrderKey))
	}

	// Counters and stats
	err = inc(tx, statsBucket, updatedKey, 1, false)
	return err
}

// deleteRecords deletes one or more reliableClientRecord from status ID updating counters at same time
func (dsrc *ReliableClient) deleteRecords(IDs ...string) error {
	for _, ID := range IDs {
		err := dsrc.deleteRecordRaw([]byte(ID))
		if err != nil {
			return fmt.Errorf("Error when delete record with ID: %s: %w", ID, err)
		}
	}

	return nil
}

// deleteRecords deletes one reliableClientRecord from status ID updating counters at same time
func (dsrc *ReliableClient) deleteRecordRaw(idsAsBytes []byte) error {
	err := dsrc.db.Update(func(tx *nutsdb.Tx) error {
		return deleteRecordRawInTx(tx, idsAsBytes)
	})

	if err != nil {
		return fmt.Errorf("Error when deleteRecord with %s ID: %w", string(idsAsBytes), err)
	}

	return nil
}

// deleteRecords deletes one reliableClientRecord from status ID updating counters at
// same time using a provided status db transaction
func deleteRecordRawInTx(tx *nutsdb.Tx, idAsBytes []byte) error {

	// Load key in ctrl idx index
	ok, err := tx.SAreMembers(ctrlBucket, keysKey, idAsBytes)
	if nutsdbIsNotFoundError(err) {
		return nil
	} else if err != nil {

		return fmt.Errorf("Error when look for member %s in %s.%s: %w",
			string(idAsBytes), ctrlBucket, string(keysKey), err)
	}

	if ok {
		// Check for existence  to increment evicted  counter
		_, err := tx.Get(dataBucket, idAsBytes)
		if err == nutsdb.ErrNotFoundKey || err == nutsdb.ErrKeyNotFound {
			// Only update evicted stat because real data could be expired
			err = inc(tx, statsBucket, evictedKey, 1, false)
			if err != nil {
				return fmt.Errorf("Error when increment %s: %w", string(evictedKey), err)
			}
		}

		err = tx.Delete(dataBucket, idAsBytes)
		if err != nil {
			return fmt.Errorf("Error when delete key %s from data: %w", string(idAsBytes), err)
		}

		err = tx.SRem(ctrlBucket, keysKey, idAsBytes)
		if err != nil {
			return fmt.Errorf("Error when delete key %s from %s.%s: %w",
				string(idAsBytes), ctrlBucket, string(keysKey), err)
		}
	}

	_, err = tx.LRem(ctrlBucket, keysInOrderKey, 0, idAsBytes)
	if err != nil {
		return fmt.Errorf("Error when delete key %s from %s.%s: %w",
			string(idAsBytes), ctrlBucket, string(keysInOrderKey), err)
	}

	err = dec(tx, statsBucket, countKey, 1, true)
	if err != nil {
		return fmt.Errorf("Error when decrement %s: %w", string(countKey), err)
	}

	err = inc(tx, statsBucket, finishedKey, 1, false)
	if err != nil {
		return fmt.Errorf("Error when increment %s: %w", string(finishedKey), err)
	}

	return nil
}

// dropRecords drops one or more older records and update the stat counters too.
func (dsrc *ReliableClient) dropRecords(n int) error {

	err := dsrc.db.Update(func(tx *nutsdb.Tx) error {
		return dropRecordsInTx(tx, n)
	})

	if err != nil {
		return fmt.Errorf("Error when dropRecord number %d: %w", n, err)
	}

	return nil
}

// dropRecords drops one or more older records and update the stat counters too using
// a provided status db transaction
func dropRecordsInTx(tx *nutsdb.Tx, n int) error {
	// Check if we have enough elemetns based on stats.count
	ce, err := tx.Get(statsBucket, countKey)
	if nutsdbIsNotFoundError(err) {
		return nil
	}
	if err != nil {
		return err
	}

	c, _ := strconv.Atoi(string(ce.Value))
	if n > c {
		n = c
	}

	// Load n last elements from ctrl.keys_in_order
	ids := make([][]byte, n)

	for i := 0; i < n; i++ {
		vs, err := tx.LPop(ctrlBucket, keysInOrderKey)
		if err != nil {
			return err
		}
		ids[i] = vs
	}

	// Now purge it
	for _, id := range ids {
		err = deleteRecordRawInTx(tx, id)
		if err != nil {
			return err
		}
	}

	// Update stats
	err = inc(tx, statsBucket, droppedKey, n, false)

	return err
}

// findAllRecordsID returns a slice with all string IDs saved in the status db
func (dsrc *ReliableClient) findAllRecordsID() []string {
	records := dsrc.findAllRecordsIDRaw()
	r := make([]string, len(records))
	for i, rc := range records {
		r[i] = string(rc)
	}
	return r
}

// findAllRecordsID returns a slice with []byte serialized representation of all
// IDs saved in the status db
func (dsrc *ReliableClient) findAllRecordsIDRaw() [][]byte {
	var r [][]byte
	err := dsrc.db.View(func(tx *nutsdb.Tx) error {
		var err error
		r, err = findAllRecordsIDRawInTx(tx)
		return err
	})

	if err != nil {
		fmt.Printf("ERROR uncontrolled when findAllRecordsID %v\n", err)
		panic(err)
	}

	return r
}

// findAllRecordsID returns a slice with []byte serialized representation of all IDs
// saved in the status db using a provided status db transaction
func findAllRecordsIDRawInTx(tx *nutsdb.Tx) ([][]byte, error) {
	// Load key in ctrl idx index
	r, err := tx.SMembers(ctrlBucket, keysKey)
	if nutsdbIsNotFoundError(err) {
		return nil, nil
	}
	return r, err
}

// getRecord returns the reliableClientRecord in the status identified by id
func (dsrc *ReliableClient) getRecord(id string) *reliableClientRecord {
	return dsrc.getRecordRaw([]byte(id))
}

// getRecordRaw returns the reliableClientRecord in the status identified by serialized
// representation of the id
func (dsrc *ReliableClient) getRecordRaw(idAsBytes []byte) *reliableClientRecord {
	var r *reliableClientRecord
	err := dsrc.db.View(func(tx *nutsdb.Tx) error {
		var err error
		r, err = getRecordRawInTx(tx, idAsBytes)

		return err
	})

	if err != nil {
		fmt.Printf("ERROR uncontrolled when getRecordRaw %v\n", err)
		panic(err)
	}

	return r
}

// getRecordRawInTx returns the reliableClientRecord in the status identified by serialized
// representation of the id using a provided status db transaction
func getRecordRawInTx(tx *nutsdb.Tx, idAsBytes []byte) (*reliableClientRecord, error) {
	ve, err := tx.Get(dataBucket, idAsBytes)
	if nutsdbIsNotFoundError(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	r := &reliableClientRecord{}
	err = msgpack.Unmarshal(ve.Value, r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// dec decrements the integer value of a key. If key exists value should be transformed using
// strvconv.Atoi(string(value)) before decrements the value. If key does not exist it will create
// with -1 value unless errorIfNotFound parameter is true. On this case return error.
// WARNING. Dec is not prepare to be called more than one time on same transaction. To solve this
// problem use set value instead.
func dec(tx *nutsdb.Tx, bucket string, key []byte, n int, errorIfNotFound bool) error {
	if n == 0 {
		return nil
	}

	ve, err := tx.Get(bucket, key)
	if nutsdbIsNotFoundError(err) {
		if errorIfNotFound {
			return err
		}
		err = tx.Put(bucket, key, []byte("-1"), 0)
	} else {
		v, _ := strconv.Atoi(string(ve.Value))
		v = v - n

		err = tx.Put(bucket, key, []byte(strconv.Itoa(v)), 0)
	}

	return err
}

// inc increments the integer value of a key. If key exists value should be transformed using
// strvconv.Atoi(string(value)) before increments the value. If key does not exist it will create
// with 1 value unless errorIfNotFound parameter is true. On this case it returns an error.
// WARNING. inc is not prepare to be called more than one time on same transaction. To solve this
// problem use set func instead.
func inc(tx *nutsdb.Tx, bucket string, key []byte, n int, errorIfNotFound bool) error {
	if n == 0 {
		return nil
	}

	ve, err := tx.Get(bucket, key)
	if nutsdbIsNotFoundError(err) {
		if errorIfNotFound {
			return err
		}
		err = tx.Put(bucket, key, []byte("1"), 0)
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
// WARNING. con is not prepare to be called more than one time on same transaction. You should
// solve it loading the value at the begining of the transcation and maintain internally updated.
func cont(tx *nutsdb.Tx, bucket string, key []byte, errorIfNotFound bool) (int, error) {
	ve, err := tx.Get(bucket, key)
	if nutsdbIsNotFoundError(err) {
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

var reNotFoundError = regexp.MustCompile(`^not found bucket:.*,key:.*$`)

// nutsdbIsNotFoundError check and retur if error parameter is one of the "Not found"
// recognized errors returned by nutsdb operations.
func nutsdbIsNotFoundError(err error) bool {
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
	if err.Error() == "key not exits" {
		return true
	}
	if err.Error() == "item not exits" {
		return true
	}

	errStr := fmt.Sprint(err)
	return reNotFoundError.MatchString(errStr)
}
