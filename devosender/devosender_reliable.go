package devosender

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cyberluisda/devo-go/applogger"
	uuid "github.com/satori/go.uuid"
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
	appLogger                applogger.SimpleAppLogger
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
		appLogger:                &applogger.NoLogAppLogger{},
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
// Value is set only if size less or equal than math.MaxInt64
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

// AppLogger sets the AppLogger used to send logger messages in case that errors can not be
// returned. Debug traces can be saved usint this logger too.
func (dsrcb *ReliableClientBuilder) AppLogger(lg applogger.SimpleAppLogger) *ReliableClientBuilder {
	dsrcb.appLogger = lg
	return dsrcb
}

// Build builds the ReliableClient based in current parameters.
func (dsrcb *ReliableClientBuilder) Build() (*ReliableClient, error) {
	// Check required config
	if dsrcb.dbOpts.Dir == "" {
		return nil, fmt.Errorf("Empty path where persist status")
	}

	if dsrcb.clientBuilder == nil {
		return nil, fmt.Errorf("Undefined inner client builder")
	}

	// Build inner Client
	cl, err := dsrcb.clientBuilder.Build()
	// we can continue in connection error scenario
	if err != nil && !isConnectionError(err) {
		return nil, err
	}

	r := &ReliableClient{
		Client:                   cl,
		clientBuilder:            dsrcb.clientBuilder, // We maybe need the builder when will need to recreate client
		bufferSize:               dsrcb.bufferEventsSize,
		eventTTLSeconds:          dsrcb.eventTimeToLive,
		retryWait:                dsrcb.retryDaemonOpts.waitBtwChecks,
		reconnWait:               dsrcb.clientReconnOpts.waitBtwChecks,
		daemonStopTimeout:        dsrcb.daemonStopTimeout,
		daemonStopped:            make(chan bool),
		flushTimeout:             dsrcb.flushTimeout,
		enableStandByModeTimeout: dsrcb.enableStandByModeTimeout,
		appLogger:                dsrcb.appLogger,
	}

	// Status DB
	r.db, err = nutsdb.Open(dsrcb.dbOpts)
	if err != nil {
		return nil, fmt.Errorf("Error when load persistence engine with %+v options: %w", dsrcb.dbOpts, err)
	}

	// Daemons startup
	r.daemonsSartup()

	return r, nil
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
	appLogger                applogger.SimpleAppLogger
}

// SendAsync sends Async message in same way like Client.SendAsync but saving the message
// in status until can ensure, at certain level of confiance, that it was sent
func (dsrc *ReliableClient) SendAsync(m string) string {
	var id string
	if dsrc.IsStandBy() || dsrc.Client == nil {
		id = nonConnIDPrefix + uuid.NewV4().String()
	} else {
		id = dsrc.Client.SendAsync(m)
	}

	record := &reliableClientRecord{
		AsyncIDs:  []string{id},
		Timestamp: time.Now(),
		Msg:       m,
	}
	err := dsrc.newRecord(record)
	if err != nil {
		dsrc.appLogger.Logf(applogger.ERROR, "Uncontrolled error when create status record in SendAsync: %v", err)
	}

	return id
}

// SendWTagAsync sends Async message in same way like Client.SendWTagAsync but saving the message
// and tag in status until can ensure, at certain level of confiance, that it was sent
func (dsrc *ReliableClient) SendWTagAsync(t, m string) string {
	var id string
	if dsrc.IsStandBy() || dsrc.Client == nil {
		id = nonConnIDPrefix + uuid.NewV4().String()
	} else {
		id = dsrc.Client.SendWTagAsync(t, m)
	}

	record := &reliableClientRecord{
		AsyncIDs:  []string{id},
		Timestamp: time.Now(),
		Tag:       t,
		Msg:       m,
	}
	err := dsrc.newRecord(record)
	if err != nil {
		dsrc.appLogger.Logf(applogger.ERROR, "Uncontrolled error when create status record in SendWTagAsync: %v", err)
	}

	return id
}

// SendWTagAndCompressorAsync sends Async message in same way like Client.SendWTagAndCompressorAsync
// but saving the message,tag and Compressor in status until can ensure, at certain level of
// confiance, that it was sent
func (dsrc *ReliableClient) SendWTagAndCompressorAsync(t string, m string, c *Compressor) string {
	var id string
	if dsrc.IsStandBy() || dsrc.Client == nil {
		id = nonConnIDPrefix + uuid.NewV4().String()
	} else {
		id = dsrc.Client.SendWTagAndCompressorAsync(t, m, c)
	}

	record := &reliableClientRecord{
		AsyncIDs:   []string{id},
		Timestamp:  time.Now(),
		Tag:        t,
		Msg:        m,
		Compressor: c,
	}
	err := dsrc.newRecord(record)
	if err != nil {
		dsrc.appLogger.Logf(applogger.ERROR, "Uncontrolled error when create status record in SendWTagAndCompressorAsync: %v", err)
	}

	return id
}

// Flush checks all pending messages (sent with Async funcs), waits for pending async messages
// and update status of all of them. This func can call on demand but it is called by
// internal retry send events daemon too
func (dsrc *ReliableClient) Flush() error {

	isClientUp := !dsrc.IsStandBy()
	if isClientUp {
		dsrc.clientMtx.Lock()
		isClientUp = !(dsrc.Client == nil)
		dsrc.clientMtx.Unlock()
	}

	// Recollect pending events
	allIds, err := dsrc.findAllRecordsID()
	if err != nil {
		return fmt.Errorf("Error when findAllRecords before flush: %w", err)
	}

	if isClientUp {
		err = dsrc.WaitForPendingAsyncMsgsOrTimeout(dsrc.flushTimeout)
		if err != nil {
			return fmt.Errorf("Timeout %s reached when wait for pending async msgs: %w", dsrc.flushTimeout, err)
		}

		idsToBeResend := map[string]error{}
		assumingWasSent := make([]string, 0)
		for _, id := range allIds {
			// If Id is no connecion
			if strings.HasPrefix(id, nonConnIDPrefix) {
				idsToBeResend[id] = nil
			} else {
				// Check if Id is not pending
				if !dsrc.IsAsyncActive(id) {
					// Load errors and check on it
					errorsMap := dsrc.AsyncErrors()
					if err, ok := errorsMap[id]; ok {
						// We have found error, retrying
						idsToBeResend[id] = err

						// Advise through log that "ErrorTagEmpty" will be never send
						if errors.Is(err, ErrorTagEmpty) {
							dsrc.appLogger.Logf(
								applogger.ERROR,
								"Message with id '%s' will be never send because has empty tag", id,
							)
						}
					} else {
						assumingWasSent = append(assumingWasSent, id)
					}
				}
			}
		}

		// Now resend pending or mark as Evicted
		evicted := make([]string, 0)
		for k, v := range idsToBeResend {
			record, err := dsrc.getRecord(k)
			if err != nil {
				return fmt.Errorf("Error when load record from status with id %s to be processed: %w", k, err)
			}
			if record == nil {
				// Evicted
				evicted = append(evicted, k)
			} else {
				record.LastError = v
				err = dsrc.resendRecord(record)
				if err != nil {
					return fmt.Errorf("Error when resend record with id %s: %w", k, err)
				}
			}
		}

		// Ensure evicted are removed:
		err = dsrc.deleteRecords(evicted...)
		if err != nil {
			return fmt.Errorf("Error when delete one evicted status record: %w", err)
		}

		// Remove records was send
		err = dsrc.deleteRecords(assumingWasSent...)
		if err != nil {
			return fmt.Errorf("Error when delete one status record that I assumed that was sent: %w", err)
		}

	} else {
		// Passing all elemetns as no-conn
		for _, id := range allIds {
			// If Id is no connecion
			if !strings.HasPrefix(id, nonConnIDPrefix) {
				record, err := dsrc.getRecord(id)
				if err != nil {
					return fmt.Errorf("Error when load record from status with id %s: %w", id, err)
				}

				err = dsrc.updateRecord(record, nonConnIDPrefix+id)
				if err != nil {
					return fmt.Errorf("Error when pass one status record with old id %s to no-conn state: %w", id, err)
				}
			}
		}
	}

	return nil
}

// Close closes current client. This implies operations like shutdown daemons, call Flush func, etc.
func (dsrc *ReliableClient) Close() error {
	errors := make([]error, 0)

	err := dsrc.daemonsShutdown()
	if err != nil {
		errors = append(errors, fmt.Errorf("Error when shutdown daemons: %w", err))
	}

	err = dsrc.Flush()
	if err != nil {
		errors = append(errors, fmt.Errorf("Error when flush: %w", err))
	}

	if dsrc.Client != nil {
		dsrc.Client.Close()
		if err != nil {
			errors = append(errors, fmt.Errorf("Error when close client: %w", err))
		}
	}

	err = dsrc.ResetSessionStats()
	if err != nil {
		errors = append(errors, fmt.Errorf("Error when reset session stats: %w", err))
	}

	err = dsrc.db.Close()
	if err != nil {
		errors = append(errors, fmt.Errorf("Error when close db: %w", err))
	}

	if len(errors) == 0 {
		return nil
	}

	err = fmt.Errorf("")
	for _, e := range errors {
		err = fmt.Errorf("%v, %v", e, err)
	}

	return err
}

// StandBy put current client in stand by mode closing active connection and saving
// new incoming events from Async operations in status.
// Note that after call StandBy, Send Sync operations will return errors.
func (dsrc *ReliableClient) StandBy() error {
	// If this is started we stop
	if dsrc.Client != nil {
		dsrc.clientMtx.Lock()
		defer dsrc.clientMtx.Unlock()

		if dsrc.enableStandByModeTimeout > 0 {
			err := dsrc.WaitForPendingAsyncMsgsOrTimeout(dsrc.enableStandByModeTimeout)
			if err != nil {
				return fmt.Errorf("Error when wait for pending async operations, timeout %s: %w",
					dsrc.enableStandByModeTimeout, err)
			}
		}

		err := dsrc.Client.Close()
		if err != nil {
			dsrc.standByMode = true
			return fmt.Errorf("Error when close client passing to StandBy: %w", err)
		}
		// Destroy curret client to ensrue will be recreated when WakeUp
		dsrc.Client = nil
	}

	dsrc.standByMode = true
	return nil
}

// WakeUp is the inverse oeration to call StandBy
func (dsrc *ReliableClient) WakeUp() error {
	// We ever pass to standByMode to false, and delegate
	// and delegate in reconnDaemon to try to restart connection if we are
	// failling here
	dsrc.standByMode = false

	if dsrc.Client == nil {
		dsrc.clientMtx.Lock()
		defer dsrc.clientMtx.Unlock()

		var err error
		dsrc.Client, err = dsrc.clientBuilder.Build()
		if err != nil {
			return fmt.Errorf("Error when creating new client. StandByMode deactivated anyway: %w", err)
		}
	}

	return nil
}

// IsStandBy retursn true when client is in StandBy() mode
func (dsrc *ReliableClient) IsStandBy() bool {
	return dsrc.standByMode
}

// ReliableClientStats represents the stats that can be queried
type ReliableClientStats struct {
	Count    int
	Updated  int
	Finished int
	Dropped  int
	Evicted  int
}

// Stats returns the curren stats (session + persisted). Erros when load stas are ignored
func (dsrc *ReliableClient) Stats() ReliableClientStats {
	r := ReliableClientStats{}
	dsrc.db.View(func(tx *nutsdb.Tx) error {
		v, _ := cont(tx, statsBucket, countKey, false)
		r.Count = v

		v, _ = cont(tx, statsBucket, updatedKey, false)
		r.Updated = v

		v, _ = cont(tx, statsBucket, finishedKey, false)
		r.Finished = v

		v, _ = cont(tx, statsBucket, droppedKey, false)
		r.Dropped = v

		v, _ = cont(tx, statsBucket, evictedKey, false)
		r.Evicted = v

		return nil
	})

	return r
}

// ResetSessionStats remove stats values from status. Stats values considerd at
// session scope are: 'update', 'deleted', 'dropped' and 'evicted' counters
func (dsrc *ReliableClient) ResetSessionStats() error {
	// session stas are: updated, deleted, dropped and evicted:
	err := dsrc.db.Update(func(tx *nutsdb.Tx) error {
		for _, key := range [4][]byte{updatedKey, finishedKey, droppedKey, evictedKey} {
			err := del(tx, statsBucket, key)
			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("Error when reset session stats: %w", err)
	}

	return nil
}

func (dsrc *ReliableClient) String() string {
	db := "<nil>"
	if dsrc.db != nil {
		db = fmt.Sprintf("{KeyCount: %d, ListIdx: %v}", dsrc.db.KeyCount, dsrc.db.ListIdx)
	}
	return fmt.Sprintf(
		"Client: {%s}, db: %s, bufferSize: %d, eventTTLSeconds: %d, retryWait: %v, "+
			"reconnWait: %v, retryStop: %v, reconnStop: %v, retryInitDelay: %v, reconnInitDelay: %v, "+
			"daemonStopTimeout: %v, standByMode: %v, enableStandByModeTimeout: %v, dbInitCleanedup: %v, "+
			"daemonStopped: %v, flushTimeout: %v",
		dsrc.Client.String(),
		db,
		dsrc.bufferSize,
		dsrc.eventTTLSeconds,
		dsrc.retryWait,
		dsrc.reconnWait,
		dsrc.retryStop,
		dsrc.reconnStop,
		dsrc.retryInitDelay,
		dsrc.reconnInitDelay,
		dsrc.daemonStopTimeout,
		dsrc.standByMode,
		dsrc.enableStandByModeTimeout,
		dsrc.dbInitCleanedup,
		dsrc.daemonStopped,
		dsrc.flushTimeout,
	)
}

// daemonsSartup perform init cleanup (only once) and starts the resend events and
// reconnect daemons, capture interrumnt and term signals to close database, etc...
func (dsrc *ReliableClient) daemonsSartup() error {
	if dsrc.db == nil {
		return fmt.Errorf("db is nil any setup action can not be done")
	}

	// Old saved state cleanup
	err := dsrc.dbInitCleanup()
	if err != nil {
		return err
	}

	// Capture termination and close client
	go func() {
		sigchan := make(chan os.Signal)
		signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
		s := <-sigchan

		err := dsrc.Close()
		if err != nil {
			dsrc.appLogger.Logf(
				applogger.ERROR,
				"Error while close ReliableClient by daemonStartup after %v signal received: %v",
				s,
				err,
			)
		}

		dsrc.appLogger.Log(applogger.INFO, "Bye!")
	}()

	// Pending events daemon
	err = dsrc.startRetryEventsDaemon()
	if err != nil {
		return err
	}

	// Client reconnection
	err = dsrc.clientReconnectionDaemon()
	if err != nil {
		return err
	}

	return nil
}

// daemonsShutdown tries to stop daemons in gracefull mode, grace period to wait for
// each daemon stopped is set in dsrc.daemonStopTimeout
func (dsrc *ReliableClient) daemonsShutdown() error {
	dsrc.retryStop = true
	dsrc.reconnStop = true

	errors := make([]error, 0)

	// two daemons
	for i := 0; i < 2; i++ {
		select {
		case <-time.After(dsrc.daemonStopTimeout):
			errors = append(errors, fmt.Errorf("Timeout when wait for daemon number: %d", i))
		case <-dsrc.daemonStopped:
			dsrc.appLogger.Log(
				applogger.INFO,
				"Bye! daemon number", i,
			)
		}
	}

	if len(errors) == 0 {
		return nil
	}
	err := fmt.Errorf("")
	for _, e := range errors {
		err = fmt.Errorf("%v, %v", e, err)
	}

	return fmt.Errorf("Errors when shutdown daemons: %w", err)
}

// dbInitCleanup checks and cleans database only one time per session. It is designed to be
// call at the beginning of the process, just before daemons are started.
func (dsrc *ReliableClient) dbInitCleanup() error {
	// Only run one time at startup
	if dsrc.dbInitCleanedup {
		return nil
	}
	err := dsrc.db.Update(func(tx *nutsdb.Tx) error {
		// Check if we have elements using count
		c, err := cont(tx, statsBucket, countKey, false)
		if err != nil {
			return err
		}
		if c == 0 {
			return nil
		}

		// Look for all records and move to no-conn-
		oldIds, err := findAllRecordsIDRawInTx(tx)
		if err != nil {
			return err
		}

		newIDs := make(map[string]interface{}, len(oldIds))
		for _, id := range oldIds {
			idAsStr := string(id)
			record, err := getRecordRawInTx(tx, id)
			if err != nil {
				return err
			}

			if bytes.HasPrefix(id, nonConnIDPrefixBytes) {
				newIDs[idAsStr] = nil
			} else {
				newID := nonConnIDPrefix + idAsStr
				err = updateRecordInTx(tx, record, newID, dsrc.eventTTLSeconds)
				if err != nil {
					return err
				}
				newIDs[newID] = nil
			}
		}

		// Purging orphan references

		// ctrl keys_in_order
		n, err := tx.LSize(ctrlBucket, keysInOrderKey)
		if err != nil {
			return err
		}
		keys, err := tx.LRange(ctrlBucket, keysInOrderKey, 0, n-1)
		if err != nil {
			return err
		}

		toRemove := make([][]byte, 0)
		for _, k := range keys {
			keyStr := string(k)
			if _, ok := newIDs[keyStr]; !ok {
				toRemove = append(toRemove, k)
				_, err = tx.LRem(ctrlBucket, keysInOrderKey, 0, k)
			}
		}

		if dsrc.appLogger.IsLevelEnabled(applogger.DEBUG) {
			for _, v := range toRemove {
				dsrc.appLogger.Logf(applogger.DEBUG, "ID %s removed from status just when initialize DB", string(v))
			}
		}

		// ctrl keys removed here
		err = tx.SRem(ctrlBucket, keysKey, toRemove...)
		if err != nil {
			return err
		}

		// update evicted
		err = inc(tx, statsBucket, evictedKey, len(toRemove), false)

		return err
	})

	if err != nil {
		err = fmt.Errorf("Error when make initial cleanup on status db: %w", err)
		dsrc.dbInitCleanedup = true
	}
	return err
}

// startRetryEventsDaemon runs in background the retry send events daemon. This daemon checks
// every dsr.retryWait time  the pending events and update status or resend it if error
// was saved by inner client. This actions are delegated to call Flush func
func (dsrc *ReliableClient) startRetryEventsDaemon() error {
	if dsrc.retryWait <= 0 {
		return fmt.Errorf("Time to wait between each check to retry events is not enough: %s", dsrc.retryWait)
	}
	go func() {
		// Init delay
		time.Sleep(dsrc.retryInitDelay)

		// Daemon loop
		for !dsrc.retryStop {
			err := dsrc.Flush()
			if err != nil {
				dsrc.appLogger.Logf(
					applogger.ERROR,
					"Error received while retryEventsDaemon flush client: %v", err,
				)
			}

			time.Sleep(dsrc.retryWait)
		}

		// Closed signal
		dsrc.daemonStopped <- true

	}()

	return nil
}

// clientReconnectionDaemon runs in background the reconnect  daemon. This daemon create new connection
// if ReliableClient is not in stand by mode and inner Client is nill.
// TODO Do other check if configured (for example Send Sync test message configured table) and recreate
// inner Client if it is failing
func (dsrc *ReliableClient) clientReconnectionDaemon() error {
	if dsrc.reconnWait <= 0 {
		return fmt.Errorf("Time to wait between each check to reconnect client is not enough: %s", dsrc.reconnWait)
	}
	go func() {
		// Init delay
		time.Sleep(dsrc.retryInitDelay)

		for !dsrc.reconnStop {
			dsrc.clientMtx.Lock()
			if !dsrc.IsStandBy() {
				// TODO implement other heltcheck mechanism here
				if dsrc.Client == nil {
					// Build inner Client
					var err error
					dsrc.Client, err = dsrc.clientBuilder.Build()
					// we can continue in connection error scenario
					if err != nil {
						// FIXME log
					}
				}
			}
			dsrc.clientMtx.Unlock()
			time.Sleep(dsrc.reconnWait)
		}

		// Closed signal
		dsrc.daemonStopped <- true
	}()

	return nil
}

// resendRecord send the event based on record status.
func (dsrc *ReliableClient) resendRecord(r *reliableClientRecord) error {
	var newID string
	if dsrc.IsStandBy() || dsrc.Client == nil {
		currID := r.AsyncIDs[len(r.AsyncIDs)-1]
		if strings.HasPrefix(currID, nonConnIDPrefix) {
			// Same id, noting to do because client is not active
			return nil
		}
		// Pass record to non-connection
		newID = nonConnIDPrefix + uuid.NewV4().String()

	} else {
		// Resend based on properties
		switch true {
		case r.Compressor != nil && r.Tag != "":
			newID = dsrc.Client.SendWTagAndCompressorAsync(r.Tag, r.Msg, r.Compressor)
		case r.Tag != "":
			newID = dsrc.Client.SendWTagAsync(r.Tag, r.Msg)
		default:
			newID = dsrc.Client.SendAsync(r.Msg)
		}
	}

	err := dsrc.updateRecord(r, newID)
	if err != nil {
		return fmt.Errorf("Error when updateRecord after resend with newID %s: %w", newID, err)
	}

	return nil
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
func (rcr *reliableClientRecord) Serialize() ([]byte, error) {
	r, err := msgpack.Marshal(rcr)
	if err != nil {
		return nil, fmt.Errorf("Error when serialize record: %w", err)
	}
	return r, nil
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

		bs, err := r.Serialize()
		if err != nil {
			return err
		}
		err = tx.PutWithTimestamp(dataBucket, idAsBytes, bs, dsrc.eventTTLSeconds, uint64(r.Timestamp.Unix()))
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
func (dsrc *ReliableClient) updateRecord(r *reliableClientRecord, newID string) error {
	oldID := r.AsyncIDs[len(r.AsyncIDs)-1] // Only for debug purpose

	err := dsrc.db.Update(func(tx *nutsdb.Tx) error {
		return updateRecordInTx(tx, r, newID, dsrc.eventTTLSeconds)
	})

	if err != nil {
		return fmt.Errorf("Error when updateRecord newID %s, oldID %s: %w", newID, oldID, err)
	}

	return nil
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

	bs, err := r.Serialize()
	if err != nil {
		return err
	}

	err = tx.PutWithTimestamp(dataBucket, newIDAsBytes, bs, ttl, uint64(r.Timestamp.Unix()))
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
	ids, err := tx.LRange(ctrlBucket, keysInOrderKey, 0, n-1)
	if err != nil {
		return err
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
func (dsrc *ReliableClient) findAllRecordsID() ([]string, error) {
	records, err := dsrc.findAllRecordsIDRaw()
	if err != nil {
		err = fmt.Errorf("Error when findAllRecordsID: %w", err)
		return nil, err
	}

	r := make([]string, len(records))
	for i, rc := range records {
		r[i] = string(rc)
	}

	return r, err
}

// findAllRecordsID returns a slice with []byte serialized representation of all
// IDs saved in the status db
func (dsrc *ReliableClient) findAllRecordsIDRaw() ([][]byte, error) {
	var r [][]byte
	err := dsrc.db.View(func(tx *nutsdb.Tx) error {
		var err error
		r, err = findAllRecordsIDRawInTx(tx)
		return err
	})

	if err != nil {
		err = fmt.Errorf("Error when findAllRecordsIDRaw: %w", err)
	}

	return r, err
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
func (dsrc *ReliableClient) getRecord(id string) (*reliableClientRecord, error) {
	return dsrc.getRecordRaw([]byte(id))
}

// getRecordRaw returns the reliableClientRecord in the status identified by serialized
// representation of the id
func (dsrc *ReliableClient) getRecordRaw(idAsBytes []byte) (*reliableClientRecord, error) {
	var r *reliableClientRecord
	err := dsrc.db.View(func(tx *nutsdb.Tx) error {
		var err error
		r, err = getRecordRawInTx(tx, idAsBytes)

		return err
	})

	if err != nil {
		err = fmt.Errorf("Error when getRecordRaw: %w", err)
	}

	return r, err
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
