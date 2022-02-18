package devosender

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/cyberluisda/devo-go/applogger"
	"github.com/cyberluisda/devo-go/devosender/compressor"
	"github.com/cyberluisda/devo-go/devosender/status"
)

// ReliableClientBuilder defines the Builder for build ReliableClient
type ReliableClientBuilder struct {
	clientBuilder            *ClientBuilder
	statusBuilder            status.Builder
	retryDaemonOpts          daemonOpts
	clientReconnOpts         daemonOpts
	houseKeepingDaemonOpts   daemonOpts
	daemonStopTimeout        time.Duration
	enableStandByModeTimeout time.Duration
	flushTimeout             time.Duration
	appLogger                applogger.SimpleAppLogger
	maxRecordsResendByFlush  int
}

type daemonOpts struct {
	waitBtwChecks time.Duration
	initDelay     time.Duration
}

const (
	// DefaultClientReconnDaemonWaitBtwChecks is the default time that client reconnect daemon
	// must wait between run checks or works
	DefaultClientReconnDaemonWaitBtwChecks = 10 * time.Second
	// DefaultClientReconnDaemonInitDelay is the default delay time that client reconnect daemon
	// must wait before start to work
	DefaultClientReconnDaemonInitDelay = time.Second * 30
	// DefaultDaemonMicroWait is the default micro delay to check in the midle of daemon
	// sleep time if daemon was marked to be stopped, then interrup sleep operation
	DefaultDaemonMicroWait = time.Millisecond * 200
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
	// DefaultHouseKeepingDaemonWaitBtwChecks is the default time that status housekeeping daemon must wait
	// between run checks or do any action
	DefaultHouseKeepingDaemonWaitBtwChecks = time.Minute * 5
	// DefaultHouseKeepingDaemonInitDelay is the default delay time that status housekeeping daemon
	// must wait before start to work
	DefaultHouseKeepingDaemonInitDelay = time.Second * 5
	// DefaultResendEventsDaemonWaitBtwChecks is the default time that resend pending events daemon must wait
	// between run checks or do any action
	DefaultResendEventsDaemonWaitBtwChecks = time.Second * 30
	// DefaultResendEventsDaemonInitDelay is the default delay time that resend pending events daemon
	// must wait before start to work
	DefaultResendEventsDaemonInitDelay = time.Second * 20
	// DefaultMaxRecordsResendByFlush is the default value for the max numbers of pending events to resend
	// when Flush operation is called
	DefaultMaxRecordsResendByFlush = 1000
)

// NewReliableClientBuilder return ReliableClientBuilder with intialized to default values
func NewReliableClientBuilder() *ReliableClientBuilder {

	r := &ReliableClientBuilder{
		retryDaemonOpts:          daemonOpts{DefaultResendEventsDaemonWaitBtwChecks, DefaultResendEventsDaemonInitDelay},
		clientReconnOpts:         daemonOpts{DefaultClientReconnDaemonWaitBtwChecks, DefaultClientReconnDaemonInitDelay},
		houseKeepingDaemonOpts:   daemonOpts{DefaultHouseKeepingDaemonWaitBtwChecks, DefaultHouseKeepingDaemonInitDelay},
		daemonStopTimeout:        DefaultDaemonStopTimeout,
		enableStandByModeTimeout: DefaultEnableStandByModeTimeout,
		flushTimeout:             DefaultFlushAsyncTimeout,
		appLogger:                &applogger.NoLogAppLogger{},
		maxRecordsResendByFlush:  DefaultMaxRecordsResendByFlush,
	}

	return r
}

// StatusBuilder sets the required status builder engine to save load ans manage events
// in a reliable way
func (dsrcb *ReliableClientBuilder) StatusBuilder(sb status.Builder) *ReliableClientBuilder {
	dsrcb.statusBuilder = sb
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

// HouseKeepingDaemonWaitBtwChecks sets the interval time waited by daemon between executions
// of stattus HouseKeeping operations. Value is set only if d value is greater than 0
func (dsrcb *ReliableClientBuilder) HouseKeepingDaemonWaitBtwChecks(d time.Duration) *ReliableClientBuilder {
	if d > 0 {
		dsrcb.houseKeepingDaemonOpts.waitBtwChecks = d
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

// HouseKeepingDaemonInitDelay sets the initial time delay to wait before HouseKeepingDaemon
// launchs the first execution. Value is set only if d value is greater than 0
func (dsrcb *ReliableClientBuilder) HouseKeepingDaemonInitDelay(d time.Duration) *ReliableClientBuilder {
	if d > 0 {
		dsrcb.houseKeepingDaemonOpts.initDelay = d
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

// EnableStandByModeTimeout sets and enable if value is greter than 0, the timeout to wait
// for pending async events in client when StandBy() func is called
func (dsrcb *ReliableClientBuilder) EnableStandByModeTimeout(d time.Duration) *ReliableClientBuilder {
	dsrcb.enableStandByModeTimeout = d
	return dsrcb
}

// FlushTimeout sets the timeout when wait for pending async envents in clien when
// Flush() func is called
func (dsrcb *ReliableClientBuilder) FlushTimeout(d time.Duration) *ReliableClientBuilder {
	if d > 0 {
		dsrcb.flushTimeout = d
	}

	return dsrcb
}

// ClientBuilder sets the ClientBuilder needed to build the underhood client. This is required
// to initial setup and it is used by reconnect daemon too.
// If IsConnWorkingCheckPayload is not defined in cb ClientBuilder then \x00 VALUE WILL BE fixed
// to ensure that ClientReconn daemon can recreate connection after any outage
func (dsrcb *ReliableClientBuilder) ClientBuilder(cb *ClientBuilder) *ReliableClientBuilder {
	dsrcb.clientBuilder = cb
	if cb.isConnWorkingCheckPayload == "" {
		cb.IsConnWorkingCheckPayload("\x00")
	}
	return dsrcb
}

// AppLogger sets the AppLogger used to send logger messages in case that errors can not be
// returned. Debug traces can be saved usint this logger too.
func (dsrcb *ReliableClientBuilder) AppLogger(lg applogger.SimpleAppLogger) *ReliableClientBuilder {
	dsrcb.appLogger = lg
	return dsrcb
}

// MaxRecordsResendByFlush sets the max number of pending events to be resend when Flush events
// is called. Zero or negative values deactivate the functionallity
func (dsrcb *ReliableClientBuilder) MaxRecordsResendByFlush(max int) *ReliableClientBuilder {
	dsrcb.maxRecordsResendByFlush = max
	return dsrcb
}

// Build builds the ReliableClient based in current parameters.
func (dsrcb *ReliableClientBuilder) Build() (*ReliableClient, error) {
	// Check required config
	if dsrcb.statusBuilder == nil {
		return nil, fmt.Errorf("undefined status builder")
	}

	if dsrcb.clientBuilder == nil {
		return nil, fmt.Errorf("undefined inner client builder")
	}

	// Build inner Client
	cl, err := dsrcb.clientBuilder.Build()
	// we can continue in connection error scenario
	if err != nil && !isConnectionError(err) {
		return nil, err
	}

	r := &ReliableClient{
		Client:                      cl,
		clientBuilder:               dsrcb.clientBuilder, // We maybe need the builder when will need to recreate client
		retryDaemon:                 reliableClientDaemon{daemonOpts: dsrcb.retryDaemonOpts},
		reconnDaemon:                reliableClientDaemon{daemonOpts: dsrcb.clientReconnOpts},
		houseKeepingDaemon:          reliableClientDaemon{daemonOpts: dsrcb.houseKeepingDaemonOpts},
		daemonStopTimeout:           dsrcb.daemonStopTimeout,
		daemonStopped:               make(chan bool),
		flushTimeout:                dsrcb.flushTimeout,
		enableStandByModeTimeout:    dsrcb.enableStandByModeTimeout,
		maxRecordsResendInFlushCall: dsrcb.maxRecordsResendByFlush,
		appLogger:                   dsrcb.appLogger,
	}

	// Status DB
	r.status, err = dsrcb.statusBuilder.Build()
	if err != nil {
		return nil, fmt.Errorf("while load satus engine: %w", err)
	}

	// Ensure existing elements in status are marked as no-conn (to prevent daemons eviction)
	// Passing all remaining events to no-conn
	err = r.status.BatchUpdate(
		func(o string) string {
			if isNoConnID(o) {
				return ""
			}
			return newNoConnID()
		})
	if err != nil {
		return nil, fmt.Errorf("while mark remaining IDs as no-conn in status engine: %w", err)
	}

	// Daemons startup
	err = r.daemonsSartup()
	if err != nil {
		return r, fmt.Errorf("while initialize dameons: %w", err)
	}

	return r, nil
}

// ReliableClient defines a Client with Reliable capatilities for Async operations only
type ReliableClient struct {
	*Client
	clientBuilder               *ClientBuilder
	status                      status.Status
	retryDaemon                 reliableClientDaemon
	reconnDaemon                reliableClientDaemon
	houseKeepingDaemon          reliableClientDaemon
	daemonStopTimeout           time.Duration
	clientMtx                   sync.Mutex
	standByMode                 bool
	enableStandByModeTimeout    time.Duration
	daemonStopped               chan bool
	flushTimeout                time.Duration
	maxRecordsResendInFlushCall int
	lastFlushLimitReached       bool
	appLogger                   applogger.SimpleAppLogger
}

type reliableClientDaemon struct {
	daemonOpts
	stop bool
}

func (rcd reliableClientDaemon) String() string {
	return fmt.Sprintf(
		"{ waitBtwChecks: %v, initDelay: %v, stop: %v}",
		rcd.waitBtwChecks,
		rcd.initDelay,
		rcd.stop,
	)
}

// SendAsync sends Async message in same way like Client.SendAsync but saving the message
// in status until can ensure, at certain level of confiance, that it was sent
func (dsrc *ReliableClient) SendAsync(m string) string {
	var id string
	if dsrc.IsStandBy() || dsrc.Client == nil {
		id = newNoConnID()
	} else {
		id = dsrc.Client.SendAsync(m)
	}

	record := &status.EventRecord{
		AsyncIDs:  []string{id},
		Timestamp: time.Now(),
		Msg:       m,
	}
	err := dsrc.status.New(record)
	if err != nil {
		dsrc.appLogger.Logf(applogger.ERROR, "Uncontrolled error when create status record in SendAsync, ID: %s: %v", id, err)
	}

	return id
}

// SendWTagAsync sends Async message in same way like Client.SendWTagAsync but saving the message
// and tag in status until can ensure, at certain level of confiance, that it was sent
func (dsrc *ReliableClient) SendWTagAsync(t, m string) string {
	var id string
	if dsrc.IsStandBy() || dsrc.Client == nil {
		id = newNoConnID()
	} else {
		id = dsrc.Client.SendWTagAsync(t, m)
	}

	record := &status.EventRecord{
		AsyncIDs:  []string{id},
		Timestamp: time.Now(),
		Tag:       t,
		Msg:       m,
	}
	err := dsrc.status.New(record)
	if err != nil {
		dsrc.appLogger.Logf(applogger.ERROR, "Uncontrolled error when create status record in SendWTagAsync, ID: %s: %v", id, err)
	}

	return id
}

// SendWTagAndCompressorAsync sends Async message in same way like Client.SendWTagAndCompressorAsync
// but saving the message,tag and Compressor in status until can ensure, at certain level of
// confiance, that it was sent
func (dsrc *ReliableClient) SendWTagAndCompressorAsync(t string, m string, c *compressor.Compressor) string {
	var id string
	if dsrc.IsStandBy() || dsrc.Client == nil {
		id = newNoConnID()
	} else {
		id = dsrc.Client.SendWTagAndCompressorAsync(t, m, c)
	}

	record := &status.EventRecord{
		AsyncIDs:   []string{id},
		Timestamp:  time.Now(),
		Tag:        t,
		Msg:        m,
		Compressor: c,
	}
	err := dsrc.status.New(record)
	if err != nil {
		dsrc.appLogger.Logf(applogger.ERROR, "Uncontrolled error when create status record in SendWTagAndCompressorAsync, ID: %s: %v", id, err)
	}

	return id
}

// Flush checks all pending messages (sent with Async funcs), waits for pending async messages
// and update status of all of them. This func can call on demand but it is called by
// internal retry send events daemon too
func (dsrc *ReliableClient) Flush() error {
	dsrc.lastFlushLimitReached = false

	isClientUp := !dsrc.IsStandBy()
	if isClientUp {
		dsrc.clientMtx.Lock()
		isClientUp = !(dsrc.Client == nil)
		dsrc.clientMtx.Unlock()
	}

	// Recollect pending events
	allIds, err := dsrc.status.AllIDs()
	if err != nil {
		return fmt.Errorf("while findAllRecords before flush: %w", err)
	}

	if isClientUp {
		err = dsrc.WaitForPendingAsyncMsgsOrTimeout(dsrc.flushTimeout)
		if err != nil {
			return fmt.Errorf("timeout %s reached when wait for pending async msgs: %w", dsrc.flushTimeout, err)
		}

		idsToBeResend := map[string]error{}
		assumingWasSent := make([]string, 0)
		for _, id := range allIds {
			// If Id is no connecion
			if isNoConnID(id) {
				idsToBeResend[id] = nil
			} else {
				// Check if Id is not pending
				if !dsrc.IsAsyncActive(id) {
					// Load errors and check on it
					if ok, err := dsrc.AsyncError(id); ok {
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
		eventsSent := 0
		for k, v := range idsToBeResend {

			record, pos, err := dsrc.status.Get(k) // Evicted stats is managed by Get
			if errors.Is(err, status.ErrRecordEvicted) {
				// Ignored becasue we consider event as evicted
			} else if err != nil {
				return fmt.Errorf("while load record from status with id %s, order %d to be processed: %w", k, pos, err)
			} else {
				record.LastError = v
				err = dsrc.resendRecord(record)
				eventsSent++
				if dsrc.maxRecordsResendInFlushCall > 0 && eventsSent >= dsrc.maxRecordsResendInFlushCall {
					dsrc.lastFlushLimitReached = true

					dsrc.appLogger.Logf(
						applogger.WARNING,
						"Limit of max number of events to re-send while Flush (%d) reached", eventsSent,
					)

					break
				}

				if err != nil {
					return fmt.Errorf("while resend record with id %s: %w", k, err)
				}
			}
		}

		// Remove records were send
		for _, ID := range assumingWasSent {
			err = dsrc.status.FinishRecord(ID)
			if errors.Is(err, status.ErrRecordNotFoundInIdx) {
				// Special case event was sent before
				dsrc.appLogger.Logf(
					applogger.DEBUG,
					"Ignoring record %s after assuming that was send by reason: %v", ID, err,
				)
			} else if err != nil {
				return fmt.Errorf("while delete one status record that I assumed that was sent: %w", err)
			}
		}

	} else {
		// Passing all elemetns as no-conn
		for _, id := range allIds {
			// If Id is no connecion
			if !isNoConnID(id) {
				err = dsrc.status.Update(id, toNoConnID(id))
				if err != nil {
					return fmt.Errorf("while pass one status record with old id %s to no-conn state: %w", id, err)
				}
			}
		}
	}

	return nil
}

// IsLimitReachedLastFlush treturs true if there are pending events after flush
// because max limit was reached, or false in the other case.
func (dsrc *ReliableClient) IsLimitReachedLastFlush() bool {
	return dsrc.lastFlushLimitReached
}

// PendingEventsNoConn return the number of events that are pending to send
// and was created when the connection does not was available. -1 is returned
// when value could not be solved
func (dsrc *ReliableClient) PendingEventsNoConn() int {
	ids, err := dsrc.status.AllIDs()
	if err != nil {
		return -1
	}
	cont := 0
	for _, id := range ids {
		if isNoConnID(id) {
			cont++
		}
	}

	return cont
}

// Close closes current client. This implies operations like shutdown daemons, call Flush func, etc.
func (dsrc *ReliableClient) Close() error {
	errors := make([]error, 0)

	err := dsrc.Flush()
	if err != nil {
		errors = append(errors, fmt.Errorf("while flush events: %w", err))
	}

	err = dsrc.daemonsShutdown()
	if err != nil {
		errors = append(errors, fmt.Errorf("while shutdown daemons: %w", err))
	}

	if dsrc.Client != nil {
		dsrc.Client.Close()
		if err != nil {
			errors = append(errors, fmt.Errorf("while close client: %w", err))
		}
	}

	// Passing all remaining events to no-conn
	err = dsrc.status.BatchUpdate(
		func(o string) string {
			if isNoConnID(o) {
				return ""
			}
			return newNoConnID()
		})
	if err != nil {
		errors = append(errors, fmt.Errorf("while mark remaining IDs as no-conn in status engine: %w", err))
	}

	dsrc.status.Close()
	if err != nil {
		errors = append(errors, fmt.Errorf("while close status engine: %w", err))
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
				return fmt.Errorf("while wait for pending async operations, timeout %s: %w",
					dsrc.enableStandByModeTimeout, err)
			}
		}

		err := dsrc.Client.Close()
		if err != nil {
			dsrc.standByMode = true
			return fmt.Errorf("while close client passing to StandBy: %w", err)
		}
		// Destroy curret client to ensure it will be recreated when WakeUp
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
			return fmt.Errorf("while creating new client. StandByMode deactivated anyway: %w", err)
		}
	}

	return nil
}

// IsStandBy retursn true when client is in StandBy() mode
func (dsrc *ReliableClient) IsStandBy() bool {
	return dsrc.standByMode
}

// IsConnWorking is the same as Client.IsConnWorking but check first if client is in
// stand by mode
func (dsrc *ReliableClient) IsConnWorking() (bool, error) {
	if dsrc.IsStandBy() {
		return false, nil
	}

	return dsrc.Client.IsConnWorking()
}

// Stats returns the curren stats (session + persisted). Erros when load stas are ignored
// DbDataEntries and DbKeysSize will be filled only if DEVOGO_DEBUG_SENDER_STATS_COUNT_DATA environment variable
// is set with "yes" value
func (dsrc *ReliableClient) Stats() status.Stats {
	return dsrc.status.Stats()
}

func (dsrc *ReliableClient) String() string {

	statusStr := "<nil>"
	if dsrc.status != nil {
		statusStr = dsrc.status.String()
	}

	return fmt.Sprintf(
		"Client: {%s}, status: {%s}, retryDaemon: %v, "+
			"reconnDaemon: %v, houseKeepingDaemon: %v, daemonStopTimeout: %v, "+
			"standByMode: %v, enableStandByModeTimeout: %v, daemonStopped: %v, flushTimeout: %v",
		dsrc.Client.String(),
		statusStr,
		dsrc.retryDaemon,
		dsrc.reconnDaemon,
		dsrc.houseKeepingDaemon,
		dsrc.daemonStopTimeout,
		dsrc.standByMode,
		dsrc.enableStandByModeTimeout,
		dsrc.daemonStopped,
		dsrc.flushTimeout,
	)
}

// daemonsSartup perform init cleanup (only once) and starts the resend events and
// reconnect daemons, capture interrumnt and term signals to close database, etc...
func (dsrc *ReliableClient) daemonsSartup() error {
	if dsrc.status == nil {
		return fmt.Errorf("status engine is nil any setup action can not be done")
	}

	// Capture termination and close client
	go func() {
		sigchan := make(chan os.Signal, 1)
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
	err := dsrc.startRetryEventsDaemon()
	if err != nil {
		return fmt.Errorf("while starts retry event daemon: %w", err)
	}

	// Client reconnection
	err = dsrc.clientReconnectionDaemon()
	if err != nil {
		return fmt.Errorf("while starts client reconnection daemon: %w", err)
	}

	// status HouseKeeping
	err = dsrc.statusHouseKeepingDaemon()
	if err != nil {
		return fmt.Errorf("while starts statusHouseKeeping daemon: %w", err)
	}

	return nil
}

// daemonsShutdown tries to stop daemons in gracefull mode, grace period to wait for
// each daemon stopped is set in dsrc.daemonStopTimeout
func (dsrc *ReliableClient) daemonsShutdown() error {
	dsrc.retryDaemon.stop = true
	dsrc.reconnDaemon.stop = true
	dsrc.houseKeepingDaemon.stop = true

	errors := make([]error, 0)

	// three daemons
	for i := 0; i < 3; i++ {
		select {
		case <-time.After(dsrc.daemonStopTimeout):
			errors = append(errors, fmt.Errorf("timeout when wait for daemon number: %d", i))
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

	return fmt.Errorf("while shutdown daemons: %w", err)
}

// startRetryEventsDaemon runs in background the retry send events daemon. This daemon checks
// every dsr.retryWait time  the pending events and update status or resend it if error
// was saved by inner client. This actions are delegated to call Flush func
func (dsrc *ReliableClient) startRetryEventsDaemon() error {
	if dsrc.retryDaemon.waitBtwChecks <= 0 {
		return fmt.Errorf("time to wait between each check to retry events is not enough: %s",
			dsrc.retryDaemon.waitBtwChecks)
	}
	go func() {
		// Init delay
		daemonSleep(&(dsrc.retryDaemon), DefaultDaemonMicroWait, true)

		dsrc.appLogger.Logf(applogger.DEBUG, "startRetryEventsDaemon working: %+v", dsrc.retryDaemon)

		// Daemon loop
		for !dsrc.retryDaemon.stop {

			dsrc.appLogger.Logf(applogger.DEBUG, "startRetryEventsDaemon shot: %+v", dsrc.retryDaemon)

			err := dsrc.Flush()
			if err != nil {
				dsrc.appLogger.Logf(
					applogger.ERROR,
					"Error received while retryEventsDaemon flush client: %v", err,
				)
			}

			daemonSleep(&(dsrc.retryDaemon), DefaultDaemonMicroWait, false)
		}

		// Closed signal
		dsrc.daemonStopped <- true

	}()

	return nil
}

// clientReconnectionDaemon runs in background the reconnect  daemon. This daemon create new connection
// if ReliableClient is not in stand by mode and inner Client is nill or IsConnWorking returns false without
// ErrPayloadNoDefined.
func (dsrc *ReliableClient) clientReconnectionDaemon() error {
	if dsrc.reconnDaemon.waitBtwChecks <= 0 {
		return fmt.Errorf("time to wait between each check to reconnect client is not enough: %s", dsrc.reconnDaemon.waitBtwChecks)
	}
	go func() {
		// Init delay
		daemonSleep(&(dsrc.reconnDaemon), DefaultDaemonMicroWait, true)

		dsrc.appLogger.Logf(applogger.DEBUG, "clientReconnectionDaemon working: %+v", dsrc.reconnDaemon)

		for !dsrc.reconnDaemon.stop {

			dsrc.appLogger.Logf(applogger.DEBUG, "clientReconnectionDaemon shot: %+v", dsrc.reconnDaemon)

			dsrc.clientMtx.Lock()
			if !dsrc.IsStandBy() {
				recreate := false
				if dsrc.Client == nil {
					recreate = true
				} else if ok, err := dsrc.Client.IsConnWorking(); err != ErrPayloadNoDefined && !ok {
					recreate = true
				}

				// Build inner Client
				if recreate {
					var err error
					dsrc.Client, err = dsrc.clientBuilder.Build()
					// we can continue in connection error scenario
					if err != nil {
						dsrc.appLogger.Logf(
							applogger.ERROR,
							"Error While create new client in Reconnection daemon: %v", err,
						)
					}
				}
			}
			dsrc.clientMtx.Unlock()
			daemonSleep(&(dsrc.reconnDaemon), DefaultDaemonMicroWait, false)
		}

		// Closed signal
		dsrc.daemonStopped <- true
	}()

	return nil
}

const (
	houseKeepingWarnLimit = time.Second * 10
)

// statusHouseKeepingDaemon runs in background the status housekeeping daemon. This daemon execute
// periodically the status.HouseKeeping() method
func (dsrc *ReliableClient) statusHouseKeepingDaemon() error {
	if dsrc.houseKeepingDaemon.waitBtwChecks <= 0 {
		return fmt.Errorf("invalid time to wait between each HouseKeeping execution: %s", dsrc.houseKeepingDaemon.waitBtwChecks)
	}
	go func() {
		// Init delay
		daemonSleep(&(dsrc.houseKeepingDaemon), DefaultDaemonMicroWait, true)

		dsrc.appLogger.Logf(applogger.DEBUG, "statusHouseKeepingDaemon working: %+v", dsrc.houseKeepingDaemon)

		for !dsrc.houseKeepingDaemon.stop {

			dsrc.appLogger.Logf(applogger.DEBUG, "statusHouseKeepingDaemon shot: %+v", dsrc.houseKeepingDaemon)

			var err error
			beginTime := time.Now() // For warning if spended time is high
			if dsrc.status == nil {
				err = ErrNilPointerReceiver
			} else {
				var strBefore, strAfter string
				if dsrc.appLogger.IsLevelEnabled(applogger.DEBUG) {
					strBefore = dsrc.status.String()
				}

				err = dsrc.status.HouseKeeping()

				if dsrc.appLogger.IsLevelEnabled(applogger.DEBUG) {
					strAfter = dsrc.status.String()
					if strBefore != strAfter {
						dsrc.appLogger.Logf(applogger.DEBUG, "Status HouseKeeping results: Before: %s, After: %s", strBefore, strAfter)
					}
				}
			}
			endTime := time.Now()

			thresold := beginTime.Add(houseKeepingWarnLimit)
			// Spend time warning
			if thresold.Before(endTime) {
				dsrc.appLogger.Logf(
					applogger.WARNING,
					"Spent time by status.HouseKeeping (begin: %s, end: %s, threshold: %v) was greater than warning limit: %s",
					beginTime.Format(time.RFC3339Nano),
					endTime.Format(time.RFC3339Nano),
					thresold,
					houseKeepingWarnLimit,
				)
			}

			if err != nil {
				dsrc.appLogger.Logf(
					applogger.ERROR,
					"Error While perform status.HouseKeeping in statusHouseKeepingDaemon: %v", err,
				)
			}

			daemonSleep(&(dsrc.houseKeepingDaemon), DefaultDaemonMicroWait, false)
		}

		// Closed signal
		dsrc.daemonStopped <- true
	}()

	return nil
}

// daemonSleep is a helper tool that extends simple time.Sleep. This func sleeps a total
// dbOpts.waitBtwChecks time, but check in microSleep intervals if dbOpts.stop is true
// (stopping on this case) and return this value at the end.
// if initdDelay is true sleep time used will be dOpts.initDelay instead of
// dOpts.waitBtwChecks
func daemonSleep(dOpts *reliableClientDaemon, microSleep time.Duration, initDelay bool) bool {
	sleep := dOpts.waitBtwChecks
	if initDelay {
		sleep = dOpts.initDelay
	}

	// No sleep time
	if sleep <= 0 {
		return dOpts.stop
	}

	// sleep time is less than microSleep or microSleep is not enough
	if sleep <= microSleep || microSleep <= 0 {
		time.Sleep(sleep)
		return dOpts.stop
	}

	// Sleep in micro sleeps checkin dOpts.stop
	for sleep > 0 && !dOpts.stop {
		if sleep < microSleep {
			// Remaining time
			time.Sleep(sleep)
		} else {
			// Other iteration
			time.Sleep(microSleep)
		}
		sleep -= microSleep
	}

	return dOpts.stop
}

// resendRecord send the event based on record status.
func (dsrc *ReliableClient) resendRecord(r *status.EventRecord) error {
	var newID string
	if dsrc.IsStandBy() || dsrc.Client == nil {
		currID := r.AsyncIDs[len(r.AsyncIDs)-1]
		if isNoConnID(currID) {
			// Same id, noting to do because client is not active
			return nil
		}
		// Pass record to non-connection
		newID = newNoConnID()

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

	err := dsrc.status.Update(r.EffectiveID(), newID)
	if err != nil {
		return fmt.Errorf("while update status record after resend with newID %s: %w", newID, err)
	}

	return nil
}
