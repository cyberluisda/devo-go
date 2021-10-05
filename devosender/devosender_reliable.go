package devosender

import (
	"math"
	"sync"
	"time"

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
