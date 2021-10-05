package devosender

import (
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
