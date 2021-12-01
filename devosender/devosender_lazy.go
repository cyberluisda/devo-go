package devosender

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"

	"github.com/cyberluisda/devo-go/applogger"
)

// SwitchDevoSender represents a Client that can be paused. That is that can close
// connection to Devo and save in a buffer the events that are recieved. When client is waked up
// pending events are send to Devo.
type SwitchDevoSender interface {
	io.Closer
	String() string
	SendAsync(m string) string
	SendWTagAsync(t, m string) string
	SendWTagAndCompressorAsync(t string, m string, c *Compressor) string
	WaitForPendingAsyncMsgsOrTimeout(timeout time.Duration) error
	AsyncErrors() map[string]error
	AsyncErrorsNumber() int
	AsyncIds() []string
	AreAsyncOps() bool
	IsAsyncActive(id string) bool
	Flush() error
	StandBy() error
	WakeUp() error
	IsStandBy() bool
}

// LazyClientBuilder is the builder to build LazyClient
type LazyClientBuilder struct {
	clientBuilder            *ClientBuilder
	bufferEventsSize         uint32
	enableStandByModeTimeout time.Duration
	flushTimeout             time.Duration
	appLogger                applogger.SimpleAppLogger
}

const (
	// LCBDefaultBufferEventsSize is the default BufferSize value set in LazyClientBuilder
	// when it is created with NewLazyClientBuilder
	LCBDefaultBufferEventsSize uint32 = 256000
	// LCBDefaultFlushTimeout is the default FlushTimeout value set in LazyClientBuilder
	// when it is created with NewLazyClientBuilder
	LCBDefaultFlushTimeout = time.Second * 2
)

// NewLazyClientBuilder is the factory method of LazyClientBuilder with defautl values set
func NewLazyClientBuilder() *LazyClientBuilder {
	return &LazyClientBuilder{
		bufferEventsSize: LCBDefaultBufferEventsSize,
		flushTimeout:     LCBDefaultFlushTimeout,
	}
}

// BufferSize sets the buffer size if s is greater than 0
func (lcb *LazyClientBuilder) BufferSize(s uint32) *LazyClientBuilder {
	if s > 0 {
		lcb.bufferEventsSize = s
	}
	return lcb
}

// ClientBuilder sets the ClientBuilder used to re-create connections after
// StandyBy - WakeUp situation
func (lcb *LazyClientBuilder) ClientBuilder(cb *ClientBuilder) *LazyClientBuilder {
	lcb.clientBuilder = cb
	return lcb
}

// EnableStandByModeTimeout sets and enable if value is greater than 0, the timeout to wait
// for pending async events in client when StandBy() func is called
func (lcb *LazyClientBuilder) EnableStandByModeTimeout(d time.Duration) *LazyClientBuilder {
	lcb.enableStandByModeTimeout = d
	return lcb
}

// FlushTimeout sets the timeout when wait for pending async envents in client when
// Flush() func is called. Timeout is set only if parameter is greater than 0
func (lcb *LazyClientBuilder) FlushTimeout(d time.Duration) *LazyClientBuilder {
	if d >= 0 {
		lcb.flushTimeout = d
	}
	return lcb
}

// Build creates new LazyClient instance
func (lcb *LazyClientBuilder) Build() (*LazyClient, error) {
	// Validations
	if lcb.clientBuilder == nil {
		return nil, errors.New("Undefined inner client builder")
	}

	if lcb.bufferEventsSize < 1 {
		return nil, errors.New("Buffer size less than 1")
	}
	if lcb.flushTimeout < 1 {
		return nil, errors.New("Flush timeout empty or negative")
	}

	// Default values
	if lcb.appLogger == nil {
		lcb.appLogger = &applogger.NoLogAppLogger{}
	}

	client, err := lcb.clientBuilder.Build()
	if err != nil {
		return nil, fmt.Errorf("Error while initialize client: %v", err)
	}

	// Create LazyClient
	r := &LazyClient{
		Client:       client,
		bufferSize:   lcb.bufferEventsSize,
		flushTimeout: lcb.flushTimeout,
		buffer:       []lazyClientRecord{},
		appLogger:    lcb.appLogger,
	}

	return r, nil
}

// LazyClient is a SwitchDevoSender that save events in a buffer when it is in "stand by" mode.
// Events are saved in a circular buffer, and when limit of buffer size is reached, new arrived
// events are saved at the begining of the buffer.
// when WakeUp is called, all events in buffer as send using Client.Async funcs in starting for first
// element in slide. This implies that arrived event order can not be maintained if buffer suffered
// an "oversize" situation.
type LazyClient struct {
	*Client
	clientBuilder *ClientBuilder
	bufferSize    uint32
	flushTimeout  time.Duration
	buffer        []lazyClientRecord
	appLogger     applogger.SimpleAppLogger
	clientMtx     sync.Mutex
}

// lazyClientRecord is the internal structure to save in memory of the events while
// client is in standby mode
// and allow operations like resend.
type lazyClientRecord struct {
	AsyncID    string
	Timestamp  time.Time
	Tag        string
	Msg        string
	Compressor *Compressor
	LastError  error
}

func (lc *LazyClient) IsStandBy() bool {
	lc.clientMtx.Lock()
	r := lc.isStandByUnlocked()
	lc.clientMtx.Unlock()
	return r
}

func (lc *LazyClient) isStandByUnlocked() bool {
	return lc.Client == nil
}

const nonConnIDPrefix = "non-conn-"

var nonConnIDPrefixBytes = []byte(nonConnIDPrefix)

func newNoConnID() string {
	return nonConnIDPrefix + uuid.NewV4().String()
}

func isNoConnID(id string) bool {
	return strings.HasPrefix(id, nonConnIDPrefix)
}

func isNoConnIDBytes(id []byte) bool {
	return bytes.HasPrefix(id, nonConnIDPrefixBytes)
}

func toNoConnID(id string) string {
	return nonConnIDPrefix + id
}
