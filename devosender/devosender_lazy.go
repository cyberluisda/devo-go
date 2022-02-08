package devosender

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"

	"github.com/cyberluisda/devo-go/applogger"
	"github.com/cyberluisda/devo-go/devosender/compressor"
)

// SwitchDevoSender represents a Client that can be paused. That is that can close
// connection to Devo and save in a buffer the events that are recieved. When client is waked up
// pending events are send to Devo.
type SwitchDevoSender interface {
	io.Closer
	String() string
	SendAsync(m string) string
	SendWTagAsync(t, m string) string
	SendWTagAndCompressorAsync(t string, m string, c *compressor.Compressor) string
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
	LastSendCallTimestamp() time.Time
}

// LazyClientBuilder is the builder to build LazyClient
type LazyClientBuilder struct {
	clientBuilder            *ClientBuilder
	bufferEventsSize         uint32
	enableStandByModeTimeout time.Duration
	flushTimeout             time.Duration
	maxRecordsResendByFlush  int
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
		bufferEventsSize:        LCBDefaultBufferEventsSize,
		flushTimeout:            LCBDefaultFlushTimeout,
		maxRecordsResendByFlush: DefaultMaxRecordsResendByFlush,
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
	if d > 0 {
		lcb.flushTimeout = d
	}
	return lcb
}

// MaxRecordsResendByFlush sets the max number of pending events to be resend when Flush events
// is called. Zero or negative values deactivate the functionallity
func (lcb *LazyClientBuilder) MaxRecordsResendByFlush(max int) *LazyClientBuilder {
	lcb.maxRecordsResendByFlush = max
	return lcb
}

// AppLogger sets the applogger.SimpleAppLogger used to write log messages
func (lcb *LazyClientBuilder) AppLogger(log applogger.SimpleAppLogger) *LazyClientBuilder {
	if log != nil {
		lcb.appLogger = log
	}
	return lcb
}

// Build creates new LazyClient instance
func (lcb *LazyClientBuilder) Build() (*LazyClient, error) {
	// Validations
	if lcb.clientBuilder == nil {
		return nil, errors.New("undefined inner client builder")
	}

	if lcb.bufferEventsSize < 1 {
		return nil, errors.New("buffer size less than 1")
	}
	if lcb.flushTimeout < 1 {
		return nil, errors.New("flush timeout empty or negative")
	}

	// Default values
	if lcb.appLogger == nil {
		lcb.appLogger = &applogger.NoLogAppLogger{}
	}

	client, err := lcb.clientBuilder.Build()
	if err != nil {
		return nil, fmt.Errorf("while initialize client: %w", err)
	}

	// Create LazyClient
	r := &LazyClient{
		Client:           client,
		bufferSize:       lcb.bufferEventsSize,
		flushTimeout:     lcb.flushTimeout,
		standByTimeout:   lcb.enableStandByModeTimeout,
		appLogger:        lcb.appLogger,
		clientBuilder:    lcb.clientBuilder,
		maxRecordsResend: lcb.maxRecordsResendByFlush,
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
	clientBuilder    *ClientBuilder
	bufferSize       uint32
	flushTimeout     time.Duration
	standByTimeout   time.Duration
	buffer           []*lazyClientRecord
	appLogger        applogger.SimpleAppLogger
	clientMtx        sync.Mutex
	maxRecordsResend int
	Stats            LazyClientStats
}

// lazyClientRecord is the internal structure to save in memory of the events while
// client is in standby mode
// and allow operations like resend.
type lazyClientRecord struct {
	AsyncID    string
	Timestamp  time.Time
	Tag        string
	Msg        string
	Compressor *compressor.Compressor
	LastError  error
}

func (lc *LazyClient) String() string {
	return fmt.Sprintf(
		"bufferSize: %d, standByMode: %t, #eventsInBuffer: %d, flushTimeout: %v, standByModeTimeout: %v, Client: {%v}",
		lc.bufferSize,
		lc.IsStandBy(),
		len(lc.buffer),
		lc.flushTimeout,
		lc.standByTimeout,
		lc.Client,
	)
}

// StandBy closes connection to Devo and pass the Client to admit Async send events calls only
// These events will be saved in a buffer.
// Client will wait for Pending events before to pass to StandBy, if timeout is triggered during
// this operation, error will be returned and stand by mode will not be active
func (lc *LazyClient) StandBy() error {
	if !lc.IsStandBy() {
		lc.clientMtx.Lock()

		var err error
		if lc.standByTimeout > 0 {
			err = lc.WaitForPendingAsyncMsgsOrTimeout(lc.standByTimeout)
		} else {
			err = lc.WaitForPendingAsyncMessages()
		}
		if err != nil {
			lc.clientMtx.Unlock()
			return fmt.Errorf("while wait for pending async messages: %w", err)
		}

		// Closing current client
		err = lc.Client.Close()
		if err != nil {
			lc.appLogger.Logf(
				applogger.WARNING,
				"Error while close inner client. Uninstantiate client anyway: %v", err,
			)
		}
		lc.Client = nil
		lc.clientMtx.Unlock()
	}
	return nil
}

// IsStandBy returns true if client is in STandBy mode or false in otherwise
func (lc *LazyClient) IsStandBy() bool {
	lc.clientMtx.Lock()
	r := lc.isStandByUnlocked()
	lc.clientMtx.Unlock()
	return r
}

func (lc *LazyClient) isStandByUnlocked() bool {
	return lc.Client == nil
}

// WakeUp leave stand by mode creating new connection to Devo. Any action will be done
// if Client is not in stand-by mode.
func (lc *LazyClient) WakeUp() error {
	if lc.IsStandBy() {
		lc.clientMtx.Lock()

		client, err := lc.clientBuilder.Build()
		if err != nil {
			lc.clientMtx.Unlock()
			return fmt.Errorf("while (re)create client: %w", err)
		}
		lc.Client = client
		lc.clientMtx.Unlock()

		// Flush will send pending events
		err = lc.Flush()
		if err != nil {
			return fmt.Errorf("while flush pending events: %w", err)
		}
	}
	return nil
}

// Flush send all events in buffer only if client is not in stand by mode (IsStandBy == false)
func (lc *LazyClient) Flush() error {
	if !lc.IsStandBy() {
		lc.clientMtx.Lock()

		// Send events
		newIDs := make([]string, len(lc.buffer))
		events := 0
		for i, event := range lc.buffer {
			newID := lc.Client.SendWTagAndCompressorAsync(event.Tag, event.Msg, event.Compressor)
			newIDs[i] = newID
			events++
			if lc.maxRecordsResend > 0 && events >= lc.maxRecordsResend {
				lc.appLogger.Logf(
					applogger.WARNING,
					"Limit of max number of events to re-send while Flush (%d) reached", events,
				)

				break
			}

			lc.Stats.SendFromBuffer++ // Update stats
		}

		lc.appLogger.Logf(applogger.DEBUG, "IDS of events send from buffer: %v", newIDs)

		err := lc.Client.WaitForPendingAsyncMsgsOrTimeout(lc.flushTimeout)
		if err != nil {
			lc.clientMtx.Unlock()
			return fmt.Errorf("while waiting for pending messages: %w", err)
		}

		lc.resetBuffer()
		lc.clientMtx.Unlock()
	}
	return nil
}

// Close send all events forcing WakeUp if it is in stand-by mode and then close the client
func (lc *LazyClient) Close() error {
	err := lc.WakeUp()
	if err != nil {
		return fmt.Errorf("while WakeUp client: %w", err)
	}

	err = lc.Flush()
	if err != nil {
		return fmt.Errorf("while Flush events: %w", err)
	}

	err = lc.StandBy()
	if err != nil {
		return fmt.Errorf("while pass to StandBy to force close inner client: %w", err)
	}

	return nil
}

// SendWTagAndCompressorAsync is the same as Client.SendWTagAndCompressorAsync but if the
// Lazy Client is in stand-by mode then the event is saved in buffer
func (lc *LazyClient) SendWTagAndCompressorAsync(t, m string, c *compressor.Compressor) string {
	lc.clientMtx.Lock()
	defer lc.clientMtx.Unlock()

	lc.Stats.AsyncEvents++ // Update stats

	saveRecord := false
	if lc.isStandByUnlocked() {
		saveRecord = true
	} else {
		// Check if client needs to be restarted
		if ok, err := lc.IsConnWorking(); err != ErrPayloadNoDefined && !ok {
			lc.Client.Close() // Close ignoring errors
			lc.Client = nil   // Passing to standby mode and easy gc

			// New client
			client, err := lc.clientBuilder.Build()
			if err != nil {
				saveRecord = true
			} else {
				lc.Client = client
			}
		}
	}

	var r string
	if saveRecord {
		// Save in buffer
		r = newNoConnID()
		record := &lazyClientRecord{
			AsyncID:    r,
			Timestamp:  time.Now(),
			Tag:        t,
			Msg:        m,
			Compressor: c,
		}
		err := lc.addToBuffer(record)
		if err == ErrBufferOverflow {
			lc.appLogger.Log(
				applogger.WARNING,
				"Message discarted from buffer because buffer is full",
			)
			lc.Stats.BufferedLost++ // Update stats
		}
		lc.Stats.TotalBuffered++ // Update stats
	} else {
		r = lc.Client.SendWTagAndCompressorAsync(t, m, c)
	}

	return r
}

// SendWTagAsync is the same as Client.SendWTagAsync but if the
// Lazy Client is in stand-by mode then the event is saved in buffer
func (lc *LazyClient) SendWTagAsync(t, m string) string {
	var compressor *compressor.Compressor
	if lc.Client != nil {
		compressor = lc.compressor
	}
	return lc.SendWTagAndCompressorAsync(t, m, compressor)
}

// SendAsync is the same as Client.SendAsync but if the
// Lazy Client is in stand-by mode then the event is saved in buffer
func (lc *LazyClient) SendAsync(m string) string {
	return lc.SendWTagAsync(lc.defaultTag, m)
}

var (
	// ErrBufferOverflow is the error returned when buffer is full and element was lost
	ErrBufferOverflow = errors.New("overwriting item(s) because buffer is full")
	//ErrBufferFull is the error returned when buffer is full and any other action taken
	ErrBufferFull = errors.New("buffer is full")
)

func (lc *LazyClient) addToBuffer(r *lazyClientRecord) error {
	var err error
	lc.buffer = append(lc.buffer, r)
	if int(lc.bufferSize) < len(lc.buffer) {
		err = ErrBufferOverflow
		// Removing first one (is the older one)
		lc.buffer[0] = nil // required to prevent memory leaks
		lc.buffer = lc.buffer[1:]
	}
	return err
}

func (lc *LazyClient) popBuffer() (*lazyClientRecord, bool) {
	if len(lc.buffer) == 0 {
		return nil, false
	}

	r := lc.buffer[0]
	// Removing first one (is the older one)
	lc.buffer[0] = nil // required to prevent memory leaks
	lc.buffer = lc.buffer[1:]

	if len(lc.buffer) == 0 {
		// Purge buffer
		lc.buffer = nil
	}

	return r, true
}

func (lc *LazyClient) undoPopBuffer(r *lazyClientRecord) error {
	if r == nil {
		return nil
	}

	if int(lc.bufferSize) == len(lc.buffer) {
		return ErrBufferFull
	}

	// Prepend implementation based on append and copy (https://stackoverflow.com/a/53737602/14506791)
	lc.buffer = append(lc.buffer, nil)
	copy(lc.buffer[1:], lc.buffer)
	lc.buffer[0] = nil // gc helps
	lc.buffer[0] = r

	return nil
}

func (lc *LazyClient) resetBuffer() {
	// Clean buffer
	for i := 0; i < len(lc.buffer); i++ {
		lc.buffer[i] = nil // To prevent memory leaks
	}
	lc.buffer = nil
}

// LazyClientStats is the metrics storage for LazyClient
type LazyClientStats struct {
	AsyncEvents    uint
	BufferedLost   uint
	TotalBuffered  uint
	SendFromBuffer uint
}

func (lcs LazyClientStats) String() string {
	return fmt.Sprintf(
		"AsyncEvents: %d, TotalBuffered: %d, BufferedLost: %d, SendFromBuffer: %d",
		lcs.AsyncEvents, lcs.TotalBuffered, lcs.BufferedLost, lcs.SendFromBuffer)
}

const nonConnIDPrefix = "non-conn-"

func newNoConnID() string {
	return nonConnIDPrefix + uuid.NewV4().String()
}

func isNoConnID(id string) bool {
	return strings.HasPrefix(id, nonConnIDPrefix)
}

func toNoConnID(id string) string {
	return nonConnIDPrefix + id
}
