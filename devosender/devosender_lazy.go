package devosender

import (
	"io"
	"sync"
	"time"

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
