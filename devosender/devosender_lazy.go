package devosender

import (
	"io"
	"time"
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
