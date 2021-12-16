/*
Package devosender implements the tools to send data to Devo in a different ways and scenarios

Interfaces to grant abstraction between implementations are defined and complex objects has associated their own builder
*/
package devosender

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/satori/go.uuid"
)

//DevoSender interface define the minimum behaviour required for Send data to Devo
type DevoSender interface {
	io.WriteCloser
	Send(m string) error
	SetDefaultTag(t string) error
	SendWTag(t, m string) error
	SendAsync(m string) string
	SendWTagAsync(t, m string) string
	WaitForPendingAsyncMessages() error
	AsyncErrors() map[string]error
	AsyncErrorsNumber() int
	PurgeAsyncErrors()
	GetEntryPoint() string
	AreAsyncOps() bool
	AsyncIds() []string
	IsAsyncActive(id string) bool
	AsyncsNumber() int
	LastSendCallTimestamp() time.Time
	String() string
}

const (
	// DevoCentralRelayUS is the public entrypoint of Devo central-relay on USA site
	DevoCentralRelayUS = "tcp://us.elb.relay.logtrust.net:443"
	// DevoCentralRelayEU is the public entrypoint of Devo central-relay on Europe site
	DevoCentralRelayEU = "tcp://eu.elb.relay.logtrust.net:443"
	// DefaultSyslogLevel is the code for facility and level used at raw syslog protocol. <14> = facility:user and level:info
	DefaultSyslogLevel = "<14>"

	// ClientBuilderRelayUS select DevoCentralRelayUS in builder
	ClientBuilderRelayUS ClienBuilderDevoCentralRelay = iota
	// ClientBuilderRelayEU select DevoCentralRelayEU in builder
	ClientBuilderRelayEU

	// ClientBuilderDefaultCompressorMinSize is the default min size of payload to apply compression
	// Following discussion in https://webmasters.stackexchange.com/questions/31750/what-is-recommended-minimum-object-size-for-gzip-performance-benefits
	// this value is set by CDN providers as Akamai, other services like goolge are more
	// aggrsive and set 150 bytes as fringe value.
	ClientBuilderDefaultCompressorMinSize = 860
)

// ClientBuilder defines builder for easy DevoSender instantiation
type ClientBuilder struct {
	entrypoint                string
	key, cert, chain          []byte
	keyFileName, certFileName string
	chainFileName             *string
	tlsInsecureSkipVerify     bool
	tlsRenegotiation          tls.RenegotiationSupport
	tcpTimeout                time.Duration
	tcpKeepAlive              time.Duration
	connExpiration            time.Duration
	compressorAlgorithm       CompressorAlgorithm
	compressorMinSize         int
	defaultDevoTag            string
}

// ClienBuilderDevoCentralRelay is the type used to set Devo central relay as entrypoint
type ClienBuilderDevoCentralRelay int

// NewClientBuilder returns new DevoSenderBuilder
func NewClientBuilder() *ClientBuilder {
	return &ClientBuilder{
		tlsInsecureSkipVerify: false,
		tlsRenegotiation:      tls.RenegotiateNever,
		compressorMinSize:     ClientBuilderDefaultCompressorMinSize,
	}
}

// EntryPoint sets entrypoint in builder used to create Client
// This value overwrite (and is overwritten) by DevoCentralEntryPoint
func (dsb *ClientBuilder) EntryPoint(entrypoint string) *ClientBuilder {
	dsb.entrypoint = entrypoint
	return dsb
}

// TLSFiles sets keys and certs from files used to make Client using TLS connection
// TLSCerts overwrites calls to this method
func (dsb *ClientBuilder) TLSFiles(keyFileName string, certFileName string, chainFileName *string) *ClientBuilder {
	dsb.keyFileName, dsb.certFileName, dsb.chainFileName = keyFileName, certFileName, chainFileName
	return dsb
}

// TLSCerts sets keys and certs used to make Client using TLS connection
// Call to this method overwrite TLSFiles
func (dsb *ClientBuilder) TLSCerts(key []byte, cert []byte, chain []byte) *ClientBuilder {
	dsb.key, dsb.cert, dsb.chain = key, cert, chain
	return dsb
}

// TLSInsecureSkipVerify sets InsecureSkipFlag, this value is used only with TLS connetions
func (dsb *ClientBuilder) TLSInsecureSkipVerify(insecureSkipVerify bool) *ClientBuilder {
	dsb.tlsInsecureSkipVerify = insecureSkipVerify
	return dsb
}

// TLSRenegotiation sets tlsRenegotiation support, this value is used only with TLS connections
func (dsb *ClientBuilder) TLSRenegotiation(renegotiation tls.RenegotiationSupport) *ClientBuilder {
	dsb.tlsRenegotiation = renegotiation
	return dsb
}

// DevoCentralEntryPoint Set One of the available Devo cental relays.
// This value overwrite (and is overwritten) by EntryPoint
func (dsb *ClientBuilder) DevoCentralEntryPoint(relay ClienBuilderDevoCentralRelay) *ClientBuilder {
	if relay == ClientBuilderRelayEU {
		dsb.EntryPoint(DevoCentralRelayEU)
	} else if relay == ClientBuilderRelayUS {
		dsb.EntryPoint(DevoCentralRelayUS)
	}
	return dsb
}

//TCPTimeout allow to set Timeout value configured in net.Dialer
func (dsb *ClientBuilder) TCPTimeout(t time.Duration) *ClientBuilder {
	dsb.tcpTimeout = t
	return dsb
}

//TCPKeepAlive allow to set KeepAlive value configured in net.Dialer
func (dsb *ClientBuilder) TCPKeepAlive(t time.Duration) *ClientBuilder {
	dsb.tcpKeepAlive = t
	return dsb
}

// ConnectionExpiration set expiration time used to recreate connection from last time was used
func (dsb *ClientBuilder) ConnectionExpiration(t time.Duration) *ClientBuilder {
	dsb.connExpiration = t
	return dsb
}

// DefaultCompressor set and enable ompression when send messages
func (dsb *ClientBuilder) DefaultCompressor(c CompressorAlgorithm) *ClientBuilder {
	dsb.compressorAlgorithm = c
	return dsb
}

// CompressorMinSize set the minium size to be applied when compress data.
func (dsb *ClientBuilder) CompressorMinSize(s int) *ClientBuilder {
	dsb.compressorMinSize = s
	return dsb
}

// DefaultDevoTag set the default tag to be used when send data to Devo in target client
func (dsb *ClientBuilder) DefaultDevoTag(t string) *ClientBuilder {
	dsb.defaultDevoTag = t
	return dsb
}

// ParseDevoCentralEntrySite returns ClientBuilderDevoCentralRelay based on site code.
// valid codes are 'US' and 'EU'
func ParseDevoCentralEntrySite(s string) (ClienBuilderDevoCentralRelay, error) {
	if strings.EqualFold("US", s) {
		return ClientBuilderRelayUS, nil
	} else if strings.EqualFold("EU", s) {
		return ClientBuilderRelayEU, nil
	} else {
		return 0, fmt.Errorf("Site '%s' is not valid", s)
	}
}

// Build implements build method of builder returning Client instance.
func (dsb *ClientBuilder) Build() (*Client, error) {
	//TLS
	var TLSSetup *tlsSetup
	if dsb.keyFileName != "" && dsb.certFileName != "" {
		// certs from files
		var err error
		dsb.key, dsb.cert, dsb.chain, err = loadTLSFiles(dsb.keyFileName, dsb.certFileName, dsb.chainFileName)
		if err != nil {
			return nil, fmt.Errorf("Error when prepare TLS connection using key file name and cert file name: %w", err)
		}
	}
	if len(dsb.key) != 0 && len(dsb.cert) != 0 {
		// TLS enabled
		TLSSetup = &tlsSetup{
			tlsConfig: &tls.Config{
				InsecureSkipVerify: dsb.tlsInsecureSkipVerify,
				Renegotiation:      dsb.tlsRenegotiation,
			},
		}

		// Create pool with chain cert
		pool := x509.NewCertPool()
		if len(dsb.chain) > 0 {
			ok := pool.AppendCertsFromPEM(dsb.chain)
			if !ok {
				return nil, fmt.Errorf("Could not parse chain certificate, content %s", string(dsb.chain))
			}
			TLSSetup.tlsConfig.RootCAs = pool
		}

		// Load key and certificate
		crts, err := tls.X509KeyPair(dsb.cert, dsb.key)
		if err != nil {
			return nil, fmt.Errorf("Error when load key and cert: %w", err)
		}
		TLSSetup.tlsConfig.Certificates = []tls.Certificate{crts}
		TLSSetup.tlsConfig.BuildNameToCertificate()
	}

	// Create client
	result := Client{
		ReplaceSequences: make(map[string]string),
		tls:              TLSSetup,
		entryPoint:       dsb.entrypoint,
		asyncErrors:      make(map[string]error),
		tcp: tcpConfig{
			tcpDialer: &net.Dialer{
				Timeout:   dsb.tcpTimeout,
				KeepAlive: dsb.tcpKeepAlive,
			},
		},
		maxTimeConnActive: dsb.connExpiration,
		asyncItems:        make(map[string]interface{}),
		defaultTag:        dsb.defaultDevoTag,
	}

	err := result.makeConnection()
	if err != nil {
		newErr := &connectionError{"Clear", err}
		if TLSSetup != nil {
			newErr.Mode = "TLS"
		}
		return nil, newErr
	}

	// compressor, only if NoCompression algorithm is selected
	if dsb.compressorAlgorithm > 0 {
		result.compressor = &Compressor{dsb.compressorAlgorithm, dsb.compressorMinSize}
	}

	// Intialize default values
	result.init()

	return &result, nil
}

type connectionError struct {
	Mode string
	Err  error
}

func (ce *connectionError) Error() string {
	return fmt.Sprintf("Error when create new DevoSender (%s): %v", ce.Mode, ce.Err)
}

func isConnectionError(e error) bool {
	var ce *connectionError
	return errors.As(e, &ce)
}

// NewDevoSenderTLS create TLS connection using ClientBuiler with minimal configuration
func NewDevoSenderTLS(entrypoint string, key []byte, cert []byte, chain []byte) (DevoSender, error) {
	return NewClientBuilder().
		EntryPoint(entrypoint).
		TLSCerts(key, cert, chain).
		Build()
}

// NewDevoSenderTLSFiles is similar to NewDevoSenderTLS but loading different certificates from files
func NewDevoSenderTLSFiles(entrypoint string, keyFileName string, certFileName string, chainFileName *string) (DevoSender, error) {
	return NewClientBuilder().
		EntryPoint(entrypoint).
		TLSFiles(keyFileName, certFileName, chainFileName).
		Build()
}

// NewDevoSender Create new DevoSender with clean comunication using ClientBuilder
// entrypoint is the Devo entrypoint where send events with protocol://fqdn:port format. You can use DevoCentralRelayXX constants to easy assign these value
func NewDevoSender(entrypoint string) (DevoSender, error) {
	return NewClientBuilder().
		EntryPoint(entrypoint).
		Build()
}

// Client is the engine that can send data to Devo throug central (tls) or in-house (clean) realy
type Client struct {
	entryPoint              string
	syslogHostname          string
	defaultTag              string
	conn                    net.Conn
	ReplaceSequences        map[string]string
	tls                     *tlsSetup
	waitGroup               sync.WaitGroup
	asyncErrors             map[string]error
	asyncErrorsMutext       sync.Mutex
	tcp                     tcpConfig
	connectionUsedTimestamp time.Time
	connectionUsedTSMutext  sync.Mutex
	maxTimeConnActive       time.Duration
	asyncItems              map[string]interface{}
	asyncItemsMutext        sync.Mutex
	lastSendCallTimestamp   time.Time
	statsMutex              sync.Mutex
	compressor              *Compressor
	isConnWorkingPayload    []byte
}

type tlsSetup struct {
	tlsConfig *tls.Config
}
type tcpConfig struct {
	tcpDialer *net.Dialer
}

// ErrNilPointerReceiver is the error returned when received funcs are call over nil pointer
var ErrNilPointerReceiver = errors.New("Receiver func call with nil pointer")

// SetSyslogHostName overwrite hostname send in raw Syslog payload
func (dsc *Client) SetSyslogHostName(host string) {
	if dsc == nil {
		return
	}
	if host == "" {
		var err error
		dsc.syslogHostname, err = os.Hostname()
		if err != nil {
			dsc.syslogHostname = "default"
		}
	} else {
		dsc.syslogHostname = host
	}
}

// SetDefaultTag set tag used when call funcs to send messages without splicit tag
func (dsc *Client) SetDefaultTag(t string) error {
	if dsc == nil {
		return ErrNilPointerReceiver
	}

	if t == "" {
		return fmt.Errorf("Tag can not be empty")
	}

	dsc.defaultTag = t

	return nil
}

//Send func send message using default tag (SetDefaultTag).
// Meessage will be transformed before send, using ReplaceAll with values from Client.ReplaceSequences
func (dsc *Client) Send(m string) error {
	if dsc == nil {
		return ErrNilPointerReceiver
	}

	err := dsc.SendWTag(dsc.defaultTag, m)
	if err != nil {
		return fmt.Errorf("Error when call SendWTag using default tag '%s': %w", dsc.defaultTag, err)
	}
	return nil
}

// SendWTag is similar to Send but using a specific tag
func (dsc *Client) SendWTag(t, m string) error {
	if dsc == nil {
		return ErrNilPointerReceiver
	}

	return dsc.SendWTagAndCompressor(t, m, dsc.compressor)
}

// ErrorTagEmpty is returneed when Devo tag is empty string
var ErrorTagEmpty error = errors.New("Tag can not be empty")

// SendWTagAndCompressor is similar to SendWTag but using a specific Compressor.
// This can be usefull, for example, to force disable compression for one message using
// Client.SendWTagAndCompressor(t, m, nil)
func (dsc *Client) SendWTagAndCompressor(t, m string, c *Compressor) error {
	if dsc == nil {
		return ErrNilPointerReceiver
	}

	if t == "" {
		return ErrorTagEmpty
	}

	dsc.sendCalled()

	// Checks if connection should be restarted
	if isExpired(dsc.connectionUsedTimestamp, dsc.maxTimeConnActive) {
		if dsc.conn != nil {
			dsc.conn.Close()
		}
		dsc.makeConnection()
	}

	now := time.Now()
	timestamp := now.Format(time.RFC3339)

	devomsg := fmt.Sprintf(
		"%s%s %s %s: %s\n",
		DefaultSyslogLevel,
		timestamp,
		dsc.syslogHostname,
		t,
		replaceSequences(m, dsc.ReplaceSequences),
	)
	bytesdevomsg := []byte(devomsg)

	if c != nil {
		compressedBytes, err := c.Compress(bytesdevomsg)
		if err == nil {
			//Ignoring compression errors
			bytesdevomsg = nil // Easy garbage collector
			bytesdevomsg = compressedBytes
		}
	}

	_, err := dsc.conn.Write(bytesdevomsg)

	if err != nil {
		return fmt.Errorf("Error when send data to devo: %w", err)
	}

	// Save timestamp of event send
	dsc.connectionUsedTSMutext.Lock()
	dsc.connectionUsedTimestamp = now
	dsc.connectionUsedTSMutext.Unlock()

	return nil
}

// SendAsync is similar to Send but send events in async way (goroutine).
// Empty string is returned in Client is nil
func (dsc *Client) SendAsync(m string) string {
	if dsc == nil {
		return ""
	}

	dsc.waitGroup.Add(1)
	id := uuid.NewV4().String()
	// Save asyncItems ref ids
	dsc.asyncItemsMutext.Lock()
	dsc.asyncItems[id] = nil
	dsc.asyncItemsMutext.Unlock()

	// Checks if connection should be restarted
	if isExpired(dsc.connectionUsedTimestamp, dsc.maxTimeConnActive) {
		if dsc.conn != nil {
			dsc.conn.Close()
		}
		dsc.makeConnection()
	}

	// Run Send with go routine (concurrent call)
	go func(id string) {
		err := dsc.Send(m)
		if err != nil {
			dsc.asyncErrorsMutext.Lock()
			dsc.asyncErrors[id] = err
			dsc.asyncErrorsMutext.Unlock()
		}

		dsc.waitGroup.Done()

		// Remove id from asyncItems
		dsc.asyncItemsMutext.Lock()
		delete(dsc.asyncItems, id)
		dsc.asyncItemsMutext.Unlock()
	}(id)

	return id
}

// SendWTagAsync is similar to SendWTag but send events in async way (goroutine).
// Empty string is returned in Client is nil
func (dsc *Client) SendWTagAsync(t, m string) string {
	if dsc == nil {
		return ""
	}

	return dsc.SendWTagAndCompressorAsync(t, m, dsc.compressor)
}

// SendWTagAndCompressorAsync is similar to SendWTagAsync but send events with specific compressor.
// This can be usefull, for example, to force disable compression for one message using
// Client.SendWTagAndCompressorAsync(t, m, nil)
// Empty string is returned in Client is nil
func (dsc *Client) SendWTagAndCompressorAsync(t, m string, c *Compressor) string {
	if dsc == nil {
		return ""
	}

	dsc.waitGroup.Add(1)
	id := uuid.NewV4().String()
	// Save asyncItems ref ids
	dsc.asyncItemsMutext.Lock()
	dsc.asyncItems[id] = nil
	dsc.asyncItemsMutext.Unlock()

	// Checks if connection should be restarted
	if isExpired(dsc.connectionUsedTimestamp, dsc.maxTimeConnActive) {
		if dsc.conn != nil {
			dsc.conn.Close()
		}
		dsc.makeConnection()
	}

	// Run Send with go routine (concurrent call)
	go func(id string) {
		err := dsc.SendWTagAndCompressor(t, m, c)
		if err != nil {
			dsc.asyncErrorsMutext.Lock()
			dsc.asyncErrors[id] = err
			dsc.asyncErrorsMutext.Unlock()
		}

		dsc.waitGroup.Done()

		// Remove id from asyncItems
		dsc.asyncItemsMutext.Lock()
		delete(dsc.asyncItems, id)
		dsc.asyncItemsMutext.Unlock()
	}(id)

	return id
}

// WaitForPendingAsyncMessages wait for all Async messages that are pending to send
func (dsc *Client) WaitForPendingAsyncMessages() error {
	if dsc == nil {
		return ErrNilPointerReceiver
	}

	dsc.waitGroup.Wait()
	return nil
}

// ErrWaitAsyncTimeout is the error returned when timeout is reached in "WaitFor" functions
var ErrWaitAsyncTimeout = errors.New("Timeout when wait for pending items")

// WaitForPendingAsyncMsgsOrTimeout is similar to WaitForPendingAsyncMessages but
// return ErrWaitAsyncTimeout error if timeout is reached
func (dsc *Client) WaitForPendingAsyncMsgsOrTimeout(timeout time.Duration) error {
	if dsc == nil {
		return ErrNilPointerReceiver
	}

	c := make(chan error)
	go func() {
		defer close(c)
		dsc.waitGroup.Wait()
	}()

	select {
	case <-c:
		return nil // completed normally
	case <-time.After(timeout):
		return ErrWaitAsyncTimeout // timed out
	}
}

// AsyncErrors return errors from async calls collected until now.
// WARNING that map returned IS NOT thread safe.
func (dsc *Client) AsyncErrors() map[string]error {
	if dsc == nil {
		return map[string]error{"": ErrNilPointerReceiver}
	}

	return dsc.asyncErrors
}

// AsyncErrorsNumber return then number of errors from async calls collected until now
func (dsc *Client) AsyncErrorsNumber() int {
	if dsc == nil {
		return 0
	}

	dsc.asyncErrorsMutext.Lock()

	r := len(dsc.asyncErrors)

	dsc.asyncErrorsMutext.Unlock()

	return r
}

// PurgeAsyncErrors cleans internal AsyncErrors captured until now
func (dsc *Client) PurgeAsyncErrors() {
	if dsc == nil {
		return
	}

	if dsc.asyncErrors != nil {
		dsc.asyncErrorsMutext.Lock()

		for k := range dsc.asyncErrors {
			delete(dsc.asyncErrors, k)
		}

		dsc.asyncErrorsMutext.Unlock()
	}
}

// GetEntryPoint return entrypoint used by client
func (dsc *Client) GetEntryPoint() string {
	if dsc == nil {
		return ""
	}

	return dsc.entryPoint
}

// AsyncIds return asyncIds that are currently runnig
func (dsc *Client) AsyncIds() []string {
	if dsc == nil {
		return nil
	}

	dsc.asyncItemsMutext.Lock()

	r := make([]string, len(dsc.asyncItems))

	i := 0
	for k := range dsc.asyncItems {
		r[i] = k
		i++
	}

	dsc.asyncItemsMutext.Unlock()

	return r
}

// AreAsyncOps returns true is there is any Async operation running
func (dsc *Client) AreAsyncOps() bool {
	dsc.asyncItemsMutext.Lock()

	r := len(dsc.asyncItems) > 0

	dsc.asyncItemsMutext.Unlock()

	return r
}

// IsAsyncActive returns true if id is present in AsyncIds(). This function is
// more optimal that look into result of AsyncIds
func (dsc *Client) IsAsyncActive(id string) bool {
	if dsc == nil {
		return false
	}

	dsc.asyncItemsMutext.Lock()

	_, ok := dsc.asyncItems[id]

	dsc.asyncItemsMutext.Unlock()

	return ok
}

// AsyncsNumber return the number of async operations pending. This is more optimal that call len(dsc.AsyncIds())
func (dsc *Client) AsyncsNumber() int {
	if dsc == nil {
		return 0
	}

	dsc.asyncItemsMutext.Lock()
	r := len(dsc.asyncItems)
	dsc.asyncItemsMutext.Unlock()

	return r
}

// LastSendCallTimestamp returns the timestamp of last time that any of SendXXXX func was called with valid parameters
// If Client is nil default time.Time value will be returned
func (dsc *Client) LastSendCallTimestamp() time.Time {
	if dsc == nil {
		return time.Time{}
	}

	dsc.statsMutex.Lock()
	r := dsc.lastSendCallTimestamp
	dsc.statsMutex.Unlock()
	return r
}

// AddReplaceSequences is helper function to add elements to Client.ReplaceSequences
// old is the string to search in message and new is the replacement string. Replacement will be done using strings.ReplaceAll
func (dsc *Client) AddReplaceSequences(old, new string) error {
	if dsc == nil {
		return ErrNilPointerReceiver
	}
	if old == "" {
		return fmt.Errorf("old param can not be empty")
	}
	if new == "" {
		return fmt.Errorf("new param can not be empty")
	}

	if old == new {
		return fmt.Errorf("old and new param values can not be the same")
	}

	if dsc.ReplaceSequences == nil {
		dsc.ReplaceSequences = make(map[string]string)
	}

	dsc.ReplaceSequences[old] = new

	return nil
}

// Write allow Client struct to follow io.Writer interface
func (dsc *Client) Write(p []byte) (n int, err error) {
	if dsc == nil {
		return 0, ErrNilPointerReceiver
	}

	msg := string(p)
	err = dsc.Send(msg)
	if err != nil {
		return 0, err
	}

	return len(msg), nil
}

// Close is the method to close all interanl elements like connection that should be closed at end
func (dsc *Client) Close() error {
	if dsc == nil {
		return ErrNilPointerReceiver
	}

	if dsc.conn == nil {
		return fmt.Errorf("Connection is nil")
	}
	return dsc.conn.Close()
}

func (dsc *Client) String() string {
	if dsc == nil {
		return "<nil>"
	}

	connAddr := "<nil>"
	if dsc.conn != nil {
		connAddr = fmt.Sprintf("%s -> %s", dsc.conn.LocalAddr(), dsc.conn.RemoteAddr())
	}

	dsc.connectionUsedTSMutext.Lock()
	connUsedTimestamp := fmt.Sprintf("%v", dsc.connectionUsedTimestamp)
	dsc.connectionUsedTSMutext.Unlock()

	return fmt.Sprintf(
		"entryPoint: '%s', syslogHostname: '%s', defaultTag: '%s', connAddr: '%s', "+
			"ReplaceSequences: %v, tls: %v, #asyncErrors: %d, tcp: %v, connectionUsedTimestamp: '%s', "+
			"maxTimeConnActive: '%v', #asyncItems: %d, lastSendCallTimestamp: '%s'",
		dsc.entryPoint,
		dsc.syslogHostname,
		dsc.defaultTag,
		connAddr,
		dsc.ReplaceSequences,
		dsc.tls,
		dsc.AsyncErrorsNumber(),
		dsc.tcp,
		connUsedTimestamp,
		dsc.maxTimeConnActive,
		dsc.AsyncsNumber(),
		dsc.LastSendCallTimestamp(),
	)
}

func (dsc *Client) makeConnection() error {
	if dsc.entryPoint == "" {
		return fmt.Errorf("Entrypoint can not be empty")
	}
	u, err := url.Parse(dsc.entryPoint)
	if err != nil {
		return fmt.Errorf("Error when parse entrypoint %s: %w", dsc.entryPoint, err)
	}

	if u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("Unexpected format (protocol://fqdn[:port]) for entrypoint: %v", dsc.entryPoint)
	}

	// Make connection BODY
	tcpConn, err := dsc.tcp.tcpDialer.Dial(u.Scheme, u.Host)
	if err != nil {
		return fmt.Errorf("Error when try to open TCP connection to scheme: %s, host: %s, error: %w", u.Scheme, u.Host, err)
	}

	// TLS
	if dsc.tls != nil {
		// Fix ServerName
		if dsc.tls.tlsConfig != nil {
			if dsc.tls.tlsConfig.ServerName == "" {
				dsc.tls.tlsConfig.ServerName = u.Hostname()
			}
		}
		dsc.conn = tls.Client(tcpConn, dsc.tls.tlsConfig)
	} else {
		dsc.conn = tcpConn
	}

	dsc.connectionUsedTimestamp = time.Now()

	return nil
}

func (dsc *Client) init() {

	// hostname
	dsc.SetSyslogHostName("")
}

// sendCalled is desigend to save stats in thread safe move. Only lastSendCallTimestamp stat is saved at the moment
func (dsc *Client) sendCalled() {
	dsc.statsMutex.Lock()
	dsc.lastSendCallTimestamp = time.Now()
	dsc.statsMutex.Unlock()
}

// CompressorAlgorithm define the compression algorithm used bye Compressor
type CompressorAlgorithm int

const (
	// CompressorNoComprs means compression disabled
	CompressorNoComprs CompressorAlgorithm = iota
	// CompressorGzip set GZIP compression
	CompressorGzip
	// CompressorZlib is Deprecated: This is not properly working if more than one message is send by same connection
	CompressorZlib
)

// Compressor is a simple compressor to work with relative small size of bytes (all is in memory)
type Compressor struct {
	Algorithm   CompressorAlgorithm
	MinimumSize int
}

// Compress compress bs input based on Algorithm and return the data compressed
func (mc *Compressor) Compress(bs []byte) ([]byte, error) {
	if mc == nil {
		return bs, nil
	}

	if mc.MinimumSize > 0 && len(bs) <= mc.MinimumSize {
		return bs, nil
	}

	var buf bytes.Buffer
	var zw io.WriteCloser
	switch mc.Algorithm {
	case CompressorNoComprs:
		r := make([]byte, len(bs))
		copy(r, bs)
		return r, nil
	case CompressorGzip:
		zw = gzip.NewWriter(&buf)
	case CompressorZlib:
		zw = zlib.NewWriter(&buf)
	default:
		return nil, fmt.Errorf("Algorithm %v is not supported", mc.Algorithm)
	}

	_, err := zw.Write(bs)
	if err != nil {
		return nil, fmt.Errorf("Compression: %w", err)
	}

	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("Close compression engine: %w", err)
	}

	return buf.Bytes(), nil
}

// StringAlgoritm return the string value of algorithm selected in Compressor object
func (mc *Compressor) StringAlgoritm() string {
	return StringCompressorAlgorithm(mc.Algorithm)
}

// StringCompressorAlgorithm return the string value of algorithm selected
func StringCompressorAlgorithm(a CompressorAlgorithm) string {
	switch a {
	case CompressorNoComprs:
		return "No compression"
	case CompressorGzip:
		return "GZIP"
	case CompressorZlib:
		return "ZLIB"
	default:
		return "Unknown"
	}
}

// ParseAlgorithm is the inverse func of StringCompressorAlgorithm. Error is returned
// if s value is not matching with none valid algorithm
func ParseAlgorithm(s string) (CompressorAlgorithm, error) {
	for a := CompressorNoComprs; a <= CompressorZlib; a++ {
		v := StringCompressorAlgorithm(a)
		if v == s {
			return a, nil
		}
	}

	return CompressorNoComprs, fmt.Errorf("%s is not a valid algorithm", s)
}

func replaceSequences(s string, sequences map[string]string) string {
	for orig, new := range sequences {
		s = strings.ReplaceAll(s, orig, new)
	}

	return s
}

func loadTLSFiles(keyFileName, certFileName string, chainFileName *string) ([]byte, []byte, []byte, error) {
	var dataKey []byte
	var dataCert []byte
	var dataChain []byte
	var err error
	dataKey, err = ioutil.ReadFile(keyFileName)
	if err != nil {
		return dataKey, dataCert, dataChain, fmt.Errorf("Error when load Key file '%s': %w", keyFileName, err)
	}

	dataCert, err = ioutil.ReadFile(certFileName)
	if err != nil {
		return dataKey, dataCert, dataChain, fmt.Errorf("Error when load Cert file '%s': %w", certFileName, err)
	}

	if chainFileName != nil {
		dataChain, err = ioutil.ReadFile(*chainFileName)
		if err != nil {
			return dataKey, dataCert, dataChain, fmt.Errorf("Error when load Cahin (RootCA) file '%s': %w", *chainFileName, err)
		}
	}
	return dataKey, dataCert, dataChain, nil
}

func isExpired(t time.Time, d time.Duration) bool {
	if d <= 0 {
		return false
	}
	n := time.Now()
	expiresAt := t.Add(d)
	return expiresAt.Before(n)
}
