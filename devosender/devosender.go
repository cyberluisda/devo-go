package devosender

import (
	"crypto/tls"
	"crypto/x509"
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
	SendWTag(t, m string) error
	SendAsync(m string) string
	SendWTagAsync(t, m string) string
	WaitForPendingAsyngMessages() error
	AsyncErrors() map[string]error
	PurgeAsyncErrors()
	GetEntryPoint() string
}

type tlsSetup struct {
	tlsConfig *tls.Config
}
type tcpConfig struct {
	tcpDialer *net.Dialer
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
}

// ClienBuilderDevoCentralRelay is the type used to set Devo central relay as entrypoint
type ClienBuilderDevoCentralRelay int

// NewClientBuilder returns new DevoSenderBuilder
func NewClientBuilder() *ClientBuilder {
	return &ClientBuilder{
		tlsInsecureSkipVerify: false,
		tlsRenegotiation:      tls.RenegotiateNever,
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
	}

	err := result.makeConnection()
	if err != nil {
		return nil, fmt.Errorf("Error when create new DevoSender (TLS): %w", err)
	}

	// Intialize default values
	result.init()

	return &result, nil
}

// NewDevoSenderTLS create TLS connection using ClientBuiler with minimal configuration
func NewDevoSenderTLS(entrypoint string, key []byte, cert []byte, chain []byte) (*Client, error) {
	return NewClientBuilder().
		EntryPoint(entrypoint).
		TLSCerts(key, cert, chain).
		Build()
}

// NewDevoSenderTLSFiles is similar to NewDevoSenderTLS but loading different certificates from files
func NewDevoSenderTLSFiles(entrypoint string, keyFileName string, certFileName string, chainFileName *string) (*Client, error) {
	return NewClientBuilder().
		EntryPoint(entrypoint).
		TLSFiles(keyFileName, certFileName, chainFileName).
		Build()
}

// NewDevoSender Create new DevoSender with clean comunication using ClientBuilder
// entrypoint is the Devo entrypoint where send events with protocol://fqdn:port format. You can use DevoCentralRelayXX constants to easy assign these value
func NewDevoSender(entrypoint string) (*Client, error) {
	return NewClientBuilder().
		EntryPoint(entrypoint).
		Build()
}

// SetSyslogHostName overwrite hostname send in raw Syslog payload
func (dsc *Client) SetSyslogHostName(host string) {
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
	if t == "" {
		return fmt.Errorf("Tag can not be empty")
	}

	dsc.defaultTag = t

	return nil
}

//Send func send message using default tag (SetDefaultTag).
// Meessage will be transformed before send, using ReplaceAll with values from Client.ReplaceSequences
func (dsc *Client) Send(m string) error {
	err := dsc.SendWTag(dsc.defaultTag, m)
	if err != nil {
		return fmt.Errorf("Error when call SendWTag using default tag '%s': %w", dsc.defaultTag, err)
	}
	return nil
}

// SendWTag is similar to Send but using a specific tag
func (dsc *Client) SendWTag(t, m string) error {
	if t == "" {
		return fmt.Errorf("Tag can not be empty")
	}

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

// SendAsync is similar to Send but send events in async wayt (goroutine)
func (dsc *Client) SendAsync(m string) string {
	dsc.waitGroup.Add(1)
	id := uuid.NewV4().String()

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
	}(id)

	return id
}

// SendWTagAsync is similar to SendWTag but send events in async wayt (goroutine)
func (dsc *Client) SendWTagAsync(t, m string) string {
	dsc.waitGroup.Add(1)
	id := uuid.NewV4().String()

	// Checks if connection should be restarted
	if isExpired(dsc.connectionUsedTimestamp, dsc.maxTimeConnActive) {
		if dsc.conn != nil {
			dsc.conn.Close()
		}
		dsc.makeConnection()
	}

	// Run Send with go routine (concurrent call)
	go func(id string) {
		err := dsc.SendWTag(t, m)
		if err != nil {
			dsc.asyncErrorsMutext.Lock()
			dsc.asyncErrors[id] = err
			dsc.asyncErrorsMutext.Unlock()
		}

		dsc.waitGroup.Done()
	}(id)

	return id
}

// WaitForPendingAsyngMessages wait for all Async messages that are pending to send
func (dsc *Client) WaitForPendingAsyngMessages() error {
	dsc.waitGroup.Wait()
	return nil
}

// AsyncErrors return errors from async calls collected until now
func (dsc *Client) AsyncErrors() map[string]error {
	return dsc.asyncErrors
}

// PurgeAsyncErrors cleans internal AsyncErrors captured until now
func (dsc *Client) PurgeAsyncErrors() {
	if dsc.asyncErrors != nil {
		for k := range dsc.asyncErrors {
			delete(dsc.asyncErrors, k)
		}
	}
}

// GetEntryPoint return entrypoint used by client
func (dsc *Client) GetEntryPoint() string {
	return dsc.entryPoint
}

// AddReplaceSequences is helper function to add elements to Client.ReplaceSequences
// old is the string to search in message and new is the replacement string. Replacement will be done using strings.ReplaceAll
func (dsc *Client) AddReplaceSequences(old, new string) error {
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
	msg := string(p)

	err = dsc.Send(msg)
	if err != nil {
		return 0, err
	}

	return len(msg), nil
}

// Close is the method to close all interanl elements like connection that should be closed at end
func (dsc *Client) Close() error {
	if dsc.conn == nil {
		return fmt.Errorf("Connection is nil")
	}
	return dsc.conn.Close()
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
