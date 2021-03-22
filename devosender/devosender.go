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

// Client is the engine that can send data to Devo throug central (tls) or in-house (clean) realy
type Client struct {
	entryPoint        string
	syslogHostname    string
	defaultTag        string
	conn              net.Conn
	ReplaceSequences  map[string]string
	tls               *tlsSetup
	waitGroup         sync.WaitGroup
	asyncErrors       map[string]error
	asyncErrorsMutext sync.Mutex
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
	}

	err := result.makeConnection()
	if err != nil {
		return nil, fmt.Errorf("Error when create new DevoSender (TLS): %w", err)
	}

	// Intialize default values
	result.init()

	return &result, nil
}

// NewDevoSenderTLS  is an alias of NewDevoSenderTLSWithConfig(entrypoint, key, cert, chain, false, tls.RenegotiateNever)
func NewDevoSenderTLS(entrypoint string, key []byte, cert []byte, chain []byte) (*Client, error) {
	// Set default tls options
	return NewDevoSenderTLSWithConfig(entrypoint, key, cert, chain, false, tls.RenegotiateNever)
}

// NewDevoSenderTLSFiles is similar to NewDevoSenderTLS but loading different certificates from files
func NewDevoSenderTLSFiles(entrypoint string, keyFileName string, certFileName string, chainFileName *string) (*Client, error) {
	dataKey, dataCert, dataChain, err := loadTLSFiles(keyFileName, certFileName, chainFileName)
	if err != nil {
		return nil, err
	}

	return NewDevoSenderTLS(entrypoint, dataKey, dataCert, dataChain)
}

// NewDevoSenderTLSWithConfig Create new DevoSender with TLS comunication and some TLS configuration parameters
// entrypoint is the Devo entrypoint where send events with protocol://fqdn:port format. You can use DevoCentralRelayXX constants to easy assign these value
// key, cert and chain are the content of X.5809 Key, Certificate and Chain CA respectively. See https://docs.devo.com/confluence/ndt/domain-administration/security-credentials/x-509-certificates for more info
// insecureSkipVerify is value asigned to tls.Config.InsecureSkipVerify tls property
// renegotiation is value asigned to tls.Config.Renegotiation tls property
func NewDevoSenderTLSWithConfig(entrypoint string, key []byte, cert []byte, chain []byte, insecureSkipVerify bool, renegotiation tls.RenegotiationSupport) (*Client, error) {

	if len(key) == 0 {
		return nil, fmt.Errorf("key param can not be empty")
	}
	if len(cert) == 0 {
		return nil, fmt.Errorf("cert param can not be empty")
	}

	// tlsSetup
	tlsSetup := &tlsSetup{
		tlsConfig: &tls.Config{
			InsecureSkipVerify: insecureSkipVerify,
			Renegotiation:      renegotiation,
		},
	}

	// Create pool with chain cert
	pool := x509.NewCertPool()
	if len(chain) > 0 {
		ok := pool.AppendCertsFromPEM(chain)
		if !ok {
			return nil, fmt.Errorf("Could not parse chain certificate, content %s", string(chain))
		}
		tlsSetup.tlsConfig.RootCAs = pool
	}

	// Load key and certificate
	crts, err := tls.X509KeyPair(cert, key)
	if err != nil {
		return nil, fmt.Errorf("Error when load key and cert: %w", err)
	}
	tlsSetup.tlsConfig.Certificates = []tls.Certificate{crts}
	tlsSetup.tlsConfig.BuildNameToCertificate()

	result := Client{
		ReplaceSequences: make(map[string]string),
		tls:              tlsSetup,
		entryPoint:       entrypoint,
		asyncErrors:      make(map[string]error),
	}

	// Create connection
	err = result.makeConnection()
	if err != nil {
		return nil, fmt.Errorf("Error when create new DevoSender (TLS): %w", err)
	}

	// Intialize default values
	result.init()

	return &result, nil

}

// NewDevoSender Create new DevoSender with clean comunication
// entrypoint is the Devo entrypoint where send events with protocol://fqdn:port format. You can use DevoCentralRelayXX constants to easy assign these value
func NewDevoSender(entrypoint string) (*Client, error) {

	result := Client{
		ReplaceSequences: make(map[string]string),
		entryPoint:       entrypoint,
		asyncErrors:      make(map[string]error),
	}

	err := result.makeConnection()
	if err != nil {
		return nil, fmt.Errorf("Error when create new DevoSender: %w", err)
	}

	// Intialize default values
	result.init()

	return &result, nil
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
	timestamp := time.Now().Format(time.RFC3339)
	if t == "" {
		return fmt.Errorf("Tag can not be empty")
	}

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

	return nil
}

// SendAsync is similar to Send but send events in async wayt (goroutine)
func (dsc *Client) SendAsync(m string) string {
	dsc.waitGroup.Add(1)
	id := uuid.NewV4().String()

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
	dialer := dsc.tcp.tcpDialer
	if dialer == nil {
		dialer = &net.Dialer{}
	}

	tcpConn, err := dialer.Dial(u.Scheme, u.Host)
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
