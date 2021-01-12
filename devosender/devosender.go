package devosender

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"time"
)

//DevoSender interface define the minimum behaviour required for Send data to Devo
type DevoSender interface {
	Send(m string) error
	SendWTag(t, m string) error
	SendAsync(m string) string
	SendWTagAsync(t, m string) string
	WaitForPendingAsyngMessages() error
}

type tlsSetup struct {
	tlsConfig *tls.Config
}

// Client is the engine that can send data to Devo throug central (tls) or in-house (clean) realy
type Client struct {
	entryPoint       string
	syslogHostname   string
	defaultTag       string
	conn             net.Conn
	ReplaceSequences map[string]string
	tls              *tlsSetup
}

const (
	// DevoCentralRelayUS is the public entrypoint of Devo central-relay on USA site
	DevoCentralRelayUS = "tcp://us.elb.relay.logtrust.net:443"
	// DevoCentralRelayEU is the public entrypoint of Devo central-relay on Europe site
	DevoCentralRelayEU = "tcp://eu.elb.relay.logtrust.net:443"
	// DefaultSyslogLevel is the code for facility and level used at raw syslog protocol. <14> = facility:user and level:info
	DefaultSyslogLevel = "<14>"
)

// NewDevoSenderTLS  is an alias of NewDevoSenderTLSWithConfig(entrypoint, key, cert, chain, false, tls.RenegotiateNever)
func NewDevoSenderTLS(entrypoint string, key []byte, cert []byte, chain []byte) (*Client, error) {
	// Set default tls options
	return NewDevoSenderTLSWithConfig(entrypoint, key, cert, chain, false, tls.RenegotiateNever)
}

// NewDevoSenderTLSFiles is similar to NewDevoSenderTLS but loading different certificates from files
func NewDevoSenderTLSFiles(entrypoint string, keyFileName string, certFileName string, chainFileName *string) (*Client, error) {

	dataKey, err := ioutil.ReadFile(keyFileName)
	if err != nil {
		return nil, fmt.Errorf("Error when load Key file '%s': %w", keyFileName, err)
	}

	dataCert, err := ioutil.ReadFile(certFileName)
	if err != nil {
		return nil, fmt.Errorf("Error when load Cert file '%s': %w", certFileName, err)
	}

	var dataChain []byte
	if chainFileName != nil {
		dataChain, err = ioutil.ReadFile(*chainFileName)
		if err != nil {
			return nil, fmt.Errorf("Error when load Cahin (RootCA) file '%s': %w", *chainFileName, err)
		}
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
func (dsc *Client) SendAsync(t string) error {
	return fmt.Errorf("Not implemented jet")
}

// SendWTagAsync is similar to SendWTag but send events in async wayt (goroutine)
func (dsc *Client) SendWTagAsync(t, m string) error {
	return fmt.Errorf("Not implemented jet")
}

// WaitForPendingAsyngMessages wait for all Async messages that are pending to send
func (dsc *Client) WaitForPendingAsyngMessages() error {
	return fmt.Errorf("Not implemented jet")
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

	dsc.ReplaceSequences[old] = new

	return nil
}

func (dsc *Client) makeConnection() error {
	if dsc.entryPoint == "" {
		return fmt.Errorf("Entrypoint can not be empty")
	}
	protocolAndURI := strings.SplitN(dsc.entryPoint, "://", 2)
	if len(protocolAndURI) != 2 {
		return fmt.Errorf("Unexpected format (protocol://fqdn[:port]) for entrypoint: %v", dsc.entryPoint)
	}

	var conn net.Conn
	var err error
	if dsc.tls != nil {
		conn, err = tls.Dial(protocolAndURI[0], protocolAndURI[1], dsc.tls.tlsConfig)
		if err != nil {
			return fmt.Errorf("Error when create TLS connection for Devo sender: %w", err)
		}
	} else {
		conn, err = net.Dial(protocolAndURI[0], protocolAndURI[1])
		if err != nil {
			return fmt.Errorf("Error when create clean connection for Devo sender: %w", err)
		}
	}

	dsc.conn = conn

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
