package devoquery

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// DevoQueryApiv2US is the public Devo API v2 URL of US site
// DevoQueryApiv2EU is the public Devo API v2 URL of EU site
const (
	DevoQueryApiv2US = "https://apiv2-us.devo.com/search/query"
	DevoQueryApiv2EU = "https://apiv2-eu.devo.com/search/query"
)

// QueryEngineToken follow QueryEngine interface to make queries based on Token go get Data from Devo
type QueryEngineToken struct {
	token        string
	apiURL       string
	DefaultQuery *string
}

// QueryEngine is the any Engine that can run queries to Devo
type QueryEngine interface {
	RunNewQuery(from time.Time, to time.Time, query string) (*QueryResult, error)
	RunDefaultQuery(from time.Time, to time.Time) (*QueryResult, error)
}

// QueryResult is the result after any QueryEngine execute a query
type QueryResult struct {
	Columns     map[string]ColumnResult
	DevoQueryID string
	Values      [][]interface{} // FIXME typefied this
}

// ColumnResult is columns specification in QueryResult
type ColumnResult struct {
	Name  string
	Type  string
	Index int
}

/*
	Example to parse
  {
    "from": {{bodyFrom}},
    "to": {{bodyTo}},
    "mode": {
        "type": "{{outputFormat}}"
    },
    "query": {{query}}
  }
*/
type devoQueryPayload struct {
	From  int64                `json:"from"`
	To    int64                `json:"to"`
	Mode  devoQueryPayloadMode `json:"mode"`
	Query string               `json:"query"`
}
type devoQueryPayloadMode struct {
	Type string `json:"type"`
}

/*
	Example to parse

	{
   "msg": "",
   "status": 0,
   "timestamp": 1609944491688,
   "cid": "e91856456d4e",
   "object": {
       "m": {
           "file_name": {
               "type": "str",
               "index": 0
           },
           "count": {
               "type": "int8",
               "index": 1
           },

...

       },
       "metadata": [
           {
               "name": "file_name",
               "type": "str"
           },
           {
               "name": "count",
               "type": "int8"
           },

...

       ],
       "d": [
           [
               "/tmp/log/xml/2_notes_xml.1",
               1077,
               82944078,
               82945155
           ],

...

        ]
    }
}
*/
type devoQueryResponseBody struct {
	Message   string                  `json:"msg"`
	Status    int                     `json:"status"`
	Timestamp int64                   `json:"timestamp"`
	CID       string                  `json:"cid"`
	Object    devoQueryResponseObject `json:"object"`
}
type devoQueryResponseObject struct {
	Fields   map[string]devoQueryResponseField `json:"m"`
	Metadata []devoQueryResponseMetadata       `json:"metadata"`
	Data     [][]interface{}                   `json:"d"`
}
type devoQueryResponseField struct {
	DevoType string `json:"type"`
	Index    int    `json:"index"`
}
type devoQueryResponseMetadata struct {
	Name     string `json:"name"`
	Devotype string `json:"type"`
}

// NewTokenEngine create new QueryEngineToken
func NewTokenEngine(apiURL string, token string) (*QueryEngineToken, error) {

	// Validations
	if apiURL == "" {
		return nil, fmt.Errorf("apiURL can not be empty")
	}

	_, err := url.ParseRequestURI(apiURL)
	if err != nil {
		return nil, fmt.Errorf("Error when pars apiURL '%s': %w", apiURL, err)
	}

	if token == "" {
		return nil, fmt.Errorf("token can not be empty")
	}

	result := &QueryEngineToken{
		token:  token,
		apiURL: apiURL,
	}
	return result, nil
}

// NewTokenEngineDefaultQuery create new QueryTokenEngine with default query set
func NewTokenEngineDefaultQuery(apiURL string, token string, query string) (*QueryEngineToken, error) {
	if query == "" {
		return nil, fmt.Errorf("query can not be empty")
	}

	result, err := NewTokenEngine(apiURL, token)
	if err != nil {
		return nil, err
	}

	result.DefaultQuery = &query
	return result, nil
}

// RunNewQuery run new query to Devo using dqt QueryEngineToken and return result
func (dqt *QueryEngineToken) RunNewQuery(from time.Time, to time.Time, query string) (*QueryResult, error) {
	if query == "" {
		return nil, fmt.Errorf("query can not be empty")
	}

	if from.Equal(to) {
		return nil, fmt.Errorf("'from' value (%s) can not be equal to 'to' value (%s)", from, to)
	}

	if from.After(to) {
		return nil, fmt.Errorf("'from' value (%s) can not be after 'to' value (%s)", from, to)
	}

	r, err := dqt.devoRequest(from.Unix(), to.Unix(), query)
	if err != nil {
		return nil, fmt.Errorf("Error when make http(s) request to apiURL (%s): %w", dqt.apiURL, err)
	}

	result, err := parseQueryResult(r)
	if err != nil {
		return nil, fmt.Errorf("Error when parse response apiURL: %s, query: %s, from: %s, to: %s: %w", dqt.apiURL, query, from, to, err)
	}
	return result, nil
}

// RunDefaultQuery run default query to Devo using dqt QueryEngineToken and return result
func (dqt *QueryEngineToken) RunDefaultQuery(from time.Time, to time.Time) (*QueryResult, error) {
	if dqt.DefaultQuery == nil {
		return nil, fmt.Errorf("Default query was not set")
	}

	return dqt.RunNewQuery(from, to, *dqt.DefaultQuery)
}

// RunNewQuery run new query to Devo using QueryEngine passed as argument and return result
func RunNewQuery(qe QueryEngine, from time.Time, to time.Time, query string) (*QueryResult, error) {
	return qe.RunNewQuery(from, to, query)
}

// RunDefaultQuery run default query to Devo using QueryEngine passed as argument and return result
func RunDefaultQuery(qe QueryEngine, from time.Time, to time.Time) (*QueryResult, error) {
	return qe.RunDefaultQuery(from, to)
}

func (dqt *QueryEngineToken) devoRequest(from int64, to int64, q string) ([]byte, error) {

	reqBody := devoQueryPayload{
		From:  from,
		To:    to,
		Query: q,
		Mode: devoQueryPayloadMode{
			Type: "json/compact",
		},
	}
	payloadBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("Error when create request body: %w", err)
	}
	body := bytes.NewReader(payloadBytes)

	req, err := http.NewRequest("POST", dqt.apiURL, body)
	if err != nil {
		return nil, fmt.Errorf("Error when make request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+dqt.token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Error when do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {

		bodyResponse := ""
		buf := new(bytes.Buffer)
		_, err = buf.ReadFrom(resp.Body)
		if err == nil {
			bodyResponse = " msg: " + string(buf.Bytes())
		}

		return nil, fmt.Errorf("Unexpected status code in response %d (%s). I expect 200.%s", resp.StatusCode, resp.Status, bodyResponse)
	}

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Error when read bytes from http response. %w", err)
	}

	return buf.Bytes(), nil
}

func parseQueryResult(d []byte) (*QueryResult, error) {
	data := devoQueryResponseBody{}

	err := json.Unmarshal(d, &data)
	if err != nil {
		return nil, fmt.Errorf("Error when parsing query results: %w", err)
	}

	if data.Status != 0 {
		return nil, fmt.Errorf("Unexpected Devo status: %d, msg: %s", data.Status, data.Message)
	}
	// Fill data
	result := QueryResult{
		DevoQueryID: data.CID,
		Columns:     map[string]ColumnResult{},
		Values:      data.Object.Data,
	}

	// Fill columns specification
	for name, def := range data.Object.Fields {
		// Checks column was not proviously added
		_, ok := result.Columns[name]
		if ok {
			return &result, fmt.Errorf("Duplicated column name (%s) is not allowed, please consider rewrite query to avoid it", name)
		}
		result.Columns[name] = ColumnResult{
			Name:  name,
			Index: def.Index,
			Type:  def.DevoType,
		}
	}

	return &result, nil
}
