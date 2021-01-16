package devologtable

import (
	"bytes"
	"fmt"
	"strconv"
	"text/template"
	"time"

	"github.com/cyberluisda/devo-go/devoquery"
	"github.com/cyberluisda/devo-go/devosender"
)

// LogTableEngine define the required behaviour to work with LogTable
type LogTableEngine interface {
	SetValue(name string, value string) error
	SetValueAndCheck(name string, value string, checkInterval time.Duration, maxRetries int) error
	SetBatchValues(values map[string]string) error
	DeleteValue(name string) error
	DeleteValueAndCheck(name string, checkInterval time.Duration, maxRetries int) error
	GetValue(name string) (*string, error)
	GetAll() (map[string]string, error)
	AddControlPoint() error
	RefreshDataHead() error
}

// LogTableOneStringColumn is the LogTableEngine implementation based on a table with a column of text type
// Table is the Devo table used to save and load data
// Column is the column in Devo table of strint type where save/load data
// BeginTable is pointer used to set `from` value when make queries to Devo. This value should be updated with RefreshDataHead() and is affected by AddControlPoint
type LogTableOneStringColumn struct {
	Table                 string
	Column                string
	BeginTable            time.Time
	devoSender            devosender.DevoSender
	queryEngine           devoquery.QueryEngine
	maxBeginTable         time.Time
	saveTpl               *template.Template
	queryAll              string
	queryGetValueTpl      *template.Template
	queryLastControlPoint string
	queryFirstDataLive    string
}

type oneColumnSaveData struct {
	Command string
	Name    string
	Value   string
	Meta    string
}

type oneColumnQueryByName struct {
	Table  string
	Column string
	Name   string
}

const (
	// MaxTimeToLookForBeginTable is the duration to move 'from' pointer when create engine
	MaxTimeToLookForBeginTable = time.Hour * 24 * (365 / 2)

	oneStringColumnQueryAllTpl = `from {{.Table}}
  select
		split({{.Column}}, "^##|", 0) as command,
		split({{.Column}}, "^##|", 1) as name,
		split({{.Column}}, "^##|", 2) as value,
		split({{.Column}}, "^##|", 3) as meta
	group every 0 by name
  select
  	last(command) as command,
    last(value) as value,
    last(meta) as meta
  `

	oneStringColumnQueryGetValueTpl = `from {{.Table}}
  select
		split({{.Column}}, "^##|", 0) as command,
		split({{.Column}}, "^##|", 1) as name,
		split({{.Column}}, "^##|", 2) as value
	where name = '{{.Name}}'
	group every 0 by name
  select
  	last(command) as command,
    last(value) as value
  `

	oneStringColumnQueryGetLastControlPointTpl = `from {{.Table}}
	select
		split({{.Column}}, "^##|", 0) as command,
		split({{.Column}}, "^##|", 3) as meta,
		int(meta) as refreshpoint
	where command = 'CP'
	group every 0 by command
	select last(refreshpoint) as refreshpoint
	`

	oneStringColumnQueryGetFirstDataLiveTpl = `from {{.Table}}
	select
		split({{.Column}}, "^##|", 0) as command
	where command /= ''
	group every 0
	select first(eventdate) as initdata
	`

	oneStringColumnSaveTpl = `{{.Command}}^##|{{.Name}}^##|{{.Value}}^##|{{.Meta}}`
)

// NewLogTableOneStringColumn create new LogTableOneStringColumn instance using:
// qe for read data,
// ds to save data to Devo,
// table is the Devo table where save/load data
// column is the Column where save/load data
func NewLogTableOneStringColumn(qe devoquery.QueryEngine, ds devosender.DevoSender, table string, column string) (*LogTableOneStringColumn, error) {
	if table == "" {
		return nil, fmt.Errorf("table param can not be empty")
	}
	if column == "" {
		return nil, fmt.Errorf("column param can not be empty")
	}

	maxBeginTable := time.Now().Add(MaxTimeToLookForBeginTable * -1)

	tplSave := template.Must(template.New("save").Parse(oneStringColumnSaveTpl))
	tplQueryGetValue := template.Must(template.New("queryByName").Parse(oneStringColumnQueryGetValueTpl))

	result := &LogTableOneStringColumn{
		Table:            table,
		Column:           column,
		BeginTable:       maxBeginTable,
		devoSender:       ds,
		queryEngine:      qe,
		maxBeginTable:    maxBeginTable,
		saveTpl:          tplSave,
		queryGetValueTpl: tplQueryGetValue,
	}

	tpl := template.Must(template.New("query").Parse(oneStringColumnQueryAllTpl))
	var buf bytes.Buffer
	err := tpl.Execute(&buf, result)
	if err != nil {
		return nil, fmt.Errorf("Error when create getAll query from '%s', params %+v: %w", oneStringColumnQueryAllTpl, result, err)
	}
	result.queryAll = buf.String()

	tpl = template.Must(template.New("lastControlPoint").Parse(oneStringColumnQueryGetLastControlPointTpl))
	buf.Reset()
	err = tpl.Execute(&buf, result)
	if err != nil {
		return nil, fmt.Errorf("Error when create getLatControlPoint query from '%s', params %+v: %w", oneStringColumnQueryGetLastControlPointTpl, result, err)
	}
	result.queryLastControlPoint = buf.String()

	tpl = template.Must(template.New("firstDataLive").Parse(oneStringColumnQueryGetFirstDataLiveTpl))
	buf.Reset()
	err = tpl.Execute(&buf, result)
	if err != nil {
		return nil, fmt.Errorf("Error when create firstDataLive query from '%s', params %+v: %w", oneStringColumnQueryGetFirstDataLiveTpl, result, err)
	}
	result.queryFirstDataLive = buf.String()

	return result, nil
}

// SetValue save new value or update one. This method only run save data withot any check about if data was saved
// name is the name of the value: The key
// value is the value used to update mentioned key.
func (ltoc *LogTableOneStringColumn) SetValue(name string, value string) error {

	rawMessage, err := solveTpl(
		ltoc.saveTpl,
		oneColumnSaveData{
			Command: "S",
			Name:    name,
			Value:   value,
		},
	)
	if err != nil {
		return fmt.Errorf("Error when create raw data from template '%s', to save name '%s', value '%s': %w", oneStringColumnSaveTpl, name, value, err)
	}

	err = ltoc.devoSender.SendWTag(ltoc.Table, rawMessage)
	if err != nil {
		return fmt.Errorf("Error when set value. name: '%s', value: '%s': %w", name, value, err)
	}

	return nil
}

// SetValueAndCheck is similar to SetValue, but check if data was saved. In consecunce this operation is more expensive that SetValue
// name is the name of the value: The key
// value is the value used to update mentioned key.
// checkInterval is the interval between check retries.
// maxRetries is the number of retries of checks. It is NOT related with any retray to save data again.
func (ltoc *LogTableOneStringColumn) SetValueAndCheck(name string, value string, checkInterval time.Duration, maxRetries int) error {
	// First set value
	err := ltoc.SetValue(name, value)
	if err != nil {
		return err
	}

	for i := maxRetries; maxRetries > 0; i-- {
		// Sleep
		time.Sleep(checkInterval)

		// check value
		remoteValue, err := ltoc.GetValue(name)
		if err != nil {
			fmt.Errorf("Error when check if value was set, name: '%s', value '%s': %w", name, value, err)
		}

		if remoteValue != nil && *remoteValue == value {
			return nil
		}
	}

	return fmt.Errorf("I can not be sure if element with name '%s' was set to '%s' after %d retries with pauses of %v between checks", name, value, maxRetries, checkInterval)
}

func (ltoc *LogTableOneStringColumn) SetBatchValues(values map[string]string) error {
	rawMessages := make([]string, len(values))
	i := 0
	for k, v := range values {
		rawMessage, err := solveTpl(
			ltoc.saveTpl,
			oneColumnSaveData{
				Command: "S",
				Name:    k,
				Value:   v,
			},
		)
		if err != nil {
			return fmt.Errorf("Error when create raw data from template '%s', to save name '%s', value '%s': %w", oneStringColumnSaveTpl, k, v, err)
		}
		rawMessages[i] = rawMessage
		i++
	}

	for _, v := range rawMessages {
		ltoc.devoSender.SendWTagAsync(ltoc.Table, v)
	}

	err := ltoc.devoSender.WaitForPendingAsyngMessages()
	if err != nil {
		return fmt.Errorf("Error when wait for pending async messages: %w", err)
	}

	if len(ltoc.devoSender.AsyncErrors()) > 0 {
		err := fmt.Errorf("Errors returned when send data in async mode:")
		for k, v := range ltoc.devoSender.AsyncErrors() {
			err = fmt.Errorf("%w, %s: %v", err, k, v)
		}
		return err
	}
	return nil
}

// DeleteValue mark a value as deleted. This method only run save data withot any check about state
// name is the name of the value to be deleted: The key
func (ltoc *LogTableOneStringColumn) DeleteValue(name string) error {
	rawMessage, err := solveTpl(
		ltoc.saveTpl,
		oneColumnSaveData{
			Command: "D",
			Name:    name,
		},
	)
	if err != nil {
		return fmt.Errorf("Error when create raw data from template '%s', to delete name '%s': %w", oneStringColumnSaveTpl, name, err)
	}

	err = ltoc.devoSender.SendWTag(ltoc.Table, rawMessage)
	if err != nil {
		return fmt.Errorf("Error when delete value. name: '%s': %w", name, err)
	}

	return nil
}

// DeleteValueAndCheck is similar to DeleteValue but check if data was updated. In consecunce this operation is more expensive that DeleteValue
// name is the name of the value to be deleted: The key
// checkInterval is the interval between check retries.
// maxRetries is the number of retries of checks. It is NOT related with any retray to save data again.
func (ltoc *LogTableOneStringColumn) DeleteValueAndCheck(name string, checkInterval time.Duration, maxRetries int) error {
	// First run value
	err := ltoc.DeleteValue(name)
	if err != nil {
		return err
	}

	for i := maxRetries; maxRetries > 0; i-- {
		// Sleep
		time.Sleep(checkInterval)

		// check value
		value, err := ltoc.GetValue(name)
		if err != nil {
			fmt.Errorf("Error when check if value was '%s' deleted': %w", name, err)
		}

		if value == nil {
			return nil
		}
	}

	return fmt.Errorf("I can not be sure if element with name '%s' was removed after %d retries with pauses of %v between checks", name, maxRetries, checkInterval)
}

// GetAll returns all the values (key, value) saved until now. Be carefully because this operation can consume a lot of resources depending of amount of data saved
func (ltoc *LogTableOneStringColumn) GetAll() (map[string]string, error) {
	data, err := ltoc.queryEngine.RunNewQuery(ltoc.BeginTable, time.Now(), ltoc.queryAll)
	if err != nil {
		return nil, fmt.Errorf("Error when run query for load all values: %w", err)
	}

	result := make(map[string]string)
	for _, row := range data.Values {
		command := fmt.Sprintf("%s", row[data.Columns["command"].Index])

		if command == "S" {
			name := fmt.Sprintf("%s", row[data.Columns["name"].Index])
			value := fmt.Sprintf("%s", row[data.Columns["value"].Index])
			result[name] = value
		}
	}

	return result, nil
}

// GetValue return the value saved for located by name, or return nil if value does not exists or was removed
func (ltoc *LogTableOneStringColumn) GetValue(name string) (*string, error) {
	query, err := solveTpl(
		ltoc.queryGetValueTpl,
		oneColumnQueryByName{
			Table:  ltoc.Table,
			Column: ltoc.Column,
			Name:   name,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("Error when create raw data from template '%s', to get value '%s': %w", oneStringColumnQueryGetValueTpl, name, err)
	}

	data, err := ltoc.queryEngine.RunNewQuery(ltoc.BeginTable, time.Now(), query)
	if err != nil {
		return nil, fmt.Errorf("Error when run query for get name '%s': %w", name, err)
	}

	if len(data.Values) == 0 {
		return nil, nil
	}

	if len(data.Values) > 1 {
		return nil, fmt.Errorf("More than one row when run query for getvalue (%d)", len(data.Values))
	}

	command := data.Values[0][data.Columns["command"].Index]
	value := data.Values[0][data.Columns["value"].Index]
	rowName := data.Values[0][data.Columns["name"].Index]

	if name != rowName {
		return nil, fmt.Errorf("Name in response (%s) is not the same as expected %s", rowName, name)
	}

	if command == "D" {
		return nil, nil
	}

	v := fmt.Sprintf("%s", value)
	return &v, nil

}

// GetValueAsNumber is similar to GetValue but parse value to float64
func (ltoc *LogTableOneStringColumn) GetValueAsNumber(name string) (*float64, error) {
	// Load as string
	val, err := ltoc.GetValue(name)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	// Parse number
	f, err := strconv.ParseFloat(*val, 64)
	if err != nil {
		return nil, fmt.Errorf("Error when parse %s as float64: %w", *val, err)
	}

	return &f, nil
}

// GetValueAsBool is similar to GetValue but parse value to boolean
func (ltoc *LogTableOneStringColumn) GetValueAsBool(name string) (*bool, error) {
	// Load as string
	val, err := ltoc.GetValue(name)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	var b bool
	if *val == "true" {
		b = true
		return &b, nil
	} else if *val == "false" {
		b = false
		return &b, nil
	}

	return nil, fmt.Errorf("Error when parse '%s' as bool", *val)
}

// RefreshDataHead query to Devo in order to move internal "from" pointer more close to Now if possible.
// This will improve performance becuase minimal time range intervals is better when make queries.
// RefreshDataHead works better if you make control points with AddControlPoint
func (ltoc *LogTableOneStringColumn) RefreshDataHead() error {

	// First get last control point
	data, err := ltoc.queryEngine.RunNewQuery(ltoc.BeginTable, time.Now(), ltoc.queryLastControlPoint)
	if err != nil {
		return fmt.Errorf("Error when run query for get last control point: %w", err)
	}

	var valIface interface{}
	if len(data.Values) > 0 {
		if len(data.Values) > 1 {
			return fmt.Errorf("More than one row returned when run query for get last control point")
		}

		valIface = data.Values[0][data.Columns["refreshpoint"].Index]
	}

	// Failing trying with first data live
	if valIface == nil {
		data, err = ltoc.queryEngine.RunNewQuery(ltoc.BeginTable, time.Now(), ltoc.queryFirstDataLive)
		if err != nil {
			return fmt.Errorf("Error when run query for get first data live point: %w", err)
		}

		if len(data.Values) == 0 {
			return fmt.Errorf("No value returned when run query for get first data live point")
		}

		if len(data.Values) > 1 {
			return fmt.Errorf("More than one row returned when run query for get first data live point")
		}

		valIface = data.Values[0][data.Columns["initdata"].Index]
	}

	if valIface != nil {
		val := int64(valIface.(float64))
		val = (val / 1000) - 1
		ltoc.BeginTable = time.Unix(val, 0)
	} else {
		return fmt.Errorf("Nill value returned by query when refresh data head")
	}

	return nil
}

// AddControlPoint Load and sava again all values and mark in low level data new point used to RefreshDataHead to update internal pointer.
func (ltoc *LogTableOneStringColumn) AddControlPoint() error {

	// Saving timestamp
	t := time.Now().Unix() * 1000

	// Get all data until now
	all, err := ltoc.GetAll()
	if err != nil {
		return fmt.Errorf("Error when load all values: %w", err)
	}

	// Save data  version
	for name, value := range all {
		err = ltoc.SetValue(name, value)
		if err != nil {
			return fmt.Errorf("Error when save name: '%s', value: '%s': %w", name, value, err)
		}
	}

	// Save control point
	rawMessage, err := solveTpl(
		ltoc.saveTpl,
		oneColumnSaveData{
			Command: "CP",
			Meta:    fmt.Sprintf("%d", t),
		},
	)
	if err != nil {
		return fmt.Errorf("Error when create raw data from template '%s', to create control point: %w", oneStringColumnSaveTpl, err)
	}
	err = ltoc.devoSender.SendWTag(ltoc.Table, rawMessage)
	if err != nil {
		return fmt.Errorf("Error when create control point: %w", err)
	}

	return nil
}

func solveTpl(tpl *template.Template, params interface{}) (string, error) {
	var buf bytes.Buffer
	err := tpl.Execute(&buf, params)
	if err != nil {
		return "", fmt.Errorf("Error when create raw data from template '%s', params %+v: %w", oneStringColumnSaveTpl, params, err)
	}

	return buf.String(), nil
}
