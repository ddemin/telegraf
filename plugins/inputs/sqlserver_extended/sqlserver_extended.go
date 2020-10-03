package sqlserver_extended

import (
	"database/sql"
	"strconv"
	"sync"
	"time"
	"strings"

	_ "github.com/denisenkom/go-mssqldb" // go-mssqldb initialization
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
)

// SQLServerExtended struct
type SQLServerExtended struct {
	Servers       []string `toml:"servers"`
	Queries       []string `toml:"queries"`
	ResultByRow   bool `toml:"result_by_row"`
	queries       MapQuery
	isInitialized bool
}

// Query struct
type Query struct {
	Script         string
	ResultByRow    bool
	OrderedColumns []string
}

// MapQuery type
type MapQuery map[string]Query

const defaultServer = "Server=.;app name=telegraf;log=1;"

const sampleConfig = `
  ## Specify instances to monitor with a list of connection strings.
  ## All connection parameters are optional.
  ## By default, the host is localhost, listening on default port, TCP 1433.
  ##   for Windows, the user is the currently running AD user (SSO).
  ##   See https://github.com/denisenkom/go-mssqldb for detailed connection
  ##   parameters, in particular, tls connections can be created like so:
  ##   "encrypt=true;certificate=<cert>;hostNameInCertificate=<SqlServer host fqdn>"
  # servers = [
  #  "Server=192.168.1.10;Port=1433;User Id=<user>;Password=<pw>;app name=telegraf;log=1;",
  # ]

  # queries = ["select 'measurement_name' as measurement, some_data as value FROM your_table", "add one more query"]
`

// SampleConfig return the sample configuration
func (s *SQLServerExtended) SampleConfig() string {
	return sampleConfig
}

// Description return plugin description
func (s *SQLServerExtended) Description() string {
	return "Read metrics from Microsoft SQL Server"
}

type scanner interface {
	Scan(dest ...interface{}) error
}

func initQueries(s *SQLServerExtended) {
	s.queries = make(MapQuery)
	queries := s.queries

	i := 0
	for _, quer := range s.Queries {
		queries["custom_"+strconv.Itoa(i)] = Query{Script: sqlPrefix + quer, ResultByRow: s.ResultByRow}
		i += 1
	}

	// Set a flag so we know that queries have already been initialized
	s.isInitialized = true
}

// Gather collect data from SQL Server
func (s *SQLServerExtended) Gather(acc telegraf.Accumulator) error {
	if !s.isInitialized {
		initQueries(s)
	}

	if len(s.Servers) == 0 {
		s.Servers = append(s.Servers, defaultServer)
	}

	var wg sync.WaitGroup

	for _, serv := range s.Servers {
		for _, query := range s.queries {
			wg.Add(1)
			go func(serv string, query Query) {
				defer wg.Done()
				acc.AddError(s.gatherServer(serv, query, acc))
			}(serv, query)
		}
	}

	wg.Wait()
	return nil
}

func (s *SQLServerExtended) gatherServer(server string, query Query, acc telegraf.Accumulator) error {
	// deferred opening
	conn, err := sql.Open("mssql", server)
	if err != nil {
		return err
	}
	defer conn.Close()

	// execute query
	rows, err := conn.Query(query.Script)
	if err != nil {
		return err
	}
	defer rows.Close()

	// grab the column information from the result
	query.OrderedColumns, err = rows.Columns()
	if err != nil {
		return err
	}

	for rows.Next() {
		err = s.accRow(query, acc, rows)
		if err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *SQLServerExtended) accRow(query Query, acc telegraf.Accumulator, row scanner) error {
	var columnVars []interface{}
	var fields = make(map[string]interface{})

	// store the column name with its *interface{}
	columnMap := make(map[string]*interface{})
	for _, column := range query.OrderedColumns {
		columnMap[column] = new(interface{})
	}
	// populate the array of interface{} with the pointers in the right order
	for i := 0; i < len(columnMap); i++ {
		columnVars = append(columnVars, columnMap[query.OrderedColumns[i]])
	}
	// deconstruct array of variables and send to Scan
	err := row.Scan(columnVars...)
	if err != nil {
		return err
	}

	// measurement: identified by the header
	// tags: all other fields with column name != 'field_%'
	tags := map[string]string{}
	var measurement string
	for header, val := range columnMap {
		if str, ok := (*val).(string); ok {
			if header == "measurement" {
				measurement = str
			} else if !strings.HasPrefix(header, "field_") {
				tags[header] = str
			}
		}
	}
	
	if measurement == "" {
		measurement = "sqlserver_extended"
	}

	if query.ResultByRow {
		acc.AddFields(measurement,
			map[string]interface{}{"value": *columnMap["value"]},
			tags, time.Now())
	} else {
		// values
		for header, val := range columnMap {
		    if strings.HasPrefix(header, "field_"){
				fields[strings.Split(header, "_")[1]] = (*val)
			}
		}
		acc.AddFields(measurement, fields, tags, time.Now())
	}
	return nil
}

func init() {
	inputs.Add("sqlserver_extended", func() telegraf.Input {
		return &SQLServerExtended{}
	})
}

const sqlPrefix string = `SET DEADLOCK_PRIORITY -10;
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
`
