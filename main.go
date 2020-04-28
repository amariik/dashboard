package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/cors"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/godror/godror"
	_ "github.com/lib/pq"
)

type Query struct {
	Name            string   `json:"name"`
	DatabaseName    string   `json:"database_name"`
	QueryString     string   `json:"query_string"`
	RefreshTime     int      `json:"refresh_time"`
	ColumnList      []string `json:"column_list"`
	RowList         []string `json:"row_list"`
	lastRefreshTime time.Time
	Locker          uint32 // locker is used with atomic operation to control updating lastDatablock
	lastDatablock   Datablock
}

type DataSelector struct {
	Name             string            `json:"name"`
	QueryName        string            `json:"query_name"`
	RuleSet          DataSelectorRules `json:"rules"`
	currentDataBlock Datablock
}

func (w *DataSelector) CurrentDataBlock() Datablock {
	return w.currentDataBlock
}

func (w *DataSelector) SetCurrentDataBlock(currentDataBlock Datablock) {
	w.currentDataBlock = currentDataBlock
}

type Datablock struct {
	Title       string                `json:"title"`
	ColumnList  []string              `json:"column_list"`
	RowList     []string              `json:"row_list"`
	Rowdata     map[int][]interface{} `json:"rowdata"`
	UpdatedTime time.Time             `json:"updated_time"`
}

type DbConfig struct {
	Name   string `json:"name"`
	DBType string `json:"db_type"`
}

type OracleSIDConfig struct {
	Name     string `json:"name"`
	DBType   string `json:"db_type"`
	Username string `json:"username"`
	Password string `json:"password"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	SIDName  string `json:"sid_name"`
}

func (cfg OracleSIDConfig) connectionString() string {
	return fmt.Sprintf("%s/%s@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS="+
		"(PROTOCOL=tcp)(HOST=%s)(PORT=%d)))(CONNECT_DATA=(SID=%s)))",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.SIDName)
}

type PostgresConfig struct {
	Name     string `json:"name"`
	DBType   string `json:"db_type"`
	DBName   string `json:"db_name"`
	Username string `json:"username"`
	Password string `json:"password"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
}

func (cfg PostgresConfig) connectionString() string {
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.DBName)
}

type MySQLConfig struct {
	Name     string `json:"name"`
	DBType   string `json:"db_type"`
	DBName   string `json:"db_name"`
	Username string `json:"username"`
	Password string `json:"password"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
}

func (cfg MySQLConfig) connectionString() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.DBName)
}

var dbMap = make(map[string]*sql.DB)
var queryMap = make(map[string]*Query)
var dataSelectorMap = make(map[string]*DataSelector)
var dataSelectorToQueryMap = make(map[string]*Query)

func main() {

	// read the cfg/db folder and create db instances for the json files
	const cfgPathDB = "./cfg/db/"
	dbFiles, err := ioutil.ReadDir(cfgPathDB)
	if err != nil {
		log.Fatal(err)
	}

	// use the db json files and create the different sql.Db into the dbMap
	for _, file := range dbFiles {
		queryFile, err := os.Open(cfgPathDB + file.Name())
		if err != nil {
			fmt.Println(err)
		}
		defer queryFile.Close()

		var jsonData []byte
		jsonData, err = ioutil.ReadAll(queryFile)
		if err != nil {
			fmt.Println(err)
		}

		var genericDB DbConfig
		json.Unmarshal(jsonData, &genericDB)

		if genericDB.DBType == "oracle" {
			var oracleDB OracleSIDConfig

			json.Unmarshal(jsonData, &oracleDB)

			db, err := sql.Open("godror", oracleDB.connectionString())

			if err != nil {
				fmt.Println(err)
				return
			}
			defer db.Close()

			err = db.Ping()
			if err != nil {
				fmt.Println(err)
			}

			dbMap[oracleDB.Name] = db
		} else if genericDB.DBType == "postgres" {
			var postgresDB PostgresConfig

			json.Unmarshal(jsonData, &postgresDB)

			db, err := sql.Open("postgres", postgresDB.connectionString())

			if err != nil {
				fmt.Println(err)
				return
			}
			defer db.Close()

			err = db.Ping()
			if err != nil {
				fmt.Println(err)
			}

			dbMap[postgresDB.Name] = db
		} else if genericDB.DBType == "mysql" {
			var mysqlDB MySQLConfig

			json.Unmarshal(jsonData, &mysqlDB)

			db, err := sql.Open("mysql", mysqlDB.connectionString())

			if err != nil {
				fmt.Println(err)
				return
			}
			defer db.Close()

			err = db.Ping()
			if err != nil {
				fmt.Println(err)
			}

			dbMap[mysqlDB.Name] = db
		} else {
			fmt.Println("Unknown db type ", genericDB.DBType)
		}
	}

	// read the cfg/query folder and load all the query json files
	const cfgPathQuery = "./cfg/query/"
	queryFiles, err := ioutil.ReadDir(cfgPathQuery)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range queryFiles {
		queryFile, err := os.Open(cfgPathQuery + file.Name())
		if err != nil {
			fmt.Println(err)
		}
		defer queryFile.Close()

		var jsonData []byte
		jsonData, err = ioutil.ReadAll(queryFile)
		if err != nil {
			fmt.Println(err)
		}

		var query Query
		json.Unmarshal(jsonData, &query)
		queryMap[query.Name] = &query

		//fmt.Println(query.Name, query.DatabaseName, query.QueryString, query.ColumnList)
	}

	// read the cfg/dataselector folder and load all the dataselector json files
	const cfgPathDataSelector = "./cfg/dataselector/"
	dataSelectorFiles, err := ioutil.ReadDir(cfgPathDataSelector)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range dataSelectorFiles {
		dataSelectorFile, err := os.Open(cfgPathDataSelector + file.Name())
		if err != nil {
			fmt.Println(err)
		}
		defer dataSelectorFile.Close()

		var jsonData []byte
		jsonData, err = ioutil.ReadAll(dataSelectorFile)
		if err != nil {
			fmt.Println(err)
		}

		var dSelector DataSelector
		err = json.Unmarshal(jsonData, &dSelector)
		if err != nil {
			fmt.Println("Error during processing ", dataSelectorFile, " error: ", err)
			continue
		}
		dataSelectorMap[dSelector.Name] = &dSelector

		query, found := queryMap[dSelector.QueryName]

		if found == false {
			fmt.Println("Could not find query in query map ", dSelector.QueryName)
		} else {
			dataSelectorToQueryMap[dSelector.Name] = query
		}

	}

	http.ListenAndServe(":9999", registerRoutes())
}

// if current time minus last time (all in seconds) is greater than the refreshtime (refresh limiter) then update
// the data block otherwise return the queries last datablock. Return bool is if the data was updated or not
func getDatablockAndUpdateIfNeeded(db *sql.DB, v *Query) (Datablock, error, bool) {

	timeSinceLastRefresh := time.Now().Unix() - v.lastRefreshTime.Unix()
	if timeSinceLastRefresh > int64(v.RefreshTime) {
		// atomically check if the value of locker is 0 and if so change it to 1 (locked)
		// if that is is false then return the older data as if it wasn't time to refresh
		// as another process is updating this query currently
		if !atomic.CompareAndSwapUint32(&v.Locker, 0, 1) {
			return v.lastDatablock, nil, false
		}
		defer atomic.StoreUint32(&v.Locker, 0)

		var result Datablock

		var allResults = make(map[int][]interface{})
		rows, err := db.Query(v.QueryString)
		if err != nil {
			return result, err, false
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			return result, err, false
		}
		rowCount := 0
		for rows.Next() {
			// Create a slice of interface{}'s to represent each column,
			// and a second slice to contain pointers to each item in the columns slice.
			columns := make([]interface{}, len(cols))
			columnPointers := make([]interface{}, len(cols))
			rowCount = rowCount + 1

			for i, _ := range columns {
				columnPointers[i] = &columns[i]
			}

			// Scan the result into the column pointers...
			err := rows.Scan(columnPointers...)
			if err != nil {
				return result, err, false
			}

			allResults[rowCount] = columns
		}

		datablock := Datablock{
			Title:       v.Name,
			ColumnList:  v.ColumnList,
			RowList:     v.RowList,
			Rowdata:     allResults,
			UpdatedTime: time.Now(),
		}

		v.lastDatablock = datablock
		v.lastRefreshTime = time.Now()
		return datablock, nil, true
	} else {
		return v.lastDatablock, nil, false
	}

}

func registerRoutes() http.Handler {
	router := chi.NewRouter()

	// Basic CORS
	// for more ideas, see: https://developer.github.com/v3/#cross-origin-resource-sharing
	cors := cors.New(cors.Options{
		// AllowedOrigins: []string{"https://foo.com"}, // Use this to allow specific origin hosts
		AllowedOrigins: []string{"*"},
		// AllowOriginFunc:  func(r *http.Request, origin string) bool { return true },
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300, // Maximum value not ignored by any of major browsers
	})
	router.Use(cors.Handler)

	router.Get("/dataselector/{dataSelectorName}", getDataSelectorHandler)
	router.Get("/dataselectordata/{dataSelectorName}", getDataSelectorDataHandler)
	router.Get("/query/{queryName}", getQueryHandler)
	router.Get("/", getRoot)

	router.Post("/search", postSearchHandler)
	router.Post("/query", postQueryHandler)

	return router
}

func postQueryHandler(writer http.ResponseWriter, request *http.Request) {

	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		log.Fatal(err)
	}

	grafanaQueryRequest, err := UnmarshalGrafanaQueryRequest(body)

	var requestedDataSelectorNames []string
	for i := range grafanaQueryRequest.Targets {
		requestedDataSelectorNames = append(requestedDataSelectorNames, grafanaQueryRequest.Targets[i].Target)
	}
	dataSelectorOutputType := grafanaQueryRequest.Targets[0].Type

	if dataSelectorOutputType == "table" {
		responseJson, httpCode, errorString := convertDataSelectorToGrafanaTable(requestedDataSelectorNames)

		if errorString != "" {
			http.Error(writer, errorString, httpCode)
		} else {
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(httpCode)
			writer.Write(responseJson)
		}
	} else {
		responseJson, httpCode, errorString := convertDataSelectorToGrafanaTimeSeries(requestedDataSelectorNames)

		if errorString != "" {
			http.Error(writer, errorString, httpCode)
		} else {
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(httpCode)
			writer.Write(responseJson)
		}
	}

}

func convertDataSelectorToGrafanaTable(dataSelectorNames []string) ([]byte, int, string) {
	var grafanaRsp GrafanaTableQueryResponse

	for i := range dataSelectorNames {

		dataSelectorName := dataSelectorNames[i]

		_, httpCode, errorString := getDataSelectorData(dataSelectorName)

		if httpCode != http.StatusOK {
			return nil, httpCode, errorString
		}

		dSelector, found := dataSelectorMap[dataSelectorName]
		if found != true {
			return nil, http.StatusNotFound, "Could not find dataselector in dataselector map " + dataSelectorName
		} else {
			var grafanaRspElement GrafanaTableQueryResponseElement

			dblockCols := dSelector.currentDataBlock.ColumnList

			for i, _ := range dblockCols {
				var rspCol GrafanaTableQueryResponseColumn
				rspCol.Text = dblockCols[i]
				rspCol.Type = "string"
				grafanaRspElement.Columns = append(grafanaRspElement.Columns, rspCol)
			}

			dblockRows := dSelector.currentDataBlock.Rowdata

			for _, k := range sortedKeysForDataBlockData(dblockRows) {
				grafanaRspElement.Rows = append(grafanaRspElement.Rows, dblockRows[k])
			}

			grafanaRspElement.Type = "table"

			grafanaRsp = append(grafanaRsp, grafanaRspElement)

		}
	}
	response, err := json.Marshal(grafanaRsp)
	if err == nil {
		return response, http.StatusOK, ""
	} else {
		return nil, http.StatusInternalServerError, err.Error()
	}
}

func convertDataSelectorToGrafanaTimeSeries(dataSelectorNames []string) ([]byte, int, string) {

	var grafanaRsp GrafanaTimeSeriesQueryResponse

	for i := range dataSelectorNames {

		dataSelectorName := dataSelectorNames[i]
		_, httpCode, errorString := getDataSelectorData(dataSelectorName)

		if httpCode != http.StatusOK {
			return nil, httpCode, errorString
		}

		dSelector, found := dataSelectorMap[dataSelectorName]
		if found != true {
			return nil, http.StatusNotFound, "Could not find dataselector in dataselector map " + dataSelectorName
		} else {

			var grafanaRspElement GrafanaTimeSeriesQueryResponseElement

			dblockRows := dSelector.currentDataBlock.Rowdata

			for _, k := range sortedKeysForDataBlockData(dblockRows) {
				datapointTime := dblockRows[k][0].(time.Time)
				datapointMetric := dblockRows[k][1]

				datapointEpocTime := datapointTime.UnixNano() / 1000000

				datapoint := []interface{}{datapointMetric, datapointEpocTime}
				grafanaRspElement.Datapoints = append(grafanaRspElement.Datapoints, datapoint)

			}

			grafanaRsp = append(grafanaRsp, grafanaRspElement)
		}
	}
	response, err := json.Marshal(grafanaRsp)
	if err == nil {
		return response, http.StatusOK, ""
	} else {
		return nil, http.StatusInternalServerError, err.Error()
	}
}

func postSearchHandler(writer http.ResponseWriter, request *http.Request) {

	var dataSelectorNames []string
	for key, _ := range dataSelectorMap {
		dataSelectorNames = append(dataSelectorNames, key)
	}
	responseJSON, err := json.Marshal(dataSelectorNames)

	if err == nil {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
		writer.Write(responseJSON)
	} else {
		http.Error(writer, "Failed to Marshall dataSelectorName list", http.StatusInternalServerError)
	}

}

func getRoot(writer http.ResponseWriter, request *http.Request) {
	writer.WriteHeader(http.StatusOK)
}

func getDataSelectorHandler(w http.ResponseWriter, r *http.Request) {
	dataSelectorName := chi.URLParam(r, "dataSelectorName")

	responseJson, httpCode, errorString := getDataSelector(dataSelectorName)

	if errorString != "" {
		http.Error(w, errorString, httpCode)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(httpCode)
		w.Write(responseJson)
	}
}

// returns
// json of dataselector requested or nil if error
// http status code to use in rsp
// error string to pass back if error
func getDataSelector(dataSelectorName string) ([]byte, int, string) {
	requestedDataSelector, found := dataSelectorMap[dataSelectorName]

	if found != true {
		return nil, http.StatusNotFound, "Could not find dataselector in dataselector map " + dataSelectorName
	} else {
		response, err := json.Marshal(requestedDataSelector)
		if err == nil {
			return response, http.StatusOK, ""
		} else {
			return nil, http.StatusInternalServerError, err.Error()
		}
	}
}

func getQueryHandler(w http.ResponseWriter, r *http.Request) {
	queryName := chi.URLParam(r, "queryName")

	responseJson, httpCode, errorString := getQuery(queryName)

	if errorString != "" {
		http.Error(w, errorString, httpCode)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(httpCode)
		w.Write(responseJson)
	}
}

// returns
// json of query requested or nil if error
// http status code to use in rsp
// error string to pass back if error
func getQuery(queryName string) ([]byte, int, string) {
	requestedQuery, found := queryMap[queryName]

	if found != true {
		return nil, http.StatusNotFound, "Could not find query in query map " + queryName
	} else {
		response, err := json.Marshal(requestedQuery)
		if err == nil {
			return response, http.StatusOK, ""
		} else {
			return nil, http.StatusInternalServerError, err.Error()
		}
	}
}

func getDataSelectorDataHandler(w http.ResponseWriter, r *http.Request) {
	dataSelectorName := chi.URLParam(r, "dataSelectorName")

	responseJson, httpCode, errorString := getDataSelectorData(dataSelectorName)

	if errorString != "" {
		http.Error(w, errorString, httpCode)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(httpCode)
		w.Write(responseJson)
	}
}

// returns
// json of dataselector's datablock requested or nil if error
// http status code to use in rsp
// error string to pass back if error
func getDataSelectorData(dataSelectorName string) ([]byte, int, string) {
	dSelector, found := dataSelectorMap[dataSelectorName]
	if found != true {
		return nil, http.StatusNotFound, "Could not find dataselector in dataselector map " + dataSelectorName
	} else {
		query, found := queryMap[dSelector.QueryName]
		if found != true {
			return nil, http.StatusNotFound, "Could not find query in query map " + dSelector.QueryName
		} else {
			db, found := dbMap[query.DatabaseName]

			if found == false {
				return nil, http.StatusNotFound, "Could not find database in DB map " + query.DatabaseName
			} else {
				datablock, err, dataUpdated := getDatablockAndUpdateIfNeeded(db, query)
				if err != nil {
					return nil, http.StatusInternalServerError, "Error getting results from query " + err.Error()
				}

				if dataUpdated {
					for i := 0; i < len(dSelector.RuleSet.Rules); i++ {
						datablock, _ = dSelector.RuleSet.Rules[i].ApplyRuleToDataBlock(datablock)
					}
					dSelector.SetCurrentDataBlock(datablock)
				}

				response, err := json.Marshal(datablock)
				if err == nil {
					return response, http.StatusOK, ""
				} else {
					return nil, http.StatusInternalServerError, err.Error()
				}
			}
		}
	}
}

func sortedKeysForDataBlockData(m map[int][]interface{}) []int {
	keys := make([]int, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	sort.Ints(keys)
	return keys
}

//*************Grafana query json stuff
func UnmarshalGrafanaQueryRequest(data []byte) (GrafanaQueryRequest, error) {
	var r GrafanaQueryRequest
	err := json.Unmarshal(data, &r)
	return r, err
}

type GrafanaQueryRequest struct {
	Timezone      string     `json:"timezone"`
	PanelID       int64      `json:"panelId"`
	Range         Range      `json:"range"`
	RangeRaw      Raw        `json:"rangeRaw"`
	Interval      string     `json:"interval"`
	IntervalMS    int64      `json:"intervalMs"`
	Targets       []Target   `json:"targets"`
	MaxDataPoints int64      `json:"maxDataPoints"`
	ScopedVars    ScopedVars `json:"scopedVars"`
}

type Range struct {
	From string `json:"from"`
	To   string `json:"to"`
	Raw  Raw    `json:"raw"`
}

type Raw struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type ScopedVars struct {
	Interval   Interval   `json:"__interval"`
	IntervalMS IntervalMS `json:"__interval_ms"`
}

type Interval struct {
	Text  string `json:"text"`
	Value string `json:"value"`
}

type IntervalMS struct {
	Text  int64 `json:"text"`
	Value int64 `json:"value"`
}

type Target struct {
	Target string `json:"target"`
	RefID  string `json:"refId"`
	Type   string `json:"type"`
}

type GrafanaTableQueryResponse []GrafanaTableQueryResponseElement

type GrafanaTableQueryResponseElement struct {
	Columns []GrafanaTableQueryResponseColumn `json:"columns"`
	Rows    [][]interface{}                   `json:"rows"`
	Type    string                            `json:"type"`
}

type GrafanaTableQueryResponseColumn struct {
	Text string `json:"text"`
	Type string `json:"type"`
}

type GrafanaTimeSeriesQueryResponse []GrafanaTimeSeriesQueryResponseElement

type GrafanaTimeSeriesQueryResponseElement struct {
	Datapoints [][]interface{} `json:"datapoints"`
	Target     string          `json:"target"`
}

//**************** RULE STUFF ************************
type DataSelectorRuleActions interface {
	// returns a datablock and a bool if the rule applied or not
	ApplyRuleToDataBlock(dataSourceDataBlock Datablock) (Datablock, bool)
	GetRuleType() string
}

type DataSelectorRuleSet []DataSelectorRuleActions

type DataSelectorRules struct {
	Rules DataSelectorRuleSet `json:"rules"`
}

type GenericDataSelectorRule struct {
	RuleType string `json:"rule_type"`
}

type GrafanaTimeSeriesRule struct {
	RuleType           string `json:"rule_type"`
	TimeColumnHeader   string `json:"time_column_header"`
	MetricColumnHeader string `json:"metric_column_header"`
}

func (rule GrafanaTimeSeriesRule) GetRuleType() string {
	return rule.RuleType
}

func (rule GrafanaTimeSeriesRule) ApplyRuleToDataBlock(dataSourceDataBlock Datablock) (Datablock, bool) {
	var timeColumnIndex int = -1
	var metricColumnIndex int = -1

	for i := 0; i < len(dataSourceDataBlock.ColumnList); i++ {
		if dataSourceDataBlock.ColumnList[i] == rule.TimeColumnHeader {
			timeColumnIndex = i
		}
		if dataSourceDataBlock.ColumnList[i] == rule.MetricColumnHeader {
			metricColumnIndex = i
		}
	}

	if timeColumnIndex != -1 && metricColumnIndex != -1 {

		var rowData = make(map[int][]interface{})
		dblockRows := dataSourceDataBlock.Rowdata

		for _, k := range sortedKeysForDataBlockData(dblockRows) {

			timeColumnData := dblockRows[k][timeColumnIndex]
			metricColumnData := dblockRows[k][metricColumnIndex]

			rowData[k] = []interface{}{timeColumnData, metricColumnData}

		}

		return Datablock{
			Title:       dataSourceDataBlock.Title,
			ColumnList:  []string{rule.TimeColumnHeader, rule.MetricColumnHeader},
			RowList:     dataSourceDataBlock.RowList,
			Rowdata:     rowData,
			UpdatedTime: dataSourceDataBlock.UpdatedTime,
		}, true
	}

	return dataSourceDataBlock, false
}

type FilterRowMatchRegexRule struct {
	RuleType            string `json:"rule_type"`
	ColumnHeaderToCheck string `json:"column_header_to_check"`
	RegexString         string `json:"regex_string"`
}

func (rule FilterRowMatchRegexRule) GetRuleType() string {
	return rule.RuleType
}

func (rule FilterRowMatchRegexRule) ApplyRuleToDataBlock(dataSourceDataBlock Datablock) (Datablock, bool) {
	var filterColumnIndex int = -1

	for i := 0; i < len(dataSourceDataBlock.ColumnList); i++ {
		if dataSourceDataBlock.ColumnList[i] == rule.ColumnHeaderToCheck {
			filterColumnIndex = i
			break
		}
	}

	if filterColumnIndex != -1 {

		var rowData = make(map[int][]interface{})
		dblockRows := dataSourceDataBlock.Rowdata

		for _, k := range sortedKeysForDataBlockData(dblockRows) {
			filterColumnData := dblockRows[k][filterColumnIndex].(string)

			matched, err := regexp.MatchString(rule.RegexString, filterColumnData)

			if matched && err == nil {
				rowData[k] = dblockRows[k]
			}
		}

		return Datablock{
			Title:       dataSourceDataBlock.Title,
			ColumnList:  dataSourceDataBlock.ColumnList,
			RowList:     dataSourceDataBlock.RowList,
			Rowdata:     rowData,
			UpdatedTime: dataSourceDataBlock.UpdatedTime,
		}, true
	}

	return dataSourceDataBlock, false
}

func (rules *DataSelectorRules) UnmarshalJSON(b []byte) error {
	rawRules := make([]json.RawMessage, 0)

	err := json.Unmarshal(b, &rawRules)

	if err != nil {
		return err
	}

	for i := 0; i < len(rawRules); i++ {

		var ruleType GenericDataSelectorRule
		err := json.Unmarshal(rawRules[i], &ruleType)

		if err != nil {
			return err
		}

		if ruleType.RuleType == "timerule" {
			rule := GrafanaTimeSeriesRule{}

			err := json.Unmarshal(rawRules[i], &rule)
			if err != nil {
				return err
			}
			rules.Rules = append(rules.Rules, rule)
		} else if ruleType.RuleType == "regexrule" {
			rule := FilterRowMatchRegexRule{}

			err := json.Unmarshal(rawRules[i], &rule)
			if err != nil {
				return err
			}
			rules.Rules = append(rules.Rules, rule)
		} else {
			return errors.New("Unknown Ruletype")
		}
	}
	return nil
}
