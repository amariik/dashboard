package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "gopkg.in/goracle.v2"
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

type Page struct {
	Name   string     `json:"name"`
	Layout [][]string `json:"layout"`
}

type Widget struct {
	Name             string `json:"name"`
	QueryName        string `json:"query_name"`
	currentDataBlock Datablock
}

func (w *Widget) CurrentDataBlock() Datablock {
	return w.currentDataBlock
}

func (w *Widget) SetCurrentDataBlock(currentDataBlock Datablock) {
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
var widgetMap = make(map[string]*Widget)
var pageMap = make(map[string]*Page)
var widgetToQueryMap = make(map[string]*Query)

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

			db, err := sql.Open("goracle", oracleDB.connectionString())

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

	// read the cfg/widget folder and load all the widget json files
	const cfgPathWidget = "./cfg/widget/"
	widgetFiles, err := ioutil.ReadDir(cfgPathWidget)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range widgetFiles {
		widgetFile, err := os.Open(cfgPathWidget + file.Name())
		if err != nil {
			fmt.Println(err)
		}
		defer widgetFile.Close()

		var jsonData []byte
		jsonData, err = ioutil.ReadAll(widgetFile)
		if err != nil {
			fmt.Println(err)
		}

		var widget Widget
		json.Unmarshal(jsonData, &widget)
		widgetMap[widget.Name] = &widget

		query, found := queryMap[widget.QueryName]

		if found == false {
			fmt.Println("Could not find query in query map ", widget.QueryName)
		} else {
			widgetToQueryMap[widget.Name] = query
		}

	}

	// read the cfg/page folder and load all the page json files
	const cfgPathPage = "./cfg/page/"
	pageFiles, err := ioutil.ReadDir(cfgPathPage)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range pageFiles {
		pageFile, err := os.Open(cfgPathPage + file.Name())
		if err != nil {
			fmt.Println(err)
		}
		defer pageFile.Close()

		var jsonData []byte
		jsonData, err = ioutil.ReadAll(pageFile)
		if err != nil {
			fmt.Println(err)
		}

		var page Page
		json.Unmarshal(jsonData, &page)
		pageMap[page.Name] = &page
	}

	http.ListenAndServe(":9000", registerRoutes())
}

// if current time minus last time (all in seconds) is greater than the refreshtime (refresh limiter) then update
// the data block otherwise return the queries last datablock
func getDatablockAndUpdateIfNeeded(db *sql.DB, v *Query) (Datablock, error) {

	timeSinceLastRefresh := time.Now().Unix() - v.lastRefreshTime.Unix()
	if timeSinceLastRefresh > int64(v.RefreshTime) {
		// atomically check if the value of locker is 0 and if so change it to 1 (locked)
		// if that is is false then return the older data as if it wasn't time to refresh
		// as another process is updating this query currently
		if !atomic.CompareAndSwapUint32(&v.Locker, 0, 1) {
			return v.lastDatablock, nil
		}
		defer atomic.StoreUint32(&v.Locker, 0)

		var result Datablock

		var allResults = make(map[int][]interface{})
		rows, err := db.Query(v.QueryString)
		if err != nil {
			return result, err
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			return result, err
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
				return result, err
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
		return datablock, nil
	} else {
		return v.lastDatablock, nil
	}

}

func registerRoutes() http.Handler {
	router := chi.NewRouter()
	router.Get("/page/{pageName}", getPageHandler)
	router.Get("/widget/{widgetName}", getWidgetHandler)
	router.Get("/widgetdata/{widgetName}", getWidgetDataHandler)
	router.Get("/query/{queryName}", getQueryHandler)
	return router
}

func getPageHandler(w http.ResponseWriter, r *http.Request) {
	pageName := chi.URLParam(r, "pageName")

	responseJson, httpCode, errorString := getPage(pageName)

	if errorString != "" {
		http.Error(w, errorString, httpCode)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(httpCode)
		w.Write(responseJson)
	}
}

// returns
// json of page requested or nil if error
// http status code to use in rsp
// error string to pass back if error
func getPage(pageName string) ([]byte, int, string) {
	requestedPage, found := pageMap[pageName]

	if found != true {
		return nil, http.StatusNotFound, "Could not find page in page map " + pageName
	} else {
		response, err := json.Marshal(requestedPage)
		if err == nil {
			return response, http.StatusOK, ""
		} else {
			return nil, http.StatusInternalServerError, err.Error()
		}
	}
}

func getWidgetHandler(w http.ResponseWriter, r *http.Request) {
	widgetName := chi.URLParam(r, "widgetName")

	responseJson, httpCode, errorString := getWidget(widgetName)

	if errorString != "" {
		http.Error(w, errorString, httpCode)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(httpCode)
		w.Write(responseJson)
	}
}

// returns
// json of widget requested or nil if error
// http status code to use in rsp
// error string to pass back if error
func getWidget(widgetName string) ([]byte, int, string) {
	requestedWidget, found := widgetMap[widgetName]

	if found != true {
		return nil, http.StatusNotFound, "Could not find widget in widget map " + widgetName
	} else {
		response, err := json.Marshal(requestedWidget)
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

func getWidgetDataHandler(w http.ResponseWriter, r *http.Request) {
	widgetName := chi.URLParam(r, "widgetName")

	responseJson, httpCode, errorString := getWidgetData(widgetName)

	if errorString != "" {
		http.Error(w, errorString, httpCode)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(httpCode)
		w.Write(responseJson)
	}
}

// returns
// json of widget's datablock requested or nil if error
// http status code to use in rsp
// error string to pass back if error
func getWidgetData(widgetName string) ([]byte, int, string) {
	widget, found := widgetMap[widgetName]
	if found != true {
		return nil, http.StatusNotFound, "Could not find widget in widget map " + widgetName
	} else {
		query, found := queryMap[widget.QueryName]
		if found != true {
			return nil, http.StatusNotFound, "Could not find query in query map " + widget.QueryName
		} else {
			db, found := dbMap[query.DatabaseName]

			if found == false {
				return nil, http.StatusNotFound, "Could not find database in DB map " + query.DatabaseName
			} else {
				datablock, err := getDatablockAndUpdateIfNeeded(db, query)
				if err != nil {
					return nil, http.StatusInternalServerError, "Error getting results from query " + err.Error()
				}
				widget.SetCurrentDataBlock(datablock)

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
