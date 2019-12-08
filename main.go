package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

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

type Datablock struct {
	Title       string
	ColumnList  []string
	RowList     []string
	Rowdata     map[int][]interface{}
	UpdatedTime time.Time
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

func main() {

	var dbMap = make(map[string]*sql.DB)
	var queryMap = make(map[string]*Query)
	var widgetMap = make(map[string]*Widget)
	var widgetQueryMap = make(map[string]*Query)
	var pageMap = make(map[string]*Page)

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
			widgetQueryMap[widget.Name] = query
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

	var testPage = "page1"

	for i := 0; i < 10; i++ {

		requestedPage, found := pageMap[testPage]

		var pageRequestResult = make([][]Datablock, 5)
		for i := range pageRequestResult {
			pageRequestResult[i] = make([]Datablock, 5)
		}

		if found != true {
			fmt.Println("Could not find page in page map ", testPage)
		} else {

			for i, row := range requestedPage.Layout {
				for j, col := range row {
					widget, found := widgetMap[col]
					if found != true {
						fmt.Println("Could not find widget in widget map ", col)
					} else {
						query, found := queryMap[widget.QueryName]
						if found != true {
							fmt.Println("Could not find query in query map ", widget.QueryName)
						} else {
							db, found := dbMap[query.DatabaseName]

							if found == false {
								fmt.Println("Could not find database in DB map ", query.DatabaseName)
							} else {
								datablock, err := getUpdatedDatablock(db, query)
								if err != nil {
									fmt.Println("Error getting results from query ", err)
								}
								fmt.Println("datablock is ", datablock)
								pageRequestResult[i][j] = datablock
							}
						}
					}
				}
			}
		}

		fmt.Println("*****************")
		fmt.Println("page result is ", pageRequestResult)
		fmt.Println("*****************")

		time.Sleep(15 * time.Second)
	}
}

// if current time minus last time (all in seconds) is greater than the refreshtime (refresh limiter) then update
// the data block otherwise return the queries last datablock
func getUpdatedDatablock(db *sql.DB, v *Query) (Datablock, error) {

	timeSinceLastRefresh := time.Now().Unix() - v.lastRefreshTime.Unix()
	if timeSinceLastRefresh > int64(v.RefreshTime) {
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
