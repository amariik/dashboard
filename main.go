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
	Layout [][]string
}

type Widget struct {
	Name             string
	QueryName        string
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

	var dbMap = make(map[string]sql.DB)
	var queryMap = make(map[string]Query)

	// read the cfg/db folder and create db instances for the json files
	const cfgPathDB = "./cfg/db/"
	dbFiles, err := ioutil.ReadDir(cfgPathDB)
	if err != nil {
		log.Fatal(err)
	}

	dbList := []sql.DB{}

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

			dbList = append(dbList, *db)
			dbMap[oracleDB.Name] = *db
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

			dbList = append(dbList, *db)
			dbMap[postgresDB.Name] = *db
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

			dbList = append(dbList, *db)
			dbMap[mysqlDB.Name] = *db
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

	queryList := []Query{}
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
		queryList = append(queryList, query)
		queryMap[query.Name] = query

		//fmt.Println(query.Name, query.DatabaseName, query.QueryString, query.ColumnList)
	}

	for _, v := range queryList {

		db, found := dbMap[v.DatabaseName]

		if found == false {
			fmt.Println("Could not find database in DB map ", v.DatabaseName)
		} else {

			datablock, err := getResults(db, v)
			if err != nil {
				fmt.Println("Error getting results from query ", err)
			}
			fmt.Println(datablock)
		}
	}
}

func getResults(db sql.DB, v Query) (Datablock, error) {
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
}
