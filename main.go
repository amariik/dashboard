package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	_ "gopkg.in/goracle.v2"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"log"
	"os"
)

type Query struct {
	Name            string   `json:"name"`
	DatabaseName    string   `json:"database_name"`
	QueryString     string   `json:"query_string"`
	RefreshTime     int      `json:"refresh_time"`
	ColumnList      []string `json:"column_list"`
	lastRefreshTime int
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
			rows, err := db.Query(v.QueryString)
			if err != nil {
				fmt.Println("Error running query", v.Name)
				fmt.Println(err)
				return
			}
			defer rows.Close()
			var thedate string
			for rows.Next() {
				rows.Scan(&thedate)
			}
			fmt.Printf("The query result is: %s\n", thedate)
		}
	}

}
