package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/pflag"

	"github.com/jiazhoulvke/mysql_binlog_reader/reader"
)

var (
	tableCreateFiles         []string
	mysqlConfig              MysqlConfig
	readerConfig             reader.Config
	getTableCreateFromServer bool
	getBinlogPathFromServer  bool
)

func init() {
	pflag.StringVarP(&readerConfig.StartDateTime, "start-datetime", "A", "", "start reading the binlog at first event having a datetime. example: \"2006-01-02 15:04:05\"")
	pflag.StringVarP(&readerConfig.StopDateTime, "stop-datetime", "B", "", "stop reading the binlog at first event having a datetime example: \"2006-01-02 15:04:05\"")
	pflag.StringVarP(&readerConfig.Databases, "databases", "", "", "list entries for just these databases. example: foo,bar")
	pflag.StringVarP(&readerConfig.Tables, "tables", "t", "", "list entries for just these tables. example: foo,bar")
	pflag.StringVarP(&readerConfig.OutputFormat, "output-format", "F", "text", "output format. supported format: text,json")
	pflag.StringVarP(&readerConfig.ExcludeJSONFields, "exclude-json-fields", "", "column_count,pos,row_type,server_id,ts", "exclude json fields")
	pflag.StringVarP(&readerConfig.IncludeDataFields, "include-data-fields", "", "", "include data fields. example: id,title")
	pflag.StringVarP(&readerConfig.ExcludeDataFields, "exclude-data-fields", "", "", "exclude data fields. example: created_at,deleted_at")

	pflag.StringVarP(&mysqlConfig.Host, "mysql-host", "H", "127.0.0.1", "mysql server host")
	pflag.IntVarP(&mysqlConfig.Port, "mysql-port", "P", 3306, "mysql server port")
	pflag.StringVarP(&mysqlConfig.User, "mysql-user", "u", "root", "mysql server user")
	pflag.StringVarP(&mysqlConfig.Password, "mysql-password", "p", "root", "mysql server password")
	pflag.StringVarP(&mysqlConfig.DBName, "mysql-dbname", "d", "", "mysql server database name")

	pflag.StringSliceVarP(&tableCreateFiles, "table-create", "C", nil, "table create sql file")
	pflag.BoolVarP(&getTableCreateFromServer, "get-table-create-from-server", "T", false, "get table create info from mysql server")
	pflag.BoolVarP(&getBinlogPathFromServer, "get-binlog-path-from-server", "L", false, "get binlog file path from mysql server")
}

var (
	db *sql.DB
)

func main() {
	pflag.Parse()
	paths := pflag.Args()
	binlogFiles := make([]string, 0, 1)
	if getTableCreateFromServer || getBinlogPathFromServer {
		var err error
		db, err = NewConnction(mysqlConfig)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer db.Close()
	}
	for _, p := range paths {
		matchSQLFiles, err := filepath.Glob(p)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		binlogFiles = append(binlogFiles, matchSQLFiles...)
	}
	if getBinlogPathFromServer {
		bFiles, err := GetMysqlBinlogFilesPathFromServer(db)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		binlogFiles = append(binlogFiles, bFiles...)
	}
	if len(binlogFiles) == 0 {
		fmt.Fprintln(os.Stderr, "no binlog file")
		os.Exit(1)
	}
	binlogReader, err := reader.NewWithConfig(readerConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if getTableCreateFromServer {
		if err := binlogReader.ReadTablesCreateFromServer(db); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	for _, f := range tableCreateFiles {
		if err := binlogReader.ReadTableCreateFromFile(f); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	for _, f := range binlogFiles {
		if err := binlogReader.OpenBinlog(f, 0); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func getMysqlBinlogPath(db *sql.DB) (string, error) {
	row := db.QueryRow("SELECT @@global.log_bin_basename")
	var logbinBaseName string
	if err := row.Scan(&logbinBaseName); err != nil {
		return "", err
	}
	return filepath.Dir(logbinBaseName), nil
}

func GetMysqlBinlogFilesPathFromServer(db *sql.DB) ([]string, error) {
	dir, err := getMysqlBinlogPath(db)
	if err != nil {
		return nil, err
	}
	files := make([]string, 0)
	rows, err := db.Query("SHOW BINARY LOGS")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var filename string
	var filesize string
	for rows.Next() {
		if err := rows.Scan(&filename, &filesize); err != nil {
			return nil, err
		}
		files = append(files, filepath.Join(dir, filename))
	}
	return files, nil
}

type MysqlConfig struct {
	Host     string
	Port     int
	DBName   string
	User     string
	Password string
}

func NewConnction(cfg MysqlConfig) (*sql.DB, error) {
	var db *sql.DB
	var err error
	db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&loc=Local&parseTime=True", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DBName))
	if err != nil {
		return nil, fmt.Errorf("connection database error: %w", err)
	}
	return db, nil
}
