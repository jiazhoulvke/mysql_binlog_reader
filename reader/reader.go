package reader

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/structs"
	"github.com/jinzhu/copier"
	"github.com/knocknote/vitess-sqlparser/sqlparser"
	"github.com/siddontang/go-mysql/replication"
)

func init() {
	structs.DefaultTagName = "json"
}

const datetimeLayout = "2006-01-02 15:04:05"

//Config config
type Config struct {
	StartDateTime     string
	StopDateTime      string
	Databases         string
	Tables            string
	Output            io.Writer
	OutputFormat      string
	ExcludeJSONFields string
	IncludeDataFields string
	ExcludeDataFields string
}

type ConfigFunc func(cfg *Config)

func NewConfig(cfgFunc ConfigFunc) Config {
	cfg := Config{
		OutputFormat: "text",
	}
	if cfgFunc != nil {
		cfgFunc(&cfg)
	}
	return cfg
}

//New new mysql binlog reader
func New() *Reader {
	r, _ := NewWithConfig(NewConfig(nil))
	return r
}

//NewWithConfig new mysql binlog reader with config
func NewWithConfig(cfg Config) (*Reader, error) {
	r := Reader{
		tablesCreate:      make(map[string]map[int]string),
		tablesName:        make(map[uint64]string),
		excludeJSONFields: make([]string, 0),
		includeDataFields: make([]string, 0),
		excludeDataFields: make([]string, 0),
	}
	r.binlogParser = replication.NewBinlogParser()
	if cfg.Output == nil {
		cfg.Output = os.Stdout
	}
	r.Output = cfg.Output
	switch cfg.OutputFormat {
	case "json":
		r.outputFunc = func(w io.Writer, d Data) {
			fmt.Fprintln(w, dataToJSON(d, r.excludeJSONFields, r.includeDataFields, r.excludeDataFields))
		}
	default:
		r.outputFunc = func(w io.Writer, d Data) {
			fmt.Fprintln(w, "==============================")
			fmt.Fprintln(w, dataToString(d, r.includeDataFields, r.excludeDataFields))
		}
	}
	if cfg.StartDateTime != "" {
		t, err := time.ParseInLocation(datetimeLayout, cfg.StartDateTime, time.Local)
		if err != nil {
			return &r, err
		}
		startTime := uint32(t.Unix())
		r.AddBinlogEventFilter(func(binlogEvent *replication.BinlogEvent) bool {
			if binlogEvent.Header.Timestamp < startTime {
				return false
			}
			return true
		})
	}
	if cfg.StopDateTime != "" {
		t, err := time.ParseInLocation(datetimeLayout, cfg.StopDateTime, time.Local)
		if err != nil {
			return &r, err
		}
		stopTime := uint32(t.Unix())
		r.AddBinlogEventFilter(func(binlogEvent *replication.BinlogEvent) bool {
			if binlogEvent.Header.Timestamp > stopTime {
				r.binlogParser.Stop()
				return false
			}
			return true
		})
	}
	if cfg.Databases != "" {
		r.AddRowsEventFilter(func(rowsEvent *replication.RowsEvent) bool {
			databases := strings.Split(cfg.Databases, ",")
			for _, d := range databases {
				if d == string(rowsEvent.Table.Schema) {
					return true
				}
			}
			return false
		})
	}
	if cfg.Tables != "" {
		r.AddRowsEventFilter(func(rowsEvent *replication.RowsEvent) bool {
			tableName := r.TableName(rowsEvent.TableID)
			tables := strings.Split(cfg.Tables, ",")
			for _, t := range tables {
				if t == tableName {
					return true
				}
			}
			return false
		})
	}
	if cfg.ExcludeJSONFields != "" {
		fields := strings.Split(cfg.ExcludeJSONFields, ",")
		for _, f := range fields {
			if f == "" {
				continue
			}
			r.excludeJSONFields = append(r.excludeJSONFields, f)
		}
	}
	if cfg.IncludeDataFields != "" {
		fields := strings.Split(cfg.IncludeDataFields, ",")
		for _, f := range fields {
			if f == "" {
				continue
			}
			r.includeDataFields = append(r.includeDataFields, f)
		}
	}
	if cfg.ExcludeDataFields != "" {
		fields := strings.Split(cfg.ExcludeDataFields, ",")
		for _, f := range fields {
			if f == "" {
				continue
			}
			r.excludeDataFields = append(r.excludeDataFields, f)
		}
	}
	return &r, nil
}

//Reader mysql binlog reader
type Reader struct {
	tablesCreate       map[string]map[int]string
	tablesName         map[uint64]string
	binlogParser       *replication.BinlogParser
	binlogEventFilters []binlogEventFilterFunc
	rowsEventFilters   []rowsEventFilterFunc
	Output             io.Writer
	startPostion       uint32
	stopPostion        uint32
	outputFunc         outputFunc
	excludeJSONFields  []string
	includeDataFields  []string
	excludeDataFields  []string
}

func (r *Reader) SetOutputFunc(of outputFunc) {
	r.outputFunc = of
}

func (r *Reader) TableName(tableID uint64) string {
	return r.tablesName[tableID]
}
func (r *Reader) AddBinlogEventFilter(filter binlogEventFilterFunc) {
	r.binlogEventFilters = append(r.binlogEventFilters, filter)
}

func (r *Reader) AddRowsEventFilter(filter rowsEventFilterFunc) {
	r.rowsEventFilters = append(r.rowsEventFilters, filter)
}

func (r *Reader) OpenBinlog(filename string, offset int64) error {
	return r.binlogParser.ParseFile(filename, offset, func(binlogEvent *replication.BinlogEvent) error {
		for _, filter := range r.binlogEventFilters {
			if !filter(binlogEvent) {
				return nil
			}
		}
		if binlogEvent.Header.EventType == replication.TABLE_MAP_EVENT {
			ev, ok := binlogEvent.Event.(*replication.TableMapEvent)
			if !ok {
				return nil
			}
			r.tablesName[ev.TableID] = string(ev.Table)
			return nil
		}
		if !isRowsEvent(binlogEvent.Header.EventType) {
			return nil
		}
		ev, ok := binlogEvent.Event.(*replication.RowsEvent)
		if !ok {
			return nil
		}
		for _, filter := range r.rowsEventFilters {
			if !filter(ev) {
				return nil
			}
		}
		tableName := r.tablesName[ev.TableID]
		dataRows := make([]Data, 0)
		rowTpl := Data{
			EventType:   EventType(binlogEvent.Header.EventType),
			ServerID:    binlogEvent.Header.ServerID,
			Timestamp:   binlogEvent.Header.Timestamp,
			Time:        time.Unix(int64(binlogEvent.Header.Timestamp), 0),
			Position:    binlogEvent.Header.LogPos,
			Database:    string(ev.Table.Schema),
			Table:       r.TableName(ev.TableID),
			ColumnCount: ev.ColumnCount,
			Data:        make(map[string]interface{}),
			OldData:     make(map[string]interface{}),
		}
		tableCreate, tableCreateOK := r.tablesCreate[tableName]
		switch binlogEvent.Header.EventType {
		case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
			rowTpl.DataType = DataTypeWrite
			for _, row := range ev.Rows {
				var dataRow Data
				if err := copier.Copy(&dataRow, &rowTpl); err != nil {
					fmt.Println("copy data error: ", err)
					return nil
				}
				dataRow.Data = ReadDataFromRow(tableCreate, tableCreateOK, row)
				dataRows = append(dataRows, dataRow)
			}
		case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			rowTpl.DataType = DataTypeUpdate
			for i := 1; i <= len(ev.Rows)/2; i++ {
				var dataRow Data
				if err := copier.Copy(&dataRow, &rowTpl); err != nil {
					fmt.Println("copy data error: ", err)
					return nil
				}
				dataRow.OldData = ReadDataFromRow(tableCreate, tableCreateOK, ev.Rows[i*2-2])
				dataRow.Data = ReadDataFromRow(tableCreate, tableCreateOK, ev.Rows[i*2-1])
				dataRows = append(dataRows, dataRow)
			}
		case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
			rowTpl.DataType = DataTypeDelete
			for _, row := range ev.Rows {
				var dataRow Data
				if err := copier.Copy(&dataRow, &rowTpl); err != nil {
					fmt.Println("copy data error: ", err)
					return nil
				}
				dataRow.Data = ReadDataFromRow(tableCreate, tableCreateOK, row)
				dataRows = append(dataRows, dataRow)
			}
		default:
			return nil
		}
		for _, row := range dataRows {
			r.outputFunc(r.Output, row)
		}
		return nil
	})
}

type outputFunc func(writer io.Writer, row Data)

type binlogEventFilterFunc func(binlogEvent *replication.BinlogEvent) bool

type rowsEventFilterFunc func(rowsEvent *replication.RowsEvent) bool

func ReadDataFromRow(tableCreate map[int]string, ok bool, row []interface{}) map[string]interface{} {
	m := make(map[string]interface{})
	for i, v := range row {
		if ok {
			m[tableCreate[i]] = v
		} else {
			m[strconv.FormatInt(int64(i), 10)] = v
		}
	}
	return m
}

type EventType replication.EventType

func (e EventType) String() string {
	return replication.EventType(e).String()
}

func (e EventType) MarshalText() ([]byte, error) {
	return []byte(e.String()), nil
}

type Data struct {
	ServerID    uint32                 `json:"server_id"`
	Timestamp   uint32                 `json:"ts"`
	Time        time.Time              `json:"time"`
	Position    uint32                 `json:"pos"`
	Table       string                 `json:"table"`
	Database    string                 `json:"database"`
	DataType    DataType               `json:"row_type"`
	EventType   EventType              `json:"event_type"`
	Data        map[string]interface{} `json:"data"`
	OldData     map[string]interface{} `json:"old_data"`
	ColumnCount uint64                 `json:"column_count"`
}

func dataToJSON(data Data, excludeJSONFields []string, includeDataFields []string, excludeDataFields []string) string {
	m := structs.Map(data)
	if data.DataType != DataTypeUpdate {
		delete(m, "old_data")
	}
	d := data.Data
	od := data.OldData
	for k := range d {
		if len(includeDataFields) > 0 && !inStringSlice(k, includeDataFields) {
			delete(d, k)
			if data.DataType == DataTypeUpdate {
				delete(od, k)
			}
		}
		if inStringSlice(k, excludeDataFields) {
			delete(d, k)
			if data.DataType == DataTypeUpdate {
				delete(od, k)
			}
		}
	}
	m["data"] = d
	if data.DataType == DataTypeUpdate {
		m["old_data"] = od
	}
	if len(excludeJSONFields) > 0 {
		for _, k := range excludeJSONFields {
			delete(m, k)
		}
	}
	j, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return string(j)
}

func dataToString(data Data, includeDataFields []string, excludeDataFields []string) string {
	bs := bytes.NewBuffer([]byte(""))
	fmt.Fprintf(bs, "database: %s\ntable: %s\ntime: %s\nposition: %d\nserver_id: %d\nevent_type: %s\n", data.Database, data.Table, time.Unix(int64(data.Timestamp), 0).Format(datetimeLayout), data.Position, data.ServerID, data.EventType.String())
	keys := make([]string, 0, len(data.Data))
	for key := range data.Data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if data.DataType == DataTypeUpdate {
		bs.WriteString("old_data: ")
		for _, key := range keys {
			if !inStringSlice(key, includeDataFields) {
				continue
			}
			if inStringSlice(key, excludeDataFields) {
				continue
			}
			fmt.Fprintf(bs, "%s:%v ", key, data.OldData[key])
		}
		bs.WriteString("\n")
	}
	bs.WriteString("data: ")
	for _, key := range keys {
		if len(includeDataFields) > 0 && !inStringSlice(key, includeDataFields) {
			continue
		}
		if inStringSlice(key, excludeDataFields) {
			continue
		}
		fmt.Fprintf(bs, "%s:%v ", key, data.Data[key])
	}
	return bs.String()
}

func inStringSlice(s string, slice []string) bool {
	for i := 0; i < len(slice); i++ {
		if s == slice[i] {
			return true
		}
	}
	return false
}

type DataType byte

const (
	DataTypeWrite = iota
	DataTypeUpdate
	DataTypeDelete
)

func (rt DataType) String() string {
	switch rt {
	case DataTypeWrite:
		return "Write"
	case DataTypeUpdate:
		return "Update"
	case DataTypeDelete:
		return "Delete"
	}
	return "Unknow"
}

func (rt DataType) MarshalText() ([]byte, error) {
	return []byte(rt.String()), nil
}

func (r *Reader) ReadTableCreateFromFile(filename string) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("read file error: %w", err)
	}
	return r.ReadTableCreateFromString(string(data))
}

func (r *Reader) ReadTableCreateFromString(str string) error {
	stmt, err := sqlparser.Parse(str)
	if err != nil {
		return fmt.Errorf("parse sql error: %w", err)
	}
	ct, ok := stmt.(*sqlparser.CreateTable)
	if !ok {
		return fmt.Errorf("not create table sql")
	}
	tableCreate := make(map[int]string)
	for i, col := range ct.Columns {
		tableCreate[i] = col.Name
	}
	r.tablesCreate[ct.NewName.Name.String()] = tableCreate
	return nil
}

func (r *Reader) ReadTablesCreateFromServer(db *sql.DB) error {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return err
	}
	defer rows.Close()
	var tableName string
	for rows.Next() {
		if err := rows.Scan(&tableName); err != nil {
			return err
		}
		row := db.QueryRow("SHOW CREATE TABLE `" + tableName + "`")
		var t string
		var content string
		if err := row.Scan(&t, &content); err != nil {
			return err
		}
		if err := r.ReadTableCreateFromString(content); err != nil {
			return err
		}
	}
	return nil
}

func isRowsEvent(eventType replication.EventType) bool {
	for _, et := range []replication.EventType{replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2} {
		if eventType == et {
			return true
		}
	}
	return false
}
