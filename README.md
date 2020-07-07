# mysql_binlog_reader

```
Usage of mysql_binlog_reader:
      --databases string               list entries for just these databases. example: foo,bar
      --exclude-data-fields string     exclude data fields. example: created_at,deleted_at
      --exclude-json-fields string     exclude json fields (default "column_count,pos,row_type,server_id,ts")
  -L, --get-binlog-path-from-server    get binlog file path from mysql server
  -T, --get-table-create-from-server   get table create info from mysql server
      --include-data-fields string     include data fields. example: id,title
  -d, --mysql-dbname string            mysql server database name
  -H, --mysql-host string              mysql server host (default "127.0.0.1")
  -p, --mysql-password string          mysql server password (default "root")
  -P, --mysql-port int                 mysql server port (default 3306)
  -u, --mysql-user string              mysql server user (default "root")
  -F, --output-format string           output format. supported format: text,json (default "text")
  -A, --start-datetime string          start reading the binlog at first event having a datetime. example: "2006-01-02 15:04:0
5"
  -B, --stop-datetime string           stop reading the binlog at first event having a datetime example: "2006-01-02 15:04:05"
  -C, --table-create strings           table create sql file
  -t, --tables string                  list entries for just these tables. example: foo,bar
```