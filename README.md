
### Running YDB in Docker

```bash
docker run -d --rm --name ydb-local -h localhost \
  -p 2135:2135 -p 8765:8765 -p 2136:2136 \
  -v $(pwd)/ydb_certs:/ydb_certs -v $(pwd)/ydb_data:/ydb_data \
  -e YDB_DEFAULT_LOG_LEVEL=NOTICE \
  -e GRPC_TLS_PORT=2135 -e GRPC_PORT=2136 -e MON_PORT=8765 \
  cr.yandex/yc/yandex-docker-local-ydb:latest
```

* To verify its running use the following command:
```bash
ydb -e grpc://localhost:2136 -d /local scheme ls
```

* Creating local profile for future YDB queries
```bash 
ydb config profile create dockerdb -e grpc://localhost:2136 -d /local
```

* Creating a simple table
```bash 
ydb -p dockerdb yql -s "create table t1( id uint64, primary key(id))"
```

* Add a row to the created table
```bash 
ydb -p dockerdb yql -s "insert into t1(id) values (1)"
```

* Select the data from the table
```bash 
ydb -p dockerdb yql -s "select * from t1"
```

* Perform backup of the database
```bash 
ydb -p dockerdb tools ydb
```

### Tool

* List all backups
```bash 
--ydb-endpoint=grpc://localhost:2136 --ydb-name=/local ls
```

* Create full backup
```bash 
--ydb-endpoint=grpc://localhost:2136 --ydb-name=/local create-full 
```

* Create incremental backup
```bash 
--ydb-endpoint=grpc://localhost:2136 --ydb-name=/local create-inc <base_backup_name>
```
