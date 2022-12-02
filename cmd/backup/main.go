package main

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"log"
	"os"
	"ydb-backup-tool/internal/saver"
	"ydb-backup-tool/internal/scanner"
)

type DBScanner interface {
	ScanAll() (result.StreamResult, error)
}

type BackupSaver interface {
	Save(stream result.StreamResult) error
}

func main() {
	url := os.Getenv("YDB_CONNECTION_URL")
	if url == "" {
		log.Fatal("YDB_CONNECTION_URL is empty")
	}

	ctx := context.Background()
	db, err := ydb.Open(ctx, url)
	if err != nil {
		log.Fatal(err)
	}

	var scnr DBScanner = scanner.NewScanner(db)
	var svr BackupSaver = saver.NewSaver()

	stream, _ := scnr.ScanAll()
	_ = svr.Save(stream)
}
