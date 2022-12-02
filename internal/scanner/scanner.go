package scanner

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
)

type Scanner struct {
	db ydb.Connection
}

func NewScanner(db ydb.Connection) *Scanner {
	return &Scanner{db}
}

func (s *Scanner) ScanAll() (result.StreamResult, error) {
	return nil, nil
}
