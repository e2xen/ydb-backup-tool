package saver

import "github.com/ydb-platform/ydb-go-sdk/v3/table/result"

type Saver struct {
}

func NewSaver() *Saver {
	return &Saver{}
}

func (s *Saver) Save(_ result.StreamResult) error {
	return nil
}
