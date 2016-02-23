package storage

import (
	"fmt"
	"testing"
)

func TestStorageBasic(t *testing.T) {
	s, err := Open("test", true)
	if err != nil {
		t.Fatal(err)
	}
	if s == nil {
		t.Fatal(fmt.Errorf("storage is nil"))
	}
	db, err := s.OpenDatabase("bank", true)
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Fatal(fmt.Errorf("db is nil"))
	}
}
