package storage

import "os"

//Storage represent all disk storage structure
type Storage struct {
	path string
}

// Open storage from given path
func Open(path string, createIfNotExists bool) (*Storage, error) {
	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		if !createIfNotExists {
			return nil, ErrStorageNotFound
		}
		if err = os.MkdirAll(path, os.ModeDir | 0700); err != nil {
			return nil, err
		}
	}
	return &Storage{
		path: path,
	}, nil
}

func (s *Storage) OpenDatabase(name string, createIfNotExists bool) (*Database, error) {
	return nil, nil
}

type Database struct {
}

type Column struct {
}

type Row struct {
}

type Cell struct {
}
