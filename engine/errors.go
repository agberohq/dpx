package engine

import "errors"

// ErrKeyNotFound is returned by StorageEngine.Get and Snapshot.Get
// when the requested key does not exist.
var ErrKeyNotFound = errors.New("key not found")
