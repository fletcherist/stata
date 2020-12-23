package memory

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fletcherist/stata"
)

// pack packs stata key to in-memory string key
func pack(key stata.Key) string {
	unixTimestamp := key.Bin.Format(key.Timestamp).Unix()
	return fmt.Sprint(
		key.Name,
		fmt.Sprintf(":%s", key.Bin.Name),
		fmt.Sprintf(":%s", strconv.FormatInt(unixTimestamp, 10)),
	)
}

// unpack unpacks key from in-memory key-value to stata key
func unpack(key string) stata.Key {
	split := strings.Split(key, ":")
	tsInt64, err := strconv.ParseInt(split[2], 10, 64)
	if err != nil {
		panic(err)
	}
	return stata.Key{
		Name: split[0],
		Bin: stata.Bin{
			Name: split[1],
		},
		Timestamp: time.Unix(tsInt64, 0),
	}
}

// NewStorage creates in-memory storage for stata counters
func NewStorage() *stata.Storage {
	storage := struct {
		mu sync.Mutex
		kv map[string]stata.Value // key-value in-memory storage
	}{
		kv: make(map[string]stata.Value),
	}

	return &stata.Storage{
		Get: func(key stata.Key) (stata.Value, error) {
			storage.mu.Lock()
			defer storage.mu.Unlock()
			val, exist := storage.kv[pack(key)]
			if !exist {
				return 0, nil
			}
			return val, nil
		},
		Set: func(key stata.Key, val stata.Value) error {
			storage.mu.Lock()
			defer storage.mu.Unlock()
			storage.kv[pack(key)] = val
			return nil
		},
		IncrBy: func(keys []stata.Key, val int64) error {
			storage.mu.Lock()
			defer storage.mu.Unlock()
			for _, key := range keys {
				storage.kv[pack(key)] += val
			}
			return nil
		},
		Clear: func() error {
			storage.mu.Lock()
			defer storage.mu.Unlock()
			for key := range storage.kv {
				delete(storage.kv, key)
			}
			return nil
		},
	}
}
