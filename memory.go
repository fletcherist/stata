package stata

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// pack packs stata key to in-memory string key
func pack(key Key) string {
	unixTimestamp := key.Bin.Format(key.Timestamp).Unix()
	return fmt.Sprint(
		key.Name,
		fmt.Sprintf(":%s", key.Bin.Name),
		fmt.Sprintf(":%s", strconv.FormatInt(unixTimestamp, 10)),
	)
}

// unpack unpacks key from in-memory key-value to stata key
func unpack(key string) Key {
	split := strings.Split(key, ":")
	tsInt64, err := strconv.ParseInt(split[2], 10, 64)
	if err != nil {
		panic(err)
	}
	return Key{
		Name: split[0],
		Bin: Bin{
			Name: split[1],
		},
		Timestamp: time.Unix(tsInt64, 0),
	}
}

// NewMemoryStorage creates in-memory storage for stata counters
func NewMemoryStorage() *Storage {
	storage := struct {
		mu sync.Mutex
		kv map[string]Value // key-value in-memory storage
	}{
		kv: make(map[string]Value),
	}

	return &Storage{
		Get: func(key Key) (Value, error) {
			storage.mu.Lock()
			defer storage.mu.Unlock()
			val, exist := storage.kv[pack(key)]
			if !exist {
				return 0, nil
			}
			return val, nil
		},
		Set: func(key Key, val Value) error {
			storage.mu.Lock()
			defer storage.mu.Unlock()
			storage.kv[pack(key)] = val
			return nil
		},
		IncrBy: func(keys []Key, val int64) error {
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
