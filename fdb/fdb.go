package fdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"time"

	"github.com/fletcherist/stata"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// pack packs key into fdb tuple
func pack(key stata.Key) tuple.Tuple {
	unixTimestamp := key.Bin.Format(key.Timestamp).Unix()
	return tuple.Tuple{
		key.Name, key.Bin.Name, unixTimestamp,
	}
}

// unpack unpacks fdb tuple to stata key
func unpack(fdbkey tuple.Tuple) stata.Key {
	return stata.Key{
		Name: fdbkey[0].(string),
		Bin: stata.Bin{
			Name: fdbkey[1].(string),
		},
		Timestamp: time.Unix(fdbkey[2].(int64), 0),
	}
}

// toInt64 converts byte array to int64
func bytesToInt64(b []byte) int64 {
	return int64(binary.LittleEndian.Uint64(b))
}

// int64ToBytes convert int64 to byte array
func int64ToBytes(i int64) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.LittleEndian, i)
	return buffer.Bytes()
}

// StorageConfig config for creating foundationdb backend
type StorageConfig struct {
	ClusterFile string
	Namespace   string // namespace is like a folder for keys
}

// NewStorage creates new foundationdb stata storage
func NewStorage(config StorageConfig) (*stata.Storage, error) {
	err := fdb.APIVersion(500)
	if err != nil {
		return nil, err
	}
	db, err := fdb.OpenDatabase(config.ClusterFile)
	if err != nil {
		return nil, err
	}

	// default namespace is "stata"
	var namespace = "stata"
	if config.Namespace != "" {
		namespace = config.Namespace
	}
	subspace, err := directory.CreateOrOpen(db, []string{namespace}, nil)

	storage := stata.Storage{
		Get: func(key stata.Key) (int64, error) {
			dbKey := subspace.Pack(pack(key))
			ret, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
				return tr.Get(dbKey).MustGet(), nil
			})
			if err != nil {
				return 0, err
			}
			bytes := ret.([]byte)
			// counter with this key does not exist
			if len(bytes) == 0 {
				return 0, errors.New("key not found")
			}
			val := bytesToInt64(bytes)
			return val, nil
		},
		IncrBy: func(keys []stata.Key, val int64) error {
			_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
				for _, key := range keys {
					dbKey := subspace.Pack(pack(key))
					tr.Add(dbKey, int64ToBytes(val))
				}
				return nil, nil
			})
			if err != nil {
				return err
			}
			return nil
		},
		GetRange: func(keyRange stata.KeyRange) ([]stata.KeyValue, error) {
			dbFrom := subspace.Pack(pack(keyRange.From))
			dbTo := subspace.Pack(pack(keyRange.To))

			ret, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
				rangeResult := tr.GetRange(fdb.KeyRange{
					Begin: dbFrom,
					End:   dbTo,
				}, fdb.RangeOptions{Limit: 10000})
				iter := rangeResult.Iterator()

				var result []stata.KeyValue
				for iter.Advance() {
					kv, err := iter.Get()
					if err != nil {
						return nil, err
					}
					fdbTuple, err := subspace.Unpack(kv.Key)
					if err != nil {
						return nil, err
					}
					key := unpack(fdbTuple)
					val := bytesToInt64(kv.Value)
					result = append(result, stata.KeyValue{
						Key: key, Value: val,
					})
				}
				return result, nil
			})

			if err != nil {
				return nil, err
			}
			result := ret.([]stata.KeyValue)
			return result, nil
		},
		Clear: func() error {
			_, err := subspace.Remove(db, []string{})
			if err != nil {
				return err
			}
			return nil
		},
	}
	return &storage, nil
}
