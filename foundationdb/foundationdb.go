package statafoundationdb

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

// fdbStoragePack packs key into fdb tuple
func fdbStoragePack(key stata.Key) tuple.Tuple {
	unixTimestamp := key.Bin.Format(key.Timestamp).Unix()
	return tuple.Tuple{
		key.Name, key.Bin.Name, unixTimestamp,
	}
}

// fdbStorageUnpack unpacks fdb tuple to stata key
func fdbStorageUnpack(fdbkey tuple.Tuple) stata.Key {
	return stata.Key{
		Name: fdbkey[0].(string),
		Bin: stata.Bin{
			Name: fdbkey[1].(string),
		},
		Timestamp: time.Unix(fdbkey[2].(int64), 0),
	}
}

// ToInt64 converts byte array to int64
func ToInt64(b []byte) int64 {
	return int64(binary.LittleEndian.Uint64(b))
}

// Int64 convert int64 to byte array
func Int64(i int64) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.LittleEndian, i)
	return buffer.Bytes()
}

// FDBStorageConfig config for creating foundationdb backend
type FDBStorageConfig struct {
	ClusterFile string
}

// NewFDBStorage creates new foundationdb stata storage
func NewFDBStorage(config FDBStorageConfig) (*stata.Storage, error) {
	err := fdb.APIVersion(500)
	if err != nil {
		return nil, err
	}
	db, err := fdb.OpenDatabase(config.ClusterFile)
	if err != nil {
		return nil, err
	}

	subspace, err := directory.CreateOrOpen(db, []string{"stata"}, nil)
	var countInc = []byte{'\x01', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00'}

	storage := stata.Storage{
		Get: func(key stata.Key) (int64, error) {
			dbKey := subspace.Pack(fdbStoragePack(key))
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
			val := ToInt64(bytes)
			return val, nil
		},
		IncrBy: func(keys []stata.Key, val int64) error {
			_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
				for _, key := range keys {
					dbKey := subspace.Pack(fdbStoragePack(key))
					tr.Add(dbKey, countInc)
				}
				return nil, nil
			})
			if err != nil {
				return err
			}
			return nil
		},
		GetRange: func(keyRange stata.KeyRange) ([]stata.KeyValue, error) {
			dbFrom := subspace.Pack(fdbStoragePack(keyRange.From))
			dbTo := subspace.Pack(fdbStoragePack(keyRange.To))

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
					key := fdbStorageUnpack(fdbTuple)
					val := ToInt64(kv.Value)
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
