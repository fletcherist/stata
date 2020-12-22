package stataredis

import (
	"fmt"
	"strconv"

	"github.com/fletcherist/stata"

	goRedis "github.com/go-redis/redis"
)

// redisStoragePack packs stata key to redis string key
func redisStoragePack(key stata.Key) string {
	unixTimestamp := key.Bin.Format(key.Timestamp).Unix()
	return fmt.Sprint(
		key.Name,
		fmt.Sprintf(".%s", key.Bin.Name),
		fmt.Sprintf(".%s", strconv.FormatInt(unixTimestamp, 10)),
	)
}

// StorageConfig config for creating stata redis storage
type StorageConfig struct {
	Client *goRedis.Client
}

// NewRedisStorage creates stata redis storage
func NewRedisStorage(config StorageConfig) *stata.Storage {
	redisClient := config.Client

	return &stata.Storage{
		Get: func(key stata.Key) (int64, error) {
			dbKey := redisStoragePack(key)
			result, err := redisClient.Get(dbKey).Result()
			if err != nil {
				return 0, err
			}
			val, err := strconv.ParseInt(result, 10, 64)
			if err != nil {
				return 0, err
			}
			return val, err
		},
		IncrBy: func(keys []stata.Key, val int64) error {
			for _, key := range keys {
				dbKey := redisStoragePack(key)
				err := redisClient.IncrBy(dbKey, val).Err()
				if err != nil {
					return err
				}
			}
			return nil
		},
	}
}
