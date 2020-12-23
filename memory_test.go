package stata

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestInc(t *testing.T) {
	storage := NewMemoryStorage()
	key := Key{
		Name:      "test",
		Timestamp: time.Now(),
		Bin:       Bins.Minute,
	}

	var wg sync.WaitGroup

	count := 1000
	for i := 1; i <= count; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			err := storage.IncrBy([]Key{key}, 1)
			if err != nil {
				t.Error(err)
			}
			wg.Done()
		}(&wg)
	}
	wg.Wait()
	val, err := storage.Get(key)
	if err != nil {
		t.Error(err)
	}
	if val != int64(count) {
		t.Error(fmt.Sprint("val is not 1 but: ", val))
	}
}
