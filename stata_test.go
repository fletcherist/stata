package stata

import (
	"fmt"
	"testing"
	"time"
)

func TestBin(t *testing.T) {
	t.Run("total", func(t *testing.T) {
		formatted := Bins.Total.Format(time.Now())
		fmt.Println(formatted)
	})
	t.Run("hour", func(t *testing.T) {
		hour := 4
		formatted := Bins.Hour.Format(time.Date(2020, 1, 1, hour, 34, 34, 0, time.UTC))
		fmt.Println(formatted)
		if formatted.Hour() != hour {
			t.Error("got", hour)
		}
	})
	t.Run("minute", func(t *testing.T) {
		minute := 53
		formatted := Bins.Minute.Format(time.Date(2020, 1, 1, 4, minute, 34, 0, time.UTC))
		fmt.Println(formatted)
		if formatted.Minute() != minute {
			t.Error("got", minute)
		}
	})
}

func TestStataInc(t *testing.T) {
	stataClient := New(&Config{
		Storage: NewMemoryStorage(),
	})

	counter := stataClient.Event("count", EventConfig{Bins: []Bin{Bins.Total}})
	counter.Inc()

	val, err := stataClient.Get(Key{
		Name: counter.Name,
		Bin:  Bins.Total,
	})
	if err != nil {
		t.Error(err)
	}
	if val != 1 {
		t.Error("event value want: 1", "got:", val)
	}
}

func TestStataAvg(t *testing.T) {
	stataClient := New(&Config{Storage: NewMemoryStorage()})

	counter := stataClient.EventAvg("count", EventConfig{Bins: []Bin{Bins.Total}})

	var (
		sum   int64 = 0
		count int64 = 0
	)

	for i := 1; i <= 10000; i++ {
		count++
		sum += int64(i)
		counter.Inc(int64(i))
	}

	avg := sum / count
	val, err := stataClient.Get(Key{
		Name: counter.Name,
		Bin:  Bins.Total,
	})
	if err != nil {
		t.Error(err)
	}
	if val != avg {
		t.Error("event value want: 1", "got:", val)
	}
	fmt.Println(val)
}
