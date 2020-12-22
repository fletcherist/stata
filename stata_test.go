package stata

import (
	"fmt"
	"testing"
	"time"
)

func TestBin(t *testing.T) {
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
