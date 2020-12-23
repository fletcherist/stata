package fdb

import (
	"fmt"
	"testing"
)

func TestInt64toBytes(t *testing.T) {
	var val int64 = 10
	fmt.Println(int64ToBytes(val))
}
