package utils

import "testing"

func TestTime(t *testing.T) {
	a, err := TimeToNano("20240115", "06:00:00.940")
	t.Log(a, err)
}
