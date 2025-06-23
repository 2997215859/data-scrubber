package service

import (
	"testing"
)

func TestTushare(t *testing.T) {
	InitTuShare()
	GetDateLimit("20190625")
}
