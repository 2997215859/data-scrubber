package service

import (
	"strings"

	"data-scrubber/biz/utils"
)

func timeToNanoOrZero(date string, timeStr string) (int64, error) {
	if strings.TrimSpace(timeStr) == "" {
		return 0, nil
	}
	return utils.TimeToNano(date, timeStr)
}
