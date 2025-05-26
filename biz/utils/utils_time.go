package utils

import (
	"fmt"
	"time"
)

// TimeToNano 转换20250403 09:15:00.040格式的时间为纳秒时间戳
func TimeToNano(date string, timeStr string) (int64, error) {
	// 定义时间格式布局
	layout := "20060102 15:04:05.000"

	// 获取本地时区
	loc, err := time.LoadLocation("Local")
	if err != nil {
		return 0, fmt.Errorf("load local timezone error: %v", err)
	}

	str := fmt.Sprintf("%s %s", date, timeStr)
	// 解析时间字符串
	t, err := time.ParseInLocation(layout, str, loc)
	if err != nil {
		return 0, fmt.Errorf("time parse(%s) error: %v", str, err)
	}

	// 返回纳秒时间戳
	return t.UnixNano(), nil
}

func NsToTimeString(ns int64) string {
	t := time.Unix(0, ns)
	return t.Format("2006-01-02 15:04:05.000000000")
}
