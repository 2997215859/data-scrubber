package utils

import (
	"fmt"
	"time"
)

// TimeToNano 转换20250403 09:15:00.040格式的时间为纳秒时间戳
func TimeToNano(date string, timeStr string) (int64, error) {
	// 定义时间格式布局
	layout := "20060102 15:04:05.000"

	str := fmt.Sprintf("%s %s", date, timeStr)
	// 解析时间字符串
	t, err := time.Parse(layout, str)
	if err != nil {
		return 0, fmt.Errorf("time parse(%s) error: %v", str, err)
	}

	// 返回纳秒时间戳
	return t.UnixNano(), nil
}
