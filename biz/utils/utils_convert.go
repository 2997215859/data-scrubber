package utils

import "regexp"

func Float64ToInt64(arr []float64) []int64 {
	result := make([]int64, len(arr))
	for i, v := range arr {
		result[i] = int64(v)
	}
	return result
}

// 判断是否为B股代码
func IsBStock(code string) bool {
	// 上海B股: 900开头的6位数字 + .SH
	// 深圳B股: 200开头的6位数字 + .SZ
	pattern := `^(900\d{3}\.SH|200\d{3}\.SZ)$`
	match, _ := regexp.MatchString(pattern, code)
	return match
}
