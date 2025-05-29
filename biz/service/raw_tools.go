package service

import (
	"fmt"
	"strconv"
	"strings"
)

// 自定义分割函数，处理行末多余的逗号
func splitLine(line string) []string {
	// 去除行末的逗号
	if strings.HasSuffix(line, ",") {
		line = strings.TrimSuffix(line, ",")
	}
	return strings.Split(line, ",")
}

// 辅助函数：解析int64类型字段
func parseInt64Field(fields []string, headerIndex map[string]int, fieldName string, target *int64) error {
	valueStr := strings.TrimSpace(fields[headerIndex[fieldName]])
	value, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		return fmt.Errorf("解析 %s 失败: %v", fieldName, err)
	}
	*target = value
	return nil
}

// 辅助函数：解析float64类型字段
func parseFloat64Field(fields []string, headerIndex map[string]int, fieldName string, target *float64) error {
	valueStr := strings.TrimSpace(fields[headerIndex[fieldName]])
	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return fmt.Errorf("解析 %s 失败: %v", fieldName, err)
	}
	*target = value
	return nil
}

// 辅助函数：解析int类型字段
func parseIntField(fields []string, headerIndex map[string]int, fieldName string, target *int) error {
	valueStr := strings.TrimSpace(fields[headerIndex[fieldName]])
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return fmt.Errorf("解析 %s 失败: %v", fieldName, err)
	}
	*target = value
	return nil
}
