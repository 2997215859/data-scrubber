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

func getFieldValue(fields []string, headerIndex map[string]int, fieldName string) (string, error) {
	idx, exists := headerIndex[fieldName]
	if !exists {
		return "", fmt.Errorf("CSV文件缺少必要标题: %s", fieldName)
	}
	if idx < 0 || idx >= len(fields) {
		return "", fmt.Errorf("%s 字段缺失: 索引 %d 超出字段数量 %d", fieldName, idx, len(fields))
	}
	return strings.TrimSpace(fields[idx]), nil
}

func getOptionalFieldValue(fields []string, headerIndex map[string]int, fieldName string) string {
	value, err := getFieldValue(fields, headerIndex, fieldName)
	if err != nil {
		return ""
	}
	return value
}

func hasRequiredFields(fields []string, headerIndex map[string]int, requiredHeaders []string) bool {
	for _, header := range requiredHeaders {
		idx, exists := headerIndex[header]
		if !exists || idx < 0 || idx >= len(fields) {
			return false
		}
	}
	return true
}

func requiredFieldCount(headerIndex map[string]int, requiredHeaders []string) int {
	maxIndex := -1
	for _, header := range requiredHeaders {
		idx, exists := headerIndex[header]
		if exists && idx > maxIndex {
			maxIndex = idx
		}
	}
	return maxIndex + 1
}

// 辅助函数：解析int64类型字段
func parseInt64Field(fields []string, headerIndex map[string]int, fieldName string, target *int64) error {
	valueStr, err := getFieldValue(fields, headerIndex, fieldName)
	if err != nil {
		return err
	}
	value, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		return fmt.Errorf("解析 %s 失败: %v", fieldName, err)
	}
	*target = value
	return nil
}

func parseOptionalInt64Field(fields []string, headerIndex map[string]int, fieldName string, target *int64) error {
	valueStr, err := getFieldValue(fields, headerIndex, fieldName)
	if err != nil || valueStr == "" {
		*target = 0
		return nil
	}
	value, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		return fmt.Errorf("解析 %s 失败: %v", fieldName, err)
	}
	*target = value
	return nil
}

// 辅助函数：解析float64类型字段
func parseFloat64Field(fields []string, headerIndex map[string]int, fieldName string, target *float64) error {
	valueStr, err := getFieldValue(fields, headerIndex, fieldName)
	if err != nil {
		return err
	}
	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return fmt.Errorf("解析 %s 失败: %v", fieldName, err)
	}
	*target = value
	return nil
}

// 辅助函数：解析int类型字段
func parseIntField(fields []string, headerIndex map[string]int, fieldName string, target *int) error {
	valueStr, err := getFieldValue(fields, headerIndex, fieldName)
	if err != nil {
		return err
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return fmt.Errorf("解析 %s 失败: %v", fieldName, err)
	}
	*target = value
	return nil
}
