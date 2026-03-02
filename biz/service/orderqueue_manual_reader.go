package service

import (
	"archive/zip"
	"bufio"
	"data-scrubber/biz/errorx"
	"data-scrubber/biz/model"
	"fmt"
	"io"
	"strconv"
	"strings"

	logger "github.com/2997215859/golog"
)

// parseIntFieldOptional 解析 int 字段，空值时默认为 0
func parseIntFieldOptional(fields []string, headerIndex map[string]int, fieldName string, target *int) {
	valueStr := strings.TrimSpace(fields[headerIndex[fieldName]])
	if valueStr == "" {
		*target = 0
		return
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		*target = 0
		return
	}
	*target = value
}

// parseFloat64FieldOptional 解析 float64 字段，空值时默认为 0
func parseFloat64FieldOptional(fields []string, headerIndex map[string]int, fieldName string, target *float64) {
	valueStr := strings.TrimSpace(fields[headerIndex[fieldName]])
	if valueStr == "" {
		*target = 0
		return
	}
	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		*target = 0
		return
	}
	*target = value
}

// ManualReadOrderQueue 从 zip 文件中读取委托队列原始数据
// market: "SH" 或 "SZ"，决定时间列名（SH=UpdateTime, SZ=DataTimeStamp）
func ManualReadOrderQueue(filepath string, market string) ([]*model.RawOrderQueue, error) {
	// 打开ZIP文件
	zipReader, err := zip.OpenReader(filepath)
	if err != nil {
		return nil, fmt.Errorf("打开ZIP文件失败: %v", err)
	}
	defer zipReader.Close()

	if len(zipReader.File) != 1 {
		return nil, errorx.NewError("zipReader.File len is not 1")
	}
	csvFile := zipReader.File[0]

	rc, err := csvFile.Open()
	if err != nil {
		return nil, fmt.Errorf("打开CSV文件失败: %v", err)
	}
	defer rc.Close()

	reader := bufio.NewReader(rc)

	// 读取标题行
	headerLine, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("读取标题行失败: %v", err)
	}
	headerLine = strings.TrimSpace(headerLine)

	headers := splitLine(headerLine)

	headerIndex := make(map[string]int)
	for i, header := range headers {
		headerIndex[strings.TrimSpace(header)] = i
	}

	// 确定时间列名
	timeColumn := "UpdateTime"
	if market == "SZ" {
		timeColumn = "DataTimeStamp"
	}

	// 验证必填字段
	requiredHeaders := []string{
		timeColumn, "SecurityID", "ImageStatus", "Side",
		"NoPriceLevel", "PrcLvlOperator", "Price", "Volume",
		"NumOrders", "NoOrders", "LocalTime", "SeqNo",
	}

	for _, header := range requiredHeaders {
		if _, exists := headerIndex[header]; !exists {
			return nil, errorx.NewError("CSV文件缺少必要标题: %s", header)
		}
	}

	// 验证 OrderQty1 存在（至少要有第一个）
	if _, exists := headerIndex["OrderQty1"]; !exists {
		return nil, errorx.NewError("CSV文件缺少必要标题: OrderQty1")
	}

	var result []*model.RawOrderQueue
	lineNum := 2

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, errorx.NewError("读取第 %d 行失败: %v", lineNum, err)
		}

		fields := splitLine(strings.TrimSpace(line))

		if len(fields) < len(requiredHeaders) {
			logger.Info("警告: 第 %d 行字段数量不足（%d/%d），跳过", lineNum, len(fields), len(requiredHeaders))
			lineNum++
			continue
		}

		oq := &model.RawOrderQueue{}

		// 时间列
		oq.Timestamp = strings.TrimSpace(fields[headerIndex[timeColumn]])
		oq.SecurityID = strings.TrimSpace(fields[headerIndex["SecurityID"]])

		if err := parseIntField(fields, headerIndex, "ImageStatus", &oq.ImageStatus); err != nil {
			logger.Info("警告: 第 %d 行 ImageStatus 解析错误: %v，跳过", lineNum, err)
			lineNum++
			continue
		}

		oq.Side = strings.TrimSpace(fields[headerIndex["Side"]])

		parseIntFieldOptional(fields, headerIndex, "NoPriceLevel", &oq.NoPriceLevel)
		parseIntFieldOptional(fields, headerIndex, "PrcLvlOperator", &oq.PrcLvlOperator)
		parseFloat64FieldOptional(fields, headerIndex, "Price", &oq.Price)
		parseFloat64FieldOptional(fields, headerIndex, "Volume", &oq.Volume)
		parseIntFieldOptional(fields, headerIndex, "NumOrders", &oq.NumOrders)
		parseIntFieldOptional(fields, headerIndex, "NoOrders", &oq.NoOrders)

		// 解析 OrderQty1~OrderQty50
		var orderQtyList []float64
		for k := 1; k <= 50 && k <= oq.NoOrders; k++ {
			colName := fmt.Sprintf("OrderQty%d", k)
			idx, exists := headerIndex[colName]
			if !exists {
				break
			}
			if idx >= len(fields) {
				break
			}
			valStr := strings.TrimSpace(fields[idx])
			if valStr == "" {
				break
			}
			val, err := strconv.ParseFloat(valStr, 64)
			if err != nil {
				break
			}
			orderQtyList = append(orderQtyList, val)
		}
		oq.OrderQtyList = orderQtyList

		oq.LocalTime = strings.TrimSpace(fields[headerIndex["LocalTime"]])

		if err := parseInt64Field(fields, headerIndex, "SeqNo", &oq.SeqNo); err != nil {
			logger.Info("警告: 第 %d 行 SeqNo 解析错误: %v，跳过", lineNum, err)
			lineNum++
			continue
		}

		result = append(result, oq)
		lineNum++
	}

	return result, nil
}
