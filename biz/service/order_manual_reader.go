package service

import (
	"archive/zip"
	"bufio"
	"data-scrubber/biz/errorx"
	"data-scrubber/biz/model"
	"fmt"
	"io"
	"strings"

	logger "github.com/2997215859/golog"
)

func ManualReadOldShRawOrder(filepath string) ([]*model.OldShRawOrder, error) {
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

	// 定义必填标题（BizIndex 可能在早期日期不存在，但逐笔委托文件中应该都有）
	requiredHeaders := []string{
		"DataStatus", "OrderIndex", "OrderChannel", "SecurityID", "OrderTime",
		"OrderType", "OrderNO", "OrderPrice", "Balance", "OrderBSFlag",
	}

	for _, header := range requiredHeaders {
		if _, exists := headerIndex[header]; !exists {
			return nil, errorx.NewError("CSV文件缺少必要标题: %s", header)
		}
	}

	// 检查 BizIndex 是否存在
	_, hasBizIndex := headerIndex["BizIndex"]

	var orders []*model.OldShRawOrder
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

		if !hasRequiredFields(fields, headerIndex, requiredHeaders) {
			logger.Info("警告: 第 %d 行字段数量不足（%d/%d），跳过", lineNum, len(fields), requiredFieldCount(headerIndex, requiredHeaders))
			lineNum++
			continue
		}

		order := &model.OldShRawOrder{}

		if err := parseIntField(fields, headerIndex, "DataStatus", &order.DataStatus); err != nil {
			logger.Info("警告: 第 %d 行 DataStatus 解析错误: %v，跳过", lineNum, err)
			lineNum++
			continue
		}

		if err := parseInt64Field(fields, headerIndex, "OrderIndex", &order.OrderIndex); err != nil {
			logger.Info("警告: 第 %d 行 OrderIndex 解析错误: %v，跳过", lineNum, err)
			lineNum++
			continue
		}

		if err := parseInt64Field(fields, headerIndex, "OrderChannel", &order.OrderChannel); err != nil {
			logger.Info("警告: 第 %d 行 OrderChannel 解析错误: %v，跳过", lineNum, err)
			lineNum++
			continue
		}

		order.SecurityID = getOptionalFieldValue(fields, headerIndex, "SecurityID")
		order.OrderTime = getOptionalFieldValue(fields, headerIndex, "OrderTime")
		order.OrderType = getOptionalFieldValue(fields, headerIndex, "OrderType")

		if err := parseInt64Field(fields, headerIndex, "OrderNO", &order.OrderNO); err != nil {
			logger.Error("警告: 第 %d 行 OrderNO 解析错误: %v，跳过", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "OrderPrice", &order.OrderPrice); err != nil {
			logger.Error("警告: 第 %d 行 OrderPrice 解析错误: %v，跳过", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "Balance", &order.Balance); err != nil {
			logger.Error("警告: 第 %d 行 Balance 解析错误: %v，跳过", lineNum, err)
			lineNum++
			continue
		}

		order.OrderBSFlag = getOptionalFieldValue(fields, headerIndex, "OrderBSFlag")

		if hasBizIndex {
			if err := parseInt64Field(fields, headerIndex, "BizIndex", &order.BizIndex); err != nil {
				logger.Error("警告: 第 %d 行 BizIndex 解析错误: %v，跳过", lineNum, err)
				lineNum++
				continue
			}
		}

		order.LocalTime = getOptionalFieldValue(fields, headerIndex, "LocalTime")

		if err := parseOptionalInt64Field(fields, headerIndex, "SeqNo", &order.SeqNo); err != nil {
			logger.Error("警告: 第 %d 行 SeqNo 解析错误: %v，跳过", lineNum, err)
			lineNum++
			continue
		}

		orders = append(orders, order)
		lineNum++
	}

	return orders, nil
}

func ManualReadSzRawOrder(filepath string) ([]*model.SzRawOrder, error) {
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

	requiredHeaders := []string{
		"ChannelNo", "ApplSeqNum", "MDStreamID", "SecurityID", "SecurityIDSource",
		"Price", "OrderQty", "Side", "TransactTime", "OrdType",
	}

	for _, header := range requiredHeaders {
		if _, exists := headerIndex[header]; !exists {
			return nil, errorx.NewError("CSV文件缺少必要标题: %s", header)
		}
	}

	var orders []*model.SzRawOrder
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

		if !hasRequiredFields(fields, headerIndex, requiredHeaders) {
			logger.Info("警告: 第 %d 行字段数量不足（%d/%d），跳过", lineNum, len(fields), requiredFieldCount(headerIndex, requiredHeaders))
			lineNum++
			continue
		}

		order := &model.SzRawOrder{}

		if err := parseInt64Field(fields, headerIndex, "ChannelNo", &order.ChannelNo); err != nil {
			logger.Error("警告: 第 %d 行 ChannelNo 解析错误: %v，跳过", lineNum, err)
			lineNum++
			continue
		}

		if err := parseInt64Field(fields, headerIndex, "ApplSeqNum", &order.ApplSeqNum); err != nil {
			logger.Error("警告: 第 %d 行 ApplSeqNum 解析错误: %v，跳过", lineNum, err)
			lineNum++
			continue
		}

		order.MDStreamID = getOptionalFieldValue(fields, headerIndex, "MDStreamID")
		order.SecurityID = getOptionalFieldValue(fields, headerIndex, "SecurityID")

		if err := parseInt64Field(fields, headerIndex, "SecurityIDSource", &order.SecurityIDSource); err != nil {
			logger.Error("警告: 第 %d 行 SecurityIDSource 解析错误: %v，跳过", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "Price", &order.Price); err != nil {
			logger.Error("警告: 第 %d 行 Price 解析错误: %v，跳过", lineNum, err)
			lineNum++
			continue
		}

		if err := parseInt64Field(fields, headerIndex, "OrderQty", &order.OrderQty); err != nil {
			logger.Error("警告: 第 %d 行 OrderQty 解析错误: %v，跳过", lineNum, err)
			lineNum++
			continue
		}

		if err := parseIntField(fields, headerIndex, "Side", &order.Side); err != nil {
			logger.Error("警告: 第 %d 行 Side 解析错误: %v，跳过", lineNum, err)
			lineNum++
			continue
		}

		order.TransactTime = getOptionalFieldValue(fields, headerIndex, "TransactTime")

		if err := parseIntField(fields, headerIndex, "OrdType", &order.OrdType); err != nil {
			logger.Error("警告: 第 %d 行 OrdType 解析错误: %v，跳过", lineNum, err)
			lineNum++
			continue
		}

		order.LocalTime = getOptionalFieldValue(fields, headerIndex, "LocalTime")

		if err := parseOptionalInt64Field(fields, headerIndex, "SeqNo", &order.SeqNo); err != nil {
			logger.Error("警告: 第 %d 行 SeqNo 解析错误: %v，跳过", lineNum, err)
			lineNum++
			continue
		}

		orders = append(orders, order)
		lineNum++
	}

	return orders, nil
}
