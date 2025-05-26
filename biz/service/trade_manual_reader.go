package service

import (
	"archive/zip"
	"data-scrubber/biz/model"
	"encoding/csv"
	"fmt"
	logger "github.com/2997215859/golog"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

func ManualReadSzRawTrade(filepath string) ([]*model.SzRawTrade, error) {
	// 打开ZIP文件
	zipReader, err := zip.OpenReader(filepath)
	if err != nil {
		return nil, fmt.Errorf("打开ZIP文件失败: %v", err)
	}
	defer zipReader.Close()

	// 检查ZIP文件中是否只有一个文件
	if len(zipReader.File) != 1 {
		return nil, fmt.Errorf("ZIP文件中应该只包含一个文件，实际包含 %d 个文件", len(zipReader.File))
	}

	// 获取ZIP文件中的文件
	zipFile := zipReader.File[0]

	// 打开ZIP文件中的文件
	rc, err := zipFile.Open()
	if err != nil {
		return nil, fmt.Errorf("打开ZIP文件中的文件失败: %v", err)
	}
	defer rc.Close()

	// 创建CSV读取器
	csvReader := csv.NewReader(rc)

	// 读取标题行
	headers, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("读取CSV标题行失败: %v", err)
	}

	// 映射标题到列索引
	headerIndex := make(map[string]int)
	for i, header := range headers {
		headerIndex[strings.TrimSpace(header)] = i
	}

	// 验证必要的标题是否存在
	requiredHeaders := []string{
		"ChannelNo", "ApplSeqNum", "MDStreamID", "BidApplSeqNum",
		"OfferApplSeqNum", "SecurityID", "SecurityIDSource",
		"LastPx", "LastQty", "ExecType", "TransactTime", "LocalTime", "SeqNo",
	}

	for _, header := range requiredHeaders {
		if _, exists := headerIndex[header]; !exists {
			return nil, fmt.Errorf("CSV文件缺少必要的标题: %s", header)
		}
	}

	// 存储解析结果
	var trades []*model.SzRawTrade

	// 读取剩余行数据
	lines, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("读取CSV数据行失败: %v", err)
	}

	// 解析每一行数据
	for i, line := range lines {
		// 确保每行有足够的字段
		if len(line) < len(requiredHeaders) {
			log.Printf("警告: 第 %d 行字段数量不足，跳过该行", i+2) // +2 是因为标题行和索引从0开始
			continue
		}

		// 创建新的model.SzRawTrade实例
		trade := &model.SzRawTrade{}

		// 使用标题映射来解析各字段
		if err := parseInt64Field(line, headerIndex, "ChannelNo", &trade.ChannelNo); err != nil {
			log.Printf("警告: 第 %d 行 ChannelNo 解析错误: %v，跳过该行", i+2, err)
			continue
		}

		if err := parseInt64Field(line, headerIndex, "ApplSeqNum", &trade.ApplSeqNum); err != nil {
			log.Printf("警告: 第 %d 行 ApplSeqNum 解析错误: %v，跳过该行", i+2, err)
			continue
		}

		trade.MDStreamID = strings.TrimSpace(line[headerIndex["MDStreamID"]])

		if err := parseInt64Field(line, headerIndex, "BidApplSeqNum", &trade.BidApplSeqNum); err != nil {
			log.Printf("警告: 第 %d 行 BidApplSeqNum 解析错误: %v，跳过该行", i+2, err)
			continue
		}

		if err := parseInt64Field(line, headerIndex, "OfferApplSeqNum", &trade.OfferApplSeqNum); err != nil {
			log.Printf("警告: 第 %d 行 OfferApplSeqNum 解析错误: %v，跳过该行", i+2, err)
			continue
		}

		trade.SecurityID = strings.TrimSpace(line[headerIndex["SecurityID"]])

		if err := parseInt64Field(line, headerIndex, "SecurityIDSource", &trade.SecurityIDSource); err != nil {
			log.Printf("警告: 第 %d 行 SecurityIDSource 解析错误: %v，跳过该行", i+2, err)
			continue
		}

		if err := parseFloat64Field(line, headerIndex, "LastPx", &trade.LastPx); err != nil {
			log.Printf("警告: 第 %d 行 LastPx 解析错误: %v，跳过该行", i+2, err)
			continue
		}

		if err := parseInt64Field(line, headerIndex, "LastQty", &trade.LastQty); err != nil {
			log.Printf("警告: 第 %d 行 LastQty 解析错误: %v，跳过该行", i+2, err)
			continue
		}

		if err := parseIntField(line, headerIndex, "ExecType", &trade.ExecType); err != nil {
			log.Printf("警告: 第 %d 行 ExecType 解析错误: %v，跳过该行", i+2, err)
			continue
		}

		trade.TransactTime = strings.TrimSpace(line[headerIndex["TransactTime"]])
		trade.LocalTime = strings.TrimSpace(line[headerIndex["LocalTime"]])

		if err := parseInt64Field(line, headerIndex, "SeqNo", &trade.SeqNo); err != nil {
			log.Printf("警告: 第 %d 行 SeqNo 解析错误: %v，跳过该行", i+2, err)
			continue
		}

		// 将解析成功的交易添加到结果列表
		trades = append(trades, trade)
	}

	return trades, nil
}

// 辅助函数：解析int64类型字段
func parseInt64Field(line []string, headerIndex map[string]int, fieldName string, target *int64) error {
	valueStr := strings.TrimSpace(line[headerIndex[fieldName]])
	value, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		return fmt.Errorf("解析 %s 失败: %v", fieldName, err)
	}
	*target = value
	return nil
}

// 辅助函数：解析float64类型字段
func parseFloat64Field(line []string, headerIndex map[string]int, fieldName string, target *float64) error {
	valueStr := strings.TrimSpace(line[headerIndex[fieldName]])
	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return fmt.Errorf("解析 %s 失败: %v", fieldName, err)
	}
	*target = value
	return nil
}

// 辅助函数：解析int类型字段
func parseIntField(line []string, headerIndex map[string]int, fieldName string, target *int) error {
	valueStr := strings.TrimSpace(line[headerIndex[fieldName]])
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return fmt.Errorf("解析 %s 失败: %v", fieldName, err)
	}
	*target = value
	return nil
}

func ManualReadShRawTrade(filepath string) ([]*model.ShRawTrade, error) {
	zipReader, err := zip.OpenReader(filepath)
	if err != nil {
		return nil, fmt.Errorf("打开zip文件失败: %v", err)
	}
	defer zipReader.Close()

	// 查找第一个CSV文件
	var csvFile *zip.File
	for _, f := range zipReader.File {
		if strings.HasSuffix(strings.ToLower(f.Name), ".csv") {
			csvFile = f
			break
		}
	}

	if csvFile == nil {
		return nil, fmt.Errorf("未找到CSV文件")
	}

	// 创建临时文件用于解压
	tmpfile, err := os.CreateTemp("", "sh_raw_trade_*.csv")
	if err != nil {
		return nil, fmt.Errorf("创建临时文件失败: %v", err)
	}
	tmpPath := tmpfile.Name()
	defer os.Remove(tmpPath) // 清理临时文件
	defer tmpfile.Close()

	// 将ZIP中的CSV解压到临时文件
	rc, err := csvFile.Open()
	if err != nil {
		return nil, fmt.Errorf("打开CSV文件失败: %v", err)
	}
	defer rc.Close()

	_, err = io.Copy(tmpfile, rc)
	if err != nil {
		return nil, fmt.Errorf("解压CSV文件失败: %v", err)
	}

	// 获取文件信息和大小
	fileInfo, err := tmpfile.Stat()
	if err != nil {
		return nil, fmt.Errorf("获取文件信息失败: %v", err)
	}
	fileSize := fileInfo.Size()

	// 读取标题行以获取列索引
	tmpfile.Seek(0, 0)
	reader := csv.NewReader(tmpfile)
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("读取CSV标题行失败: %v", err)
	}

	columnIndex := make(map[string]int, len(headers))
	for i, header := range headers {
		columnIndex[header] = i
	}

	// 确定每个工作线程处理的块大小
	numWorkers := runtime.NumCPU() // 默认使用CPU核心数
	logger.Info("numWorkers=%d", numWorkers)

	chunkSize := fileSize / int64(numWorkers)
	if chunkSize < 1024*1024 { // 最小1MB块大小
		chunkSize = 1024 * 1024
		numWorkers = int(fileSize / chunkSize)
		if numWorkers == 0 {
			numWorkers = 1
		}
	}

	// 创建通道用于收集结果
	resultChan := make(chan []*model.ShRawTrade, numWorkers)
	errorChan := make(chan error, 1)
	var wg sync.WaitGroup

	// 启动工作线程
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			start := int64(workerID) * chunkSize
			end := start + chunkSize
			if end > fileSize {
				end = fileSize
			}

			// 打开文件的新句柄
			file, err := os.Open(tmpPath)
			if err != nil {
				errorChan <- fmt.Errorf("worker %d 打开文件失败: %v", workerID, err)
				return
			}
			defer file.Close()

			// 移动到块的起始位置
			if workerID > 0 {
				// 找到下一行的开始位置
				file.Seek(start, 0)
				buf := make([]byte, 1)
				for {
					_, err := file.Read(buf)
					if err != nil || buf[0] == '\n' {
						break
					}
				}
				start, _ = file.Seek(0, 1) // 更新起始位置到下一行的开始
			}

			// 创建CSV阅读器
			reader := csv.NewReader(file)
			reader.LazyQuotes = true

			// 如果不是第一个工作线程，跳过标题行
			if workerID > 0 {
				reader.Read() // 跳过标题行
			}

			// 设置读取限制，防止读取超出块边界
			remaining := end - start
			if remaining <= 0 {
				resultChan <- []*model.ShRawTrade{}
				return
			}

			// 使用带缓冲的切片
			trades := make([]*model.ShRawTrade, 0, 10000)

			// 读取块内的所有记录
			for {
				record, err := reader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					errorChan <- fmt.Errorf("worker %d 读取记录失败: %v", workerID, err)
					return
				}

				// 检查是否超出块边界
				pos, _ := file.Seek(0, 1)
				if pos > end {
					break
				}

				// 解析记录
				trade := &model.ShRawTrade{}

				if idx, exists := columnIndex["BizIndex"]; exists {
					trade.BizIndex, _ = strconv.ParseInt(record[idx], 10, 64)
				}
				if idx, exists := columnIndex["Channel"]; exists {
					trade.Channel, _ = strconv.ParseInt(record[idx], 10, 64)
				}
				if idx, exists := columnIndex["SecurityID"]; exists {
					trade.SecurityID = record[idx]
				}
				if idx, exists := columnIndex["TickTime"]; exists {
					trade.TickTime = record[idx]
				}
				if idx, exists := columnIndex["Type"]; exists {
					trade.Type = record[idx]
				}
				if idx, exists := columnIndex["BuyOrderNO"]; exists {
					trade.BuyOrderNo, _ = strconv.ParseInt(record[idx], 10, 64)
				}
				if idx, exists := columnIndex["SellOrderNO"]; exists {
					trade.SellOrderNo, _ = strconv.ParseInt(record[idx], 10, 64)
				}
				if idx, exists := columnIndex["Price"]; exists {
					trade.Price, _ = strconv.ParseFloat(record[idx], 64)
				}
				if idx, exists := columnIndex["Qty"]; exists {
					trade.Qty, _ = strconv.ParseInt(record[idx], 10, 64)
				}
				if idx, exists := columnIndex["TradeMoney"]; exists {
					trade.TradeMoney, _ = strconv.ParseFloat(record[idx], 64)
				}
				if idx, exists := columnIndex["TickBSFlag"]; exists {
					trade.TickBSFlag = record[idx]
				}
				if idx, exists := columnIndex["LocalTime"]; exists {
					trade.LocalTime = record[idx]
				}
				if idx, exists := columnIndex["SeqNo"]; exists {
					trade.SeqNo, _ = strconv.ParseInt(record[idx], 10, 64)
				}

				trades = append(trades, trade)
			}

			resultChan <- trades
		}(i)
	}

	// 等待所有工作线程完成并收集结果
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// 合并结果
	var allTrades []*model.ShRawTrade
	for trades := range resultChan {
		allTrades = append(allTrades, trades...)
	}

	// 检查错误
	select {
	case err := <-errorChan:
		return nil, err
	default:
		return allTrades, nil
	}
}
