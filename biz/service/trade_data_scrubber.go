package service

import (
	"archive/zip"
	"compress/gzip"
	"data-scrubber/biz/constdef"
	"data-scrubber/biz/errorx"
	"data-scrubber/biz/model"
	"data-scrubber/biz/utils"
	"encoding/csv"
	"fmt"
	logger "github.com/2997215859/golog"
	"github.com/gocarina/gocsv"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// sh 处理

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

func ReadShRawTrade(filepath string) ([]*model.ShRawTrade, error) {
	fileInfo, err := os.Stat(filepath)
	if err != nil {
		return nil, errorx.NewError("os.Stat(%s): %v", filepath, err)
	}
	if fileInfo.Size() == 0 {
		return nil, errorx.NewError("file(%s) is empty", filepath)
	}

	zipReader, err := zip.OpenReader(filepath)
	if err != nil {
		return nil, errorx.NewError("open filepath(%s) error: %v", filepath, err)
	}
	defer zipReader.Close()

	if len(zipReader.File) != 1 {
		return nil, errorx.NewError("open filepath(%s) error: zip file num(%d) is not 1", filepath, len(zipReader.File))
	}

	csvFile := zipReader.File[0]
	csvReader, err := csvFile.Open()
	if err != nil {
		return nil, errorx.NewError("open filepath(%s) error: %v", filepath, err)
	}
	defer csvReader.Close()

	list := make([]*model.ShRawTrade, 0)
	//list := &model.ShRawTrade{}
	if err := gocsv.Unmarshal(csvReader, &list); err != nil {
		return nil, errorx.NewError("unmarshal filepath(%s) error: %v", filepath, err)
	}
	return list, nil
}

func ShRaw2Direction(TickBsFlag string) string {
	if TickBsFlag == "B" {
		return constdef.DirectionBuy
	}
	if TickBsFlag == "S" {
		return constdef.DirectionSell
	}
	return constdef.DirectionUnknown
}

func ShRawTrade2Trade(date string, v *model.ShRawTrade) (*model.Trade, error) {
	if v.Type != "T" {
		return nil, nil
	}

	tradeTimestamp, err := utils.TimeToNano(date, v.TickTime)
	if err != nil {
		return nil, errorx.NewError("timeToNano(%s %s) error: %v", date, v.TickTime, err)
	}

	localTimestamp, err := utils.TimeToNano(date, v.LocalTime)
	if err != nil {
		return nil, errorx.NewError("timeToNano(%s %s) error: %v", date, v.LocalTime, err)
	}

	direction := ShRaw2Direction(v.TickBSFlag)
	//if direction == constdef.DirectionUnknown {
	//	return nil, errorx.NewError("ShRaw2Direction(%s) error", v.TickBSFlag)
	//}

	res := &model.Trade{
		InstrumentId:   fmt.Sprintf("%s.SH", v.SecurityID),
		TradeTimestamp: tradeTimestamp,
		TradeId:        v.BizIndex,
		Price:          v.Price,
		Volume:         v.Qty,
		Turnover:       v.TradeMoney,
		Direction:      direction,
		BuyOrderId:     v.BuyOrderNo,
		SellOrderId:    v.SellOrderNo,
		LocalTimestamp: localTimestamp,
	}

	return res, nil
}

func ShRawTrade2TradeList(date string, rawList []*model.ShRawTrade) ([]*model.Trade, error) {
	var res []*model.Trade
	for _, v := range rawList {
		trade, err := ShRawTrade2Trade(date, v)
		if err != nil {
			return nil, err
		}
		if trade == nil { // 说明不是所需要的数据，但也不应该报 error
			continue
		}

		res = append(res, trade)
	}

	return res, nil
}

// sz 处理

func ManualReadSzRawTrade(filepath string) ([]*model.SzRawTrade, error) {
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
	tmpfile, err := os.CreateTemp("", "sz_raw_trade_*.csv")
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
	headerLine, err := readLine(tmpfile)
	if err != nil {
		return nil, fmt.Errorf("读取CSV标题行失败: %v", err)
	}

	// 手动分割标题行
	headers := splitCSVFields(headerLine)
	columnIndex := make(map[string]int, len(headers))
	for i, header := range headers {
		columnIndex[strings.TrimSpace(header)] = i
	}

	// 确定每个工作线程处理的块大小
	numWorkers := runtime.NumCPU() // 默认使用CPU核心数
	chunkSize := fileSize / int64(numWorkers)
	if chunkSize < 1024*1024 { // 最小1MB块大小
		chunkSize = 1024 * 1024
		numWorkers = int(fileSize / chunkSize)
		if numWorkers == 0 {
			numWorkers = 1
		}
	}

	// 创建通道用于收集结果
	resultChan := make(chan []*model.SzRawTrade, numWorkers)
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
				_, err := findNextLine(file)
				if err != nil {
					errorChan <- fmt.Errorf("worker %d 定位行失败: %v", workerID, err)
					return
				}
				start, _ = file.Seek(0, 1) // 更新起始位置到下一行的开始
			}

			// 使用带缓冲的切片
			trades := make([]*model.SzRawTrade, 0, 10000)

			// 读取块内的所有记录
			buffer := make([]byte, 65536) // 64KB缓冲区
			var line []byte
			var eof bool

			for {
				// 读取数据块
				n, err := file.Read(buffer)
				if err == io.EOF {
					eof = true
				} else if err != nil {
					errorChan <- fmt.Errorf("worker %d 读取文件失败: %v", workerID, err)
					return
				}

				if n == 0 {
					break
				}

				// 合并上一次的剩余数据
				if len(line) > 0 {
					line = append(line, buffer[:n]...)
				} else {
					line = buffer[:n]
				}

				// 处理所有完整的行
				for {
					idx := bytes.IndexByte(line, '\n')
					if idx == -1 {
						break
					}

					lineData := line[:idx]
					line = line[idx+1:]

					// 处理深圳CSV文件每行末尾多余的逗号
					if len(lineData) > 0 && lineData[len(lineData)-1] == ',' {
						lineData = lineData[:len(lineData)-1]
					}

					// 手动分割字段
					fields := splitCSVFields(lineData)

					// 跳过空行
					if len(fields) == 0 {
						continue
					}

					// 检查是否超出块边界
					pos, _ := file.Seek(0, 1)
					if pos > end && len(line) == 0 {
						break
					}

					// 解析记录
					trade := &model.SzRawTrade{}

					if idx, exists := columnIndex["ChannelNo"]; exists && idx < len(fields) {
						trade.ChannelNo, _ = strconv.ParseInt(strings.TrimSpace(fields[idx]), 10, 64)
					}
					if idx, exists := columnIndex["ApplSeqNum"]; exists && idx < len(fields) {
						trade.ApplSeqNum, _ = strconv.ParseInt(strings.TrimSpace(fields[idx]), 10, 64)
					}
					if idx, exists := columnIndex["MDStreamID"]; exists && idx < len(fields) {
						trade.MDStreamID = strings.TrimSpace(fields[idx])
					}
					if idx, exists := columnIndex["BidApplSeqNum"]; exists && idx < len(fields) {
						trade.BidApplSeqNum, _ = strconv.ParseInt(strings.TrimSpace(fields[idx]), 10, 64)
					}
					if idx, exists := columnIndex["OfferApplSeqNum"]; exists && idx < len(fields) {
						trade.OfferApplSeqNum, _ = strconv.ParseInt(strings.TrimSpace(fields[idx]), 10, 64)
					}
					if idx, exists := columnIndex["SecurityID"]; exists && idx < len(fields) {
						trade.SecurityID = strings.TrimSpace(fields[idx])
					}
					if idx, exists := columnIndex["SecurityIDSource"]; exists && idx < len(fields) {
						trade.SecurityIDSource, _ = strconv.ParseInt(strings.TrimSpace(fields[idx]), 10, 64)
					}
					if idx, exists := columnIndex["LastPx"]; exists && idx < len(fields) {
						trade.LastPx, _ = strconv.ParseFloat(strings.TrimSpace(fields[idx]), 64)
					}
					if idx, exists := columnIndex["LastQty"]; exists && idx < len(fields) {
						trade.LastQty, _ = strconv.ParseInt(strings.TrimSpace(fields[idx]), 10, 64)
					}
					if idx, exists := columnIndex["ExecType"]; exists && idx < len(fields) {
						execType, _ := strconv.ParseInt(strings.TrimSpace(fields[idx]), 10, 32)
						trade.ExecType = int(execType)
					}
					if idx, exists := columnIndex["TransactTime"]; exists && idx < len(fields) {
						trade.TransactTime = strings.TrimSpace(fields[idx])
					}
					if idx, exists := columnIndex["LocalTime"]; exists && idx < len(fields) {
						trade.LocalTime = strings.TrimSpace(fields[idx])
					}
					if idx, exists := columnIndex["SeqNo"]; exists && idx < len(fields) {
						trade.SeqNo, _ = strconv.ParseInt(strings.TrimSpace(fields[idx]), 10, 64)
					}

					trades = append(trades, trade)
				}

				if eof {
					break
				}
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
	var allTrades []*model.SzRawTrade
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

// 读取一行数据
func readLine(r io.Reader) ([]byte, error) {
	var line []byte
	buf := make([]byte, 4096)

	for {
		n, err := r.Read(buf)
		if n > 0 {
			line = append(line, buf[:n]...)
			if idx := bytes.IndexByte(line, '\n'); idx != -1 {
				return line[:idx], nil
			}
		}
		if err != nil {
			return line, err
		}
	}
}

// 找到下一行的开始位置
func findNextLine(r io.ReaderAt) (int64, error) {
	buf := make([]byte, 1)
	pos, _ := r.Seek(0, 1)

	for {
		n, err := r.Read(buf)
		if n == 0 || err != nil {
			return pos, err
		}
		pos++
		if buf[0] == '\n' {
			return pos, nil
		}
	}
}

// 手动分割CSV字段，处理带引号的值
func splitCSVFields(line []byte) []string {
	var fields []string
	var currentField []byte
	var inQuotes bool
	var escaped bool

	for i := 0; i < len(line); i++ {
		b := line[i]

		if escaped {
			currentField = append(currentField, b)
			escaped = false
			continue
		}

		switch b {
		case '"':
			if inQuotes {
				if i+1 < len(line) && line[i+1] == '"' {
					// 双引号表示转义
					currentField = append(currentField, '"')
					i++
				} else {
					// 引号结束
					inQuotes = false
				}
			} else {
				// 引号开始
				if len(currentField) == 0 {
					inQuotes = true
				} else {
					// 引号出现在字段中间，作为普通字符处理
					currentField = append(currentField, b)
				}
			}
		case ',':
			// 分隔符
			if !inQuotes {
				fields = append(fields, string(currentField))
				currentField = nil
			} else {
				// 引号内的逗号，作为普通字符处理
				currentField = append(currentField, b)
			}
		default:
			// 普通字符
			currentField = append(currentField, b)
		}
	}

	// 添加最后一个字段
	if currentField != nil {
		fields = append(fields, string(currentField))
	}

	return fields
}

func ReadSzRawTrade(filepath string) ([]*model.SzRawTrade, error) {
	fileInfo, err := os.Stat(filepath)
	if err != nil {
		return nil, errorx.NewError("os.Stat(%s): %v", filepath, err)
	}
	if fileInfo.Size() == 0 {
		return nil, errorx.NewError("file(%s) is empty", filepath)
	}

	zipReader, err := zip.OpenReader(filepath)
	if err != nil {
		return nil, errorx.NewError("open filepath(%s) error: %v", filepath, err)
	}
	defer zipReader.Close()

	if len(zipReader.File) != 1 {
		return nil, errorx.NewError("open filepath(%s) error: zip file num(%d) is not 1", filepath, len(zipReader.File))
	}

	csvFile := zipReader.File[0]
	csvReader, err := csvFile.Open()
	if err != nil {
		return nil, errorx.NewError("open filepath(%s) error: %v", filepath, err)
	}
	defer csvReader.Close()

	trimmedReader := &streamingTrimCommaReader{src: csvReader}

	list := make([]*model.SzRawTrade, 0)
	if err := gocsv.Unmarshal(trimmedReader, &list); err != nil {
		return nil, errorx.NewError("unmarshal filepath(%s) error: %v", filepath, err)
	}
	return list, nil
}

func SzRaw2Direction(buyOrderId, sellOrderId int64) string {
	if buyOrderId > sellOrderId {
		return constdef.DirectionBuy
	}
	if buyOrderId < sellOrderId {
		return constdef.DirectionSell
	}
	return constdef.DirectionUnknown
}

func SzRawTrade2Trade(date string, v *model.SzRawTrade) (*model.Trade, error) {
	// 只处理交易记录
	if v.ExecType != 52 {
		return nil, nil
	}

	tradeTimestamp, err := utils.TimeToNano(date, v.TransactTime)
	if err != nil {
		return nil, errorx.NewError("timeToNano(%s %s) error: %v", date, v.TransactTime, err)
	}

	localTimestamp, err := utils.TimeToNano(date, v.LocalTime)
	if err != nil {
		return nil, errorx.NewError("timeToNano(%s %s) error: %v", date, v.LocalTime, err)
	}

	direction := SzRaw2Direction(v.BidApplSeqNum, v.OfferApplSeqNum)
	//if direction == constdef.DirectionUnknown {
	//	return nil, errorx.NewError("ShRaw2Direction(%d %d) error", v.BidApplSeqNum, v.OfferApplSeqNum)
	//}

	res := &model.Trade{
		InstrumentId:   fmt.Sprintf("%s.SZ", v.SecurityID),
		TradeTimestamp: tradeTimestamp,
		TradeId:        v.SeqNo,
		Price:          v.LastPx,
		Volume:         v.LastQty,
		Turnover:       v.LastPx * float64(v.LastQty),
		Direction:      direction,
		BuyOrderId:     v.BidApplSeqNum,
		SellOrderId:    v.OfferApplSeqNum,
		LocalTimestamp: localTimestamp,
	}
	return res, nil
}

func SzRawTrade2TradeList(date string, rawList []*model.SzRawTrade) ([]*model.Trade, error) {
	var res []*model.Trade
	for _, v := range rawList {
		trade, err := SzRawTrade2Trade(date, v)
		if err != nil {
			return nil, err
		}
		if trade == nil { // 说明不是所需要的数据，但也不应该报 error
			continue
		}

		res = append(res, trade)
	}

	return res, nil
}

// ==== 合并 trade

func MergeRawTrade(srcDir string, dstDir string, date string) error {
	shFilepath := filepath.Join(srcDir, date, fmt.Sprintf("%s_mdl_4_24_0.csv.zip", date))
	szFilepath := filepath.Join(srcDir, date, fmt.Sprintf("%s_mdl_6_36_0.csv.zip", date))

	// 读取和处理深圳数据
	szRawTradeList, err := ManualReadSzRawTrade(szFilepath)
	if err != nil {
		return errorx.NewError("ReadSzRawTrade(%s) error: %s", szFilepath, err)
	}
	logger.Info("Read Sz Raw Trade End")

	szTradeList, err := SzRawTrade2TradeList(date, szRawTradeList)
	if err != nil {
		return errorx.NewError("SzRawTrade2Trade(%s) error: %s", szFilepath, err)
	}
	logger.Info("Convert Sz Raw Trade End")

	// 读取和处理上海数据
	shRawTradeList, err := ReadShRawTrade(shFilepath)
	if err != nil {
		return errorx.NewError("ReadShRawTrade(%s) error: %s", shFilepath, err)
	}
	logger.Info("Convert Sh Raw Trade End")

	shTradeList, err := ShRawTrade2TradeList(date, shRawTradeList)
	if err != nil {
		return errorx.NewError("ShRawTrade2Trade(%s) error: %s", shFilepath, err)
	}
	logger.Info("Sort Sh Raw Trade End")

	// 排序
	tradeList := SortRaw(shTradeList, szTradeList)
	logger.Info("Convert Raw Trade End")

	// 写入
	if err := WriteTrade(dstDir, date, tradeList); err != nil {
		return errorx.NewError("WriteTrade(%s) date(%s) error: %v", dstDir, date, err)
	}
	logger.Info("Write Raw Trade End")
	return nil
}

func SortRaw(a []*model.Trade, b []*model.Trade) []*model.Trade {
	// 分别对 a 和 b 排序
	sort.Slice(a, func(i, j int) bool {
		return a[i].LocalTimestamp < a[j].LocalTimestamp
	})
	sort.Slice(b, func(i, j int) bool {
		return b[i].LocalTimestamp < b[j].LocalTimestamp
	})

	// 双指针合并有序切片
	result := make([]*model.Trade, 0, len(a)+len(b))
	i, j := 0, 0

	for i < len(a) && j < len(b) {
		if a[i].LocalTimestamp < b[j].LocalTimestamp {
			result = append(result, a[i])
			i++
		} else {
			result = append(result, b[j])
			j++
		}
	}

	// 添加剩余元素
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)

	return result
}

func WriteTrade(dstDir string, date string, tradeList []*model.Trade) error {
	dstDir = filepath.Join(dstDir, date)
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return errorx.NewError("MkdirAll(%s) error: %v", dstDir, err)
	}

	filepath := filepath.Join(dstDir, fmt.Sprintf("%s_trade.gz", date))

	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return errorx.NewError("open file(%s): %v", filepath, err)
	}
	defer file.Close()

	gzWriter := gzip.NewWriter(file)
	defer gzWriter.Close()

	if err := gocsv.Marshal(&tradeList, gzWriter); err != nil {
		return errorx.NewError("filepath(%s) gocsv.Marshal error: %v", filepath, err)
	}
	return nil
}
