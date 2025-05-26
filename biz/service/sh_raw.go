package service

import (
	"archive/zip"
	"bytes"
	"data-scrubber/biz/model"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

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

	// 手动分割标题行，移除末尾空字段
	headers := splitCSVFields(headerLine)
	if len(headers) > 0 && strings.TrimSpace(headers[len(headers)-1]) == "" {
		headers = headers[:len(headers)-1]
	}

	columnIndex := make(map[string]int, len(headers))
	for i, header := range headers {
		columnIndex[strings.TrimSpace(header)] = i
	}

	expectedFieldCount := len(headers)

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
				pos, err := findNextLine(file, start)
				if err != nil {
					errorChan <- fmt.Errorf("worker %d 定位行失败: %v", workerID, err)
					return
				}
				start = pos
			} else {
				// 第一个工作线程从文件开始处读取
				file.Seek(0, 0)
			}

			// 使用带缓冲的切片
			trades := make([]*model.SzRawTrade, 0, 10000)
			lineNumber := 1 // 当前处理的行号
			if workerID > 0 {
				// 非第一个工作线程需要跳过标题行
				lineNumber = 2
			}

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

					// 验证字段数量
					if len(fields) != expectedFieldCount {
						// 尝试修复：如果字段数多1且最后一个字段为空
						if len(fields) == expectedFieldCount+1 && strings.TrimSpace(fields[len(fields)-1]) == "" {
							fields = fields[:len(fields)-1]
						} else {
							// 记录错误但继续处理
							fmt.Printf("警告: 第%d行字段数量不匹配 (期望%d, 实际%d): %s\n",
								lineNumber, expectedFieldCount, len(fields), string(lineData))
							lineNumber++
							continue
						}
					}

					// 检查是否超出块边界
					pos, _ := file.Seek(0, 1)
					if pos > end && len(line) == 0 {
						break
					}

					// 跳过标题行
					if workerID == 0 && lineNumber == 1 {
						lineNumber++
						continue
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
					lineNumber++
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

// 找到指定位置后的下一行开始位置
func findNextLine(r io.ReadSeeker, start int64) (int64, error) {
	// 移动到指定位置
	if _, err := r.Seek(start, 0); err != nil {
		return 0, err
	}

	buf := make([]byte, 1)
	pos := start

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
