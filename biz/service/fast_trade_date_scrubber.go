package service

import (
	"archive/zip"
	"data-scrubber/biz/errorx"
	"data-scrubber/biz/model"
	"data-scrubber/biz/utils"
	"fmt"
	logger "github.com/2997215859/golog"
	"github.com/gocarina/gocsv"
	"os"
	"path/filepath"
)

const QueueSize = 1000

func ReadShRawTradeStream(filepath string) (chan *model.ShRawTrade, error) {
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
	//defer zipReader.Close()

	if len(zipReader.File) != 1 {
		return nil, errorx.NewError("open filepath(%s) error: zip file num(%d) is not 1", filepath, len(zipReader.File))
	}

	csvFile := zipReader.File[0]
	csvReader, err := csvFile.Open()
	if err != nil {
		return nil, errorx.NewError("open filepath(%s) error: %v", filepath, err)
	}
	//defer csvReader.Close()

	ch := make(chan *model.ShRawTrade, QueueSize)
	go func() {
		if err := gocsv.UnmarshalToChan(csvReader, ch); err != nil {
			errorx.NewError("unmarshal filepath(%s) error: %v", filepath, err)
		}
	}()

	return ch, nil
}

//func ReadSzRawTradeStream2(filepath string) (chan *model.SzRawTrade, error) {
//
//	fileInfo, err := os.Stat(filepath)
//	if err != nil {
//		return nil, errorx.NewError("os.Stat(%s): %v", filepath, err)
//	}
//	if fileInfo.Size() == 0 {
//		return nil, errorx.NewError("file(%s) is empty", filepath)
//	}
//
//	zipReader, err := zip.OpenReader(filepath)
//	if err != nil {
//		return nil, errorx.NewError("open filepath(%s) error: %v", filepath, err)
//	}
//	//defer zipReader.Close()
//
//	if len(zipReader.File) != 1 {
//		return nil, errorx.NewError("open filepath(%s) error: zip file num(%d) is not 1", filepath, len(zipReader.File))
//	}
//
//	csvFile := zipReader.File[0]
//	csvReader, err := csvFile.Open()
//	if err != nil {
//		return nil, errorx.NewError("open filepath(%s) error: %v", filepath, err)
//	}
//	//defer csvReader.Close()
//
//	ch := make(chan *model.SzRawTrade, QueueSize)
//	go func() {
//		if err := gocsv.UnmarshalToChan(csvReader, ch); err != nil {
//			errorx.NewError("unmarshal filepath(%s) error: %v", filepath, err)
//		}
//	}()
//
//	return ch, nil
//}

func ReadSzRawTradeStream(filepath string) (chan *model.SzRawTrade, error) {

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
	//defer zipReader.Close()

	if len(zipReader.File) != 1 {
		return nil, errorx.NewError("open filepath(%s) error: zip file num(%d) is not 1", filepath, len(zipReader.File))
	}

	csvFile := zipReader.File[0]
	csvReader, err := csvFile.Open()
	if err != nil {
		return nil, errorx.NewError("open filepath(%s) error: %v", filepath, err)
	}
	//defer csvReader.Close()

	trimmedReader := &streamingTrimCommaReader{src: csvReader}

	ch := make(chan *model.SzRawTrade, QueueSize)
	go func() {
		if err := gocsv.UnmarshalToChan(trimmedReader, ch); err != nil {
			errorx.NewError("unmarshal filepath(%s) error: %v", filepath, err)
		}
	}()

	return ch, nil
}

func MergeRawTradeStream(srcDir string, dstDir string, date string) error {
	shFilepath := filepath.Join(srcDir, date, fmt.Sprintf("%s_mdl_4_24_0.csv.zip", date))
	szFilepath := filepath.Join(srcDir, date, fmt.Sprintf("%s_mdl_6_36_0.csv.zip", date))

	// 读取和处理上海数据
	shRawTradeChan, err := ReadShRawTradeStream(shFilepath)
	if err != nil {
		return errorx.NewError("ReadShRawTrade(%s) error: %s", shFilepath, err)
	}

	// 读取和处理深圳数据
	szRawTradeChan, err := ReadSzRawTradeStream(szFilepath)
	if err != nil {
		return errorx.NewError("ReadSzRawTrade(%s) error: %s", szFilepath, err)
	}

	result := make(chan *model.Trade, QueueSize)

	// 排序
	// 归并逻辑：按顺序读取两个通道的数据
	go func() {
		defer close(result)

		if err := SortRawTradeStream(date, shRawTradeChan, szRawTradeChan, result); err != nil {
			logger.Error("SortRawTradeStream error: %s", err)
		}
	}()

	if err := WriteParquetStream(date, dstDir, result); err != nil {
		logger.Error("WriteParquetStream error: %s", err)
	}

	return nil
}

func SortRawTradeStream(date string, shRawTradeChan <-chan *model.ShRawTrade, szRawTradeChan <-chan *model.SzRawTrade, result chan<- *model.Trade) error {

	// 初始化：读取两个通道的第一个值
	val1, ok1 := <-shRawTradeChan
	val2, ok2 := <-szRawTradeChan

	for ok1 || ok2 { // 只要有一个通道未读完
		// 情况1：两个通道都有数据，比较当前值大小
		if ok1 && ok2 {
			if val1.LocalTime <= val2.LocalTime {
				v, err := ShRawTrade2Trade(date, val1)
				if err != nil {
					return errorx.NewError("ShRawTrade2Trade(%+v) error: %v", val1, err)
				}
				if v != nil {
					//result <- v
				}
				val1, ok1 = <-shRawTradeChan
			} else {
				v, err := SzRawTrade2Trade(date, val2)
				if err != nil {
					return errorx.NewError("SzRawTrade2Trade(%+v) error: %v", val2, err)
				}
				if v != nil {
					result <- v
				}
				val2, ok2 = <-szRawTradeChan
			}
			continue
		}

		// 情况2：只有 ch1 有数据
		if ok1 {
			v, err := ShRawTrade2Trade(date, val1)
			if err != nil {
				return errorx.NewError("ShRawTrade2Trade(%+v) error: %v", val1, err)
			}
			if v != nil {
				//result <- v
			}
			val1, ok1 = <-shRawTradeChan
			continue
		}

		// 情况3：只有 ch2 有数据
		if ok2 {
			v, err := SzRawTrade2Trade(date, val2)
			if err != nil {
				return errorx.NewError("SzRawTrade2Trade(%+v) error: %v", val2, err)
			}
			if v != nil {
				result <- v
			}
			val2, ok2 = <-szRawTradeChan
			continue
		}
	}
	return nil
}

// 修改原有的WriteParquetStream函数
func WriteParquetStream(date string, dstDir string, result <-chan *model.Trade) error {
	dstDir = filepath.Join(dstDir, date)
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return errorx.NewError("MkdirAll(%s) error: %v", dstDir, err)
	}

	// 构建输出文件名
	outputPath := filepath.Join(dstDir, fmt.Sprintf("%s_trades.parquet", date))

	//创建写入器
	pw, err := NewParquetWriter(outputPath, new(model.Trade))
	if err != nil {
		return errorx.NewError("NewParquetWriter error: %s", err)
	}

	defer func() {
		if err := pw.Close(); err != nil {
			logger.Error("关闭Parquet写入器时出错: %v", err)
		}
	}()

	// 从通道读取数据并写入Parquet文件
	for trade := range result {
		if trade == nil {
			continue
		}

		logger.Info("aaaaaaaaaaaaaa %+v %+v %+v %+v", trade.InstrumentId, trade.TradeId, trade.LocalTimestamp, utils.NsToTimeString(trade.LocalTimestamp))

		if err := pw.Write(trade); err != nil {
			logger.Error("WriteParquetStream error: %v", err)
		}
	}

	return nil
}
