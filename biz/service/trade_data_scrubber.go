package service

import (
	"archive/zip"
	"compress/gzip"
	"data-scrubber/biz/constdef"
	"data-scrubber/biz/errorx"
	"data-scrubber/biz/model"
	"data-scrubber/biz/utils"
	"fmt"
	logger "github.com/2997215859/golog"
	"github.com/gocarina/gocsv"
	"os"
	"path/filepath"
	"sort"
)

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
	if v.ExecType != 70 {
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

	// 读取和处理上海数据
	logger.Info("Read Sh Raw Trade Begin")
	shRawTradeList, err := ManualReadShRawTrade(shFilepath)
	if err != nil {
		return errorx.NewError("ReadShRawTrade(%s) error: %s", shFilepath, err)
	}
	logger.Info("Read Sh Raw Trade End")

	shTradeList, err := ShRawTrade2TradeList(date, shRawTradeList)
	if err != nil {
		return errorx.NewError("ShRawTrade2Trade(%s) error: %s", shFilepath, err)
	}
	logger.Info("Sort Sh Raw Trade End")

	// 读取和处理深圳数据
	logger.Info("Read Sz Raw Trade Begin")
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

	// 排序
	tradeList := SortRaw(shTradeList, szTradeList)
	logger.Info("Convert Raw Trade End")

	// 写入
	logger.Info("Write Trade.gz Begin")
	if err := WriteTrade(dstDir, date, tradeList); err != nil {
		return errorx.NewError("WriteTrade(%s) date(%s) error: %v", dstDir, date, err)
	}
	logger.Info("Write Trade.gz End")

	logger.Info("Write Trade.parquet Begin")
	if err := WriteParquet(dstDir, date, tradeList); err != nil {
		return errorx.NewError("WriteParquet(%s) date(%s) error: %v", dstDir, date, err)
	}
	logger.Info("Write Trade.parquet End")
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

func WriteParquet(dstDir string, date string, tradeList []*model.Trade) error {
	dstDir = filepath.Join(dstDir, date)
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return errorx.NewError("MkdirAll(%s) error: %v", dstDir, err)
	}

	filepath := filepath.Join(dstDir, fmt.Sprintf("%s_trade.parquet", date))

	//创建写入器
	pw, err := NewParquetWriter(filepath, new(model.Trade))
	if err != nil {
		return errorx.NewError("NewParquetWriter error: %s", err)
	}

	defer func() {
		if err := pw.Close(); err != nil {
			logger.Error("关闭Parquet写入器时出错: %v", err)
		}
	}()

	for _, trade := range tradeList {
		if trade == nil {
			continue
		}

		if err := pw.Write(trade); err != nil {
			logger.Error("WriteParquetStream error: %v", err)
		}
	}

	return nil
}
