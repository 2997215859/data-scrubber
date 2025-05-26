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
	"sort"
	"strconv"
	"strings"
)

// sh 处理

func ManualReadShRawTrade(filepath string) ([]*model.ShRawTrade, error) {
	zipReader, err := zip.OpenReader(filepath)
	if err != nil {
		return nil, fmt.Errorf("打开zip文件失败: %v", err)
	}
	defer zipReader.Close()

	// 快速查找第一个CSV文件，避免遍历所有文件
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

	// 打开CSV文件
	csvReader, err := csvFile.Open()
	if err != nil {
		return nil, fmt.Errorf("打开CSV文件失败: %v", err)
	}
	defer csvReader.Close()

	// 使用预分配的切片，减少内存重分配
	trades := make([]*model.ShRawTrade, 0, 10000)

	csvParser := csv.NewReader(csvReader)
	csvParser.Comma = ','
	csvParser.LazyQuotes = true

	// 读取标题行
	headers, err := csvParser.Read()
	if err != nil {
		return nil, fmt.Errorf("读取CSV标题行失败: %v", err)
	}

	// 映射列名到索引位置
	columnIndex := make(map[string]int, len(headers))
	for i, header := range headers {
		columnIndex[header] = i
	}

	// 批量读取记录，提高效率
	//bufferSize := 4096
	records, err := csvParser.ReadAll()
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("读取CSV记录失败: %v", err)
	}

	// 预分配解析后的交易数据
	trades = make([]*model.ShRawTrade, 0, len(records))

	// 使用局部变量减少循环中的重复查找
	var (
		bizIndexIdx    int
		channelIdx     int
		securityIDIdx  int
		tickTimeIdx    int
		typeIdx        int
		buyOrderNoIdx  int
		sellOrderNoIdx int
		priceIdx       int
		qtyIdx         int
		tradeMoneyIdx  int
		tickBSFlagIdx  int
		localTimeIdx   int
		seqNoIdx       int
		exists         bool
	)

	if bizIndexIdx, exists = columnIndex["BizIndex"]; !exists {
		return nil, fmt.Errorf("CSV文件缺少BizIndex列")
	}
	if channelIdx, exists = columnIndex["Channel"]; !exists {
		return nil, fmt.Errorf("CSV文件缺少Channel列")
	}
	if securityIDIdx, exists = columnIndex["SecurityID"]; !exists {
		return nil, fmt.Errorf("CSV文件缺少SecurityID列")
	}
	if tickTimeIdx, exists = columnIndex["TickTime"]; !exists {
		return nil, fmt.Errorf("CSV文件缺少TickTime列")
	}
	if typeIdx, exists = columnIndex["Type"]; !exists {
		return nil, fmt.Errorf("CSV文件缺少Type列")
	}
	if buyOrderNoIdx, exists = columnIndex["BuyOrderNO"]; !exists {
		return nil, fmt.Errorf("CSV文件缺少BuyOrderNO列")
	}
	if sellOrderNoIdx, exists = columnIndex["SellOrderNO"]; !exists {
		return nil, fmt.Errorf("CSV文件缺少SellOrderNO列")
	}
	if priceIdx, exists = columnIndex["Price"]; !exists {
		return nil, fmt.Errorf("CSV文件缺少Price列")
	}
	if qtyIdx, exists = columnIndex["Qty"]; !exists {
		return nil, fmt.Errorf("CSV文件缺少Qty列")
	}
	if tradeMoneyIdx, exists = columnIndex["TradeMoney"]; !exists {
		return nil, fmt.Errorf("CSV文件缺少TradeMoney列")
	}
	if tickBSFlagIdx, exists = columnIndex["TickBSFlag"]; !exists {
		return nil, fmt.Errorf("CSV文件缺少TickBSFlag列")
	}
	if localTimeIdx, exists = columnIndex["LocalTime"]; !exists {
		return nil, fmt.Errorf("CSV文件缺少LocalTime列")
	}
	if seqNoIdx, exists = columnIndex["SeqNo"]; !exists {
		return nil, fmt.Errorf("CSV文件缺少SeqNo列")
	}

	// 优化的记录解析，减少重复检查
	for _, record := range records {
		if len(record) < len(headers) {
			continue // 跳过不完整的行
		}

		trade := &model.ShRawTrade{}

		// 解析各字段，使用预先获取的索引位置
		trade.BizIndex, _ = strconv.ParseInt(record[bizIndexIdx], 10, 64)
		trade.Channel, _ = strconv.ParseInt(record[channelIdx], 10, 64)
		trade.SecurityID = record[securityIDIdx]
		trade.TickTime = record[tickTimeIdx]
		trade.Type = record[typeIdx]
		trade.BuyOrderNo, _ = strconv.ParseInt(record[buyOrderNoIdx], 10, 64)
		trade.SellOrderNo, _ = strconv.ParseInt(record[sellOrderNoIdx], 10, 64)
		trade.Price, _ = strconv.ParseFloat(record[priceIdx], 64)
		trade.Qty, _ = strconv.ParseInt(record[qtyIdx], 10, 64)
		trade.TradeMoney, _ = strconv.ParseFloat(record[tradeMoneyIdx], 64)
		trade.TickBSFlag = record[tickBSFlagIdx]
		trade.LocalTime = record[localTimeIdx]
		trade.SeqNo, _ = strconv.ParseInt(record[seqNoIdx], 10, 64)

		trades = append(trades, trade)
	}

	return trades, nil
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

	list := make([]*model.SzRawTrade, 0)
	if err := gocsv.Unmarshal(csvReader, &list); err != nil {
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

	// 读取和处理上海数据
	shRawTradeList, err := ManualReadShRawTrade(shFilepath)
	if err != nil {
		return errorx.NewError("ReadShRawTrade(%s) error: %s", shFilepath, err)
	}
	logger.Info("Read Sh Raw Trade End")

	shTradeList, err := ShRawTrade2TradeList(date, shRawTradeList)
	if err != nil {
		return errorx.NewError("ShRawTrade2Trade(%s) error: %s", shFilepath, err)
	}
	logger.Info("Convert Sh Raw Trade End")

	// 读取和处理深圳数据
	szRawTradeList, err := ReadSzRawTrade(szFilepath)
	if err != nil {
		return errorx.NewError("ReadSzRawTrade(%s) error: %s", shFilepath, err)
	}
	szTradeList, err := SzRawTrade2TradeList(date, szRawTradeList)
	if err != nil {
		return errorx.NewError("SzRawTrade2Trade(%s) error: %s", shFilepath, err)
	}
	logger.Info("Read Sz Raw Trade End")

	// 排序
	tradeList := SortRaw(shTradeList, szTradeList)
	logger.Info("Sort Raw Trade End")

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
