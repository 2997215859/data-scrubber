package service

import (
	"archive/zip"
	"bufio"
	"compress/gzip"
	"data-scrubber/biz/constdef"
	"data-scrubber/biz/errorx"
	"data-scrubber/biz/model"
	"data-scrubber/biz/utils"
	"data-scrubber/config"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	logger "github.com/2997215859/golog"
	"github.com/gocarina/gocsv"
)

// ==== sh 处理

func ManualReadShRawSnapshot(filepath string) ([]*model.ShRawSnapshot, error) {
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

	// 创建文本读取器
	reader := bufio.NewReader(rc)

	// 读取标题行
	headerLine, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("读取标题行失败: %v", err)
	}
	headerLine = strings.TrimSpace(headerLine)

	// 处理标题行，去除行末可能的逗号
	headers := splitLine(headerLine)

	// 映射标题到列索引
	headerIndex := make(map[string]int)
	for i, header := range headers {
		headerIndex[strings.TrimSpace(header)] = i
	}

	// 验证必要的标题是否存在
	requiredHeaders := []string{
		"UpdateTime", "SecurityID", "ImageStatus", "PreCloPrice", "OpenPrice",
		"WarUpperPri", "WarLowerPri",
		"HighPrice", "LowPrice", "LastPrice", "ClosePrice", "InstruStatus",
		"TradNumber", "TradVolume", "Turnover", "TotalBidVol", "WAvgBidPri",
		"AltWAvgBidPri", "TotalAskVol", "WAvgAskPri", "AltWAvgAskPri", "EtfBuyNumber",
		"EtfBuyVolume", "EtfBuyMoney", "EtfSellNumber", "EtfSellVolume", "ETFSellMoney",
		"YieldToMatu", "TotWarExNum", "WarLowerPri", "WarUpperPri", "WiDBuyNum",
		"WiDBuyVol", "WiDBuyMon", "WiDSellNum", "WiDSellVol", "WiDSellMon",
		"TotBidNum", "TotSellNum", "MaxBidDur", "MaxSellDur", "BidNum",
		"SellNum", "IOPV",
	}
	for _, header := range requiredHeaders {
		if _, exists := headerIndex[header]; !exists {
			return nil, fmt.Errorf("CSV文件缺少必要的标题: %s", header)
		}
	}

	// 存储解析结果
	var snapshots []*model.ShRawSnapshot
	lineNum := 2 // 从第2行开始(标题是第1行)
	// todo 这里的 lineNum 处理有问题，但因为现在还没用到，所以没有造成实际影响，其他数据结构也是，后面需要改掉

	// 逐行读取数据
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("读取数据行失败: %v", err)
		}

		// 处理当前行，去除行末可能的逗号
		fields := splitLine(strings.TrimSpace(line))

		// 确保每行有足够的字段
		if len(fields) < len(requiredHeaders) {
			logger.Warn("警告: 第 %d 行字段数量不足，跳过该行", lineNum)
			lineNum++
			continue
		}

		// 创建新的ShRawSnapshot实例
		snapshot := &model.ShRawSnapshot{}

		// 使用标题映射来解析各字段
		snapshot.UpdateTime = strings.TrimSpace(fields[headerIndex["UpdateTime"]])
		snapshot.SecurityID = strings.TrimSpace(fields[headerIndex["SecurityID"]])

		if err := parseIntField(fields, headerIndex, "ImageStatus", &snapshot.ImageStatus); err != nil {
			logger.Warn("警告: 第 %d 行 ImageStatus 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "PreCloPrice", &snapshot.PreCloPrice); err != nil {
			logger.Warn("警告: 第 %d 行 PreCloPrice 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "OpenPrice", &snapshot.OpenPrice); err != nil {
			logger.Warn("警告: 第 %d 行 OpenPrice 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "HighPrice", &snapshot.HighPrice); err != nil {
			logger.Warn("警告: 第 %d 行 HighPrice 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "LowPrice", &snapshot.LowPrice); err != nil {
			logger.Warn("警告: 第 %d 行 LowPrice 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "LastPrice", &snapshot.LastPrice); err != nil {
			logger.Warn("警告: 第 %d 行 LastPrice 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "ClosePrice", &snapshot.ClosePrice); err != nil {
			logger.Warn("警告: 第 %d 行 ClosePrice 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "WarLowerPri", &snapshot.WarLowerPri); err != nil {
			logger.Warn("警告: 第 %d 行 LastPrice 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "WarUpperPri", &snapshot.WarUpperPri); err != nil {
			logger.Warn("警告: 第 %d 行 ClosePrice 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		snapshot.InstruStatus = strings.TrimSpace(fields[headerIndex["InstruStatus"]])

		if err := parseInt64Field(fields, headerIndex, "TradNumber", &snapshot.TradNumber); err != nil {
			logger.Warn("警告: 第 %d 行 TradNumber 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "TradVolume", &snapshot.TradVolume); err != nil {
			logger.Warn("警告: 第 %d 行 TradVolume 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "Turnover", &snapshot.Turnover); err != nil {
			logger.Warn("警告: 第 %d 行 Turnover 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "TotalBidVol", &snapshot.TotalBidVol); err != nil {
			logger.Warn("警告: 第 %d 行 TotalBidVol 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "WAvgBidPri", &snapshot.WAvgBidPri); err != nil {
			logger.Warn("警告: 第 %d 行 WAvgBidPri 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "AltWAvgBidPri", &snapshot.AltWAvgBidPri); err != nil {
			logger.Warn("警告: 第 %d 行 AltWAvgBidPri 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "TotalAskVol", &snapshot.TotalAskVol); err != nil {
			logger.Warn("警告: 第 %d 行 TotalAskVol 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "WAvgAskPri", &snapshot.WAvgAskPri); err != nil {
			logger.Warn("警告: 第 %d 行 WAvgAskPri 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "AltWAvgAskPri", &snapshot.AltWAvgAskPri); err != nil {
			logger.Warn("警告: 第 %d 行 AltWAvgAskPri 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseIntField(fields, headerIndex, "EtfBuyNumber", &snapshot.EtfBuyNumber); err != nil {
			logger.Warn("警告: 第 %d 行 EtfBuyNumber 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "EtfBuyVolume", &snapshot.EtfBuyVolume); err != nil {
			logger.Warn("警告: 第 %d 行 EtfBuyVolume 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "EtfBuyMoney", &snapshot.EtfBuyMoney); err != nil {
			logger.Warn("警告: 第 %d 行 EtfBuyMoney 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseIntField(fields, headerIndex, "EtfSellNumber", &snapshot.EtfSellNumber); err != nil {
			logger.Warn("警告: 第 %d 行 EtfSellNumber 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "EtfSellVolume", &snapshot.EtfSellVolume); err != nil {
			logger.Warn("警告: 第 %d 行 EtfSellVolume 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "ETFSellMoney", &snapshot.ETFSellMoney); err != nil {
			logger.Warn("警告: 第 %d 行 ETFSellMoney 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "YieldToMatu", &snapshot.YieldToMatu); err != nil {
			logger.Warn("警告: 第 %d 行 YieldToMatu 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "TotWarExNum", &snapshot.TotWarExNum); err != nil {
			logger.Warn("警告: 第 %d 行 TotWarExNum 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "WarLowerPri", &snapshot.WarLowerPri); err != nil {
			logger.Warn("警告: 第 %d 行 WarLowerPri 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "WarUpperPri", &snapshot.WarUpperPri); err != nil {
			logger.Warn("警告: 第 %d 行 WarUpperPri 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseIntField(fields, headerIndex, "WiDBuyNum", &snapshot.WiDBuyNum); err != nil {
			logger.Warn("警告: 第 %d 行 WiDBuyNum 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "WiDBuyVol", &snapshot.WiDBuyVol); err != nil {
			logger.Warn("警告: 第 %d 行 WiDBuyVol 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "WiDBuyMon", &snapshot.WiDBuyMon); err != nil {
			logger.Warn("警告: 第 %d 行 WiDBuyMon 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseIntField(fields, headerIndex, "WiDSellNum", &snapshot.WiDSellNum); err != nil {
			logger.Warn("警告: 第 %d 行 WiDSellNum 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "WiDSellVol", &snapshot.WiDSellVol); err != nil {
			logger.Warn("警告: 第 %d 行 WiDSellVol 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "WiDSellMon", &snapshot.WiDSellMon); err != nil {
			logger.Warn("警告: 第 %d 行 WiDSellMon 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseIntField(fields, headerIndex, "TotBidNum", &snapshot.TotBidNum); err != nil {
			logger.Warn("警告: 第 %d 行 TotBidNum 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseIntField(fields, headerIndex, "TotSellNum", &snapshot.TotSellNum); err != nil {
			logger.Warn("警告: 第 %d 行 TotSellNum 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseIntField(fields, headerIndex, "MaxBidDur", &snapshot.MaxBidDur); err != nil {
			logger.Warn("警告: 第 %d 行 MaxBidDur 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseIntField(fields, headerIndex, "MaxSellDur", &snapshot.MaxSellDur); err != nil {
			logger.Warn("警告: 第 %d 行 MaxSellDur 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseIntField(fields, headerIndex, "BidNum", &snapshot.BidNum); err != nil {
			logger.Warn("警告: 第 %d 行 BidNum 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseIntField(fields, headerIndex, "SellNum", &snapshot.SellNum); err != nil {
			logger.Warn("警告: 第 %d 行 SellNum 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "IOPV", &snapshot.IOPV); err != nil {
			logger.Warn("警告: 第 %d 行 IOPV 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		// 解析10档卖盘数据
		for i := 1; i <= 10; i++ {
			priceField := fmt.Sprintf("AskPrice%d", i)
			volumeField := fmt.Sprintf("AskVolume%d", i)

			var pricePtr *float64
			var volumePtr *float64

			switch i {
			case 1:
				pricePtr = &snapshot.AskPrice1
				volumePtr = &snapshot.AskVolume1
			case 2:
				pricePtr = &snapshot.AskPrice2
				volumePtr = &snapshot.AskVolume2
			case 3:
				pricePtr = &snapshot.AskPrice3
				volumePtr = &snapshot.AskVolume3
			case 4:
				pricePtr = &snapshot.AskPrice4
				volumePtr = &snapshot.AskVolume4
			case 5:
				pricePtr = &snapshot.AskPrice5
				volumePtr = &snapshot.AskVolume5
			case 6:
				pricePtr = &snapshot.AskPrice6
				volumePtr = &snapshot.AskVolume6
			case 7:
				pricePtr = &snapshot.AskPrice7
				volumePtr = &snapshot.AskVolume7
			case 8:
				pricePtr = &snapshot.AskPrice8
				volumePtr = &snapshot.AskVolume8
			case 9:
				pricePtr = &snapshot.AskPrice9
				volumePtr = &snapshot.AskVolume9
			case 10:
				pricePtr = &snapshot.AskPrice10
				volumePtr = &snapshot.AskVolume10
			}

			_ = parseFloat64Field(fields, headerIndex, priceField, pricePtr)

			_ = parseFloat64Field(fields, headerIndex, volumeField, volumePtr)

			lineNum++
		}

		// 解析10档买盘数据
		for i := 1; i <= 10; i++ {
			priceField := fmt.Sprintf("BidPrice%d", i)
			volumeField := fmt.Sprintf("BidVolume%d", i)

			var pricePtr *float64
			var volumePtr *float64

			switch i {
			case 1:
				pricePtr = &snapshot.BidPrice1
				volumePtr = &snapshot.BidVolume1
			case 2:
				pricePtr = &snapshot.BidPrice2
				volumePtr = &snapshot.BidVolume2
			case 3:
				pricePtr = &snapshot.BidPrice3
				volumePtr = &snapshot.BidVolume3
			case 4:
				pricePtr = &snapshot.BidPrice4
				volumePtr = &snapshot.BidVolume4
			case 5:
				pricePtr = &snapshot.BidPrice5
				volumePtr = &snapshot.BidVolume5
			case 6:
				pricePtr = &snapshot.BidPrice6
				volumePtr = &snapshot.BidVolume6
			case 7:
				pricePtr = &snapshot.BidPrice7
				volumePtr = &snapshot.BidVolume7
			case 8:
				pricePtr = &snapshot.BidPrice8
				volumePtr = &snapshot.BidVolume8
			case 9:
				pricePtr = &snapshot.BidPrice9
				volumePtr = &snapshot.BidVolume9
			case 10:
				pricePtr = &snapshot.BidPrice10
				volumePtr = &snapshot.BidVolume10
			}

			_ = parseFloat64Field(fields, headerIndex, priceField, pricePtr)

			_ = parseFloat64Field(fields, headerIndex, volumeField, volumePtr)

			lineNum++
		}

		//// 解析买卖盘订单数
		//for i := 1; i <= 10; i++ {
		//	buyOrdersField := fmt.Sprintf("NumOrdersB%d", i)
		//	sellOrdersField := fmt.Sprintf("NumOrdersS%d", i)
		//
		//	var buyOrdersPtr *int
		//	var sellOrdersPtr *int
		//
		//	switch i {
		//	case 1:
		//		buyOrdersPtr = &snapshot.NumOrdersB1
		//		sellOrdersPtr = &snapshot.NumOrdersS1
		//	case 2:
		//		buyOrdersPtr = &snapshot.NumOrdersB2
		//		sellOrdersPtr = &snapshot.NumOrdersS2
		//	case 3:
		//		buyOrdersPtr = &snapshot.NumOrdersB3
		//		sellOrdersPtr = &snapshot.NumOrdersS3
		//	case 4:
		//		buyOrdersPtr = &snapshot.NumOrdersB4
		//		sellOrdersPtr = &snapshot.NumOrdersS4
		//	case 5:
		//		buyOrdersPtr = &snapshot.NumOrdersB5
		//		sellOrdersPtr = &snapshot.NumOrdersS5
		//	case 6:
		//		buyOrdersPtr = &snapshot.NumOrdersB6
		//		sellOrdersPtr = &snapshot.NumOrdersS6
		//	case 7:
		//		buyOrdersPtr = &snapshot.NumOrdersB7
		//		sellOrdersPtr = &snapshot.NumOrdersS7
		//	case 8:
		//		buyOrdersPtr = &snapshot.NumOrdersB8
		//		sellOrdersPtr = &snapshot.NumOrdersS8
		//	case 9:
		//		buyOrdersPtr = &snapshot.NumOrdersB9
		//		sellOrdersPtr = &snapshot.NumOrdersS9
		//	case 10:
		//		buyOrdersPtr = &snapshot.NumOrdersB10
		//		sellOrdersPtr = &snapshot.NumOrdersS10
		//	}
		//
		//	parseIntField(fields, headerIndex, buyOrdersField, buyOrdersPtr)
		//
		//	parseIntField(fields, headerIndex, sellOrdersField, sellOrdersPtr)
		//
		//	lineNum++
		//
		//}

		snapshot.LocalTime = strings.TrimSpace(fields[headerIndex["LocalTime"]])

		if err := parseInt64Field(fields, headerIndex, "SeqNo", &snapshot.SeqNo); err != nil {
			logger.Warn("警告: 第 %d 行 SeqNo 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		// 将解析成功的快照添加到结果列表
		snapshots = append(snapshots, snapshot)
		lineNum++
	}

	return snapshots, nil
}

// 计算涨跌停价格
func CalculateLimitPrices(instrumentId string, preClose float64) (highLimit, lowLimit float64) {
	// 处理停牌场景
	//if snapshot.InstruStatus != "Normal" {
	//	return math.NaN(), math.NaN()
	//}

	// 判断证券类型（主板/ST/科创板等）
	var ratio float64
	if strings.HasPrefix(instrumentId, "60") { // 主板股票
		if strings.Contains(instrumentId, "ST") {
			ratio = 0.05 // ST 股票 5%
		} else {
			ratio = 0.10 // 主板非 ST 10%
		}
	} else if strings.HasPrefix(instrumentId, "68") { // 科创板
		ratio = 0.20 // 科创板 20%
	}

	// 核心计算（四舍五入到小数点后 2 位）
	highLimit = math.Round(preClose*(1+ratio)*100) / 100
	lowLimit = math.Round(preClose*(1-ratio)*100) / 100

	// 新股/特殊规则单独处理（示例逻辑，需补充）
	//if isNewStock(snapshot.InstrumentId) {
	//	return handleNewStockLimits(snapshot.PreClose)
	//}

	return highLimit, lowLimit
}

func ShRawSnapshot2Snapshot(date string, v *model.ShRawSnapshot) (*model.Snapshot, error) {
	updateTimestamp, err := utils.TimeToNano(date, v.UpdateTime)
	if err != nil {
		return nil, errorx.NewError("timeToNano(%s %s) error: %v", date, v.UpdateTime, err)
	}

	localTimestamp, err := utils.TimeToNano(date, v.LocalTime)
	if err != nil {
		return nil, errorx.NewError("timeToNano(%s %s) error: %v", date, v.LocalTime, err)
	}

	//highLimit, lowLimit := CalculateLimitPrices(v.SecurityID, v.PreCloPrice)
	if utils.IsChinaStockCodeFirstChar(v.SecurityID) == false {
		return nil, nil
	}

	instrumentId := fmt.Sprintf("%s.SH", v.SecurityID)

	//if utils.IsBStock(instrumentId) {
	//	return nil, nil
	//}

	priceLimit, err := GetStockLimit(instrumentId)
	if err != nil {
		logger.Error("GetStockLimit(%s) error: %v", instrumentId, err)
		priceLimit = &PriceLimit{
			InstrumentId: instrumentId,
			HighLimit:    0.0,
			LowLimit:     0.0,
		}
	}

	res := &model.Snapshot{
		InstrumentId:    instrumentId,
		UpdateTimestamp: updateTimestamp,
		Last:            v.LastPrice,
		PreClose:        v.PreCloPrice,
		Open:            v.OpenPrice,
		High:            v.HighPrice,
		Low:             v.LowPrice,
		Close:           v.ClosePrice,
		TradeNumber:     v.TradNumber,
		TradeVolume:     int64(v.TradVolume),
		TradeTurnover:   v.Turnover,
		HighLimit:       priceLimit.HighLimit,
		LowLimit:        priceLimit.LowLimit,
		BidVolumeList: utils.Float64ToInt64([]float64{
			v.BidVolume1, v.BidVolume2, v.BidVolume3, v.BidVolume4, v.BidVolume5,
			v.BidVolume6, v.BidVolume7, v.BidVolume8, v.BidVolume9, v.BidVolume10,
		}),
		BidPriceList: []float64{
			v.BidPrice1, v.BidPrice2, v.BidPrice3, v.BidPrice4, v.BidPrice5,
			v.BidPrice6, v.BidPrice7, v.BidPrice8, v.BidPrice9, v.BidPrice10,
		},
		AskVolumeList: utils.Float64ToInt64([]float64{
			v.AskVolume1, v.AskVolume2, v.AskVolume3, v.AskVolume4, v.AskVolume5,
			v.AskVolume6, v.AskVolume7, v.AskVolume8, v.AskVolume9, v.AskVolume10,
		}),
		AskPriceList: []float64{
			v.AskPrice1, v.AskPrice2, v.AskPrice3, v.AskPrice4, v.AskPrice5,
			v.AskPrice6, v.AskPrice7, v.AskPrice8, v.AskPrice9, v.AskPrice10,
		},
		LocalTimestamp: localTimestamp,
	}
	return res, nil
}

func ShRawSnapshot2SnapshotList(date string, rawList []*model.ShRawSnapshot) ([]*model.Snapshot, error) {
	var res []*model.Snapshot
	for _, v := range rawList {
		item, err := ShRawSnapshot2Snapshot(date, v)
		if err != nil {
			return nil, err
		}
		if item == nil { // 说明不是所需要的数据，但也不应该报 error
			continue
		}

		res = append(res, item)
	}

	return res, nil
}

// ==== sz 处理

func ManualReadSzRawSnapshot(filepath string) ([]*model.SzRawSnapshot, error) {
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

	// 创建文本读取器
	reader := bufio.NewReader(rc)

	// 读取标题行
	headerLine, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("读取标题行失败: %v", err)
	}
	headerLine = strings.TrimSpace(headerLine)

	// 处理标题行，去除行末可能的逗号
	headers := splitLine(headerLine)

	// 映射标题到列索引
	headerIndex := make(map[string]int)
	for i, header := range headers {
		headerIndex[strings.TrimSpace(header)] = i
	}

	// 验证必要的标题是否存在
	requiredHeaders := []string{
		"UpdateTime", "SecurityID", "PreCloPrice", "TurnNum", "Volume", "Turnover", "LastPrice",
		"OpenPrice", "HighPrice", "LowPrice", "TotalBidQty", "TotalOfferQty",
		"HighLimitPrice", "LowLimitPrice",
		"AskPrice1", "AskVolume1", "BidPrice1", "BidVolume1", "NumOrdersB1", "NumOrdersS1", "LocalTime", "SeqNo",
		"AskPrice2", "AskVolume2", "BidPrice2", "BidVolume2", "NumOrdersB2", "NumOrdersS2",
		"AskPrice3", "AskVolume3", "BidPrice3", "BidVolume3", "NumOrdersB3", "NumOrdersS3",
		"AskPrice4", "AskVolume4", "BidPrice4", "BidVolume4", "NumOrdersB4", "NumOrdersS4",
		"AskPrice5", "AskVolume5", "BidPrice5", "BidVolume5", "NumOrdersB5", "NumOrdersS5",
		"AskPrice6", "AskVolume6", "BidPrice6", "BidVolume6", "NumOrdersB6", "NumOrdersS6",
		"AskPrice7", "AskVolume7", "BidPrice7", "BidVolume7", "NumOrdersB7", "NumOrdersS7",
		"AskPrice8", "AskVolume8", "BidPrice8", "BidVolume8", "NumOrdersB8", "NumOrdersS8",
		"AskPrice9", "AskVolume9", "BidPrice9", "BidVolume9", "NumOrdersB9", "NumOrdersS9",
		"AskPrice10", "AskVolume10", "BidPrice10", "BidVolume10", "NumOrdersB10", "NumOrdersS10",
	}
	for _, header := range requiredHeaders {
		if _, exists := headerIndex[header]; !exists {
			return nil, fmt.Errorf("CSV文件缺少必要的标题: %s", header)
		}
	}

	// 存储解析结果
	var snapshots []*model.SzRawSnapshot
	lineNum := 2 // 从第2行开始(标题是第1行)

	// 逐行读取数据
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("读取数据行失败: %v", err)
		}

		// 处理当前行，去除行末可能的逗号
		fields := splitLine(strings.TrimSpace(line))

		// 确保每行有足够的字段
		if len(fields) < len(requiredHeaders) {
			logger.Warn("警告: 第 %d 行字段数量不足，跳过该行", lineNum)
			lineNum++
			continue
		}

		// 创建新的SzRawSnapshot实例
		snapshot := &model.SzRawSnapshot{}

		// 使用标题映射来解析各字段
		snapshot.UpdateTime = strings.TrimSpace(fields[headerIndex["UpdateTime"]])
		snapshot.SecurityID = strings.TrimSpace(fields[headerIndex["SecurityID"]])

		if err := parseFloat64Field(fields, headerIndex, "PreCloPrice", &snapshot.PreCloPrice); err != nil {
			logger.Warn("警告: 第 %d 行 PreCloPrice 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseInt64Field(fields, headerIndex, "TurnNum", &snapshot.TurnNum); err != nil {
			logger.Warn("警告: 第 %d 行 TurnNum 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseInt64Field(fields, headerIndex, "Volume", &snapshot.Volume); err != nil {
			logger.Warn("警告: 第 %d 行 Volume 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "Turnover", &snapshot.Turnover); err != nil {
			logger.Warn("警告: 第 %d 行 Turnover 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "LastPrice", &snapshot.LastPrice); err != nil {
			logger.Warn("警告: 第 %d 行 LastPrice 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "OpenPrice", &snapshot.OpenPrice); err != nil {
			logger.Warn("警告: 第 %d 行 OpenPrice 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "HighPrice", &snapshot.HighPrice); err != nil {
			logger.Warn("警告: 第 %d 行 HighPrice 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "LowPrice", &snapshot.LowPrice); err != nil {
			logger.Warn("警告: 第 %d 行 LowPrice 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseInt64Field(fields, headerIndex, "TotalBidQty", &snapshot.TotalBidQty); err != nil {
			logger.Warn("警告: 第 %d 行 TotalBidQty 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseInt64Field(fields, headerIndex, "TotalOfferQty", &snapshot.TotalOfferQty); err != nil {
			logger.Warn("警告: 第 %d 行 TotalOfferQty 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "HighLimitPrice", &snapshot.HighLimitPrice); err != nil {
			logger.Warn("警告: 第 %d 行 TotalBidQty 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		if err := parseFloat64Field(fields, headerIndex, "LowLimitPrice", &snapshot.LowLimitPrice); err != nil {
			logger.Warn("警告: 第 %d 行 TotalOfferQty 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		// 解析10档卖盘数据
		for i := 1; i <= 10; i++ {
			priceField := fmt.Sprintf("AskPrice%d", i)
			volumeField := fmt.Sprintf("AskVolume%d", i)

			var pricePtr *float64
			var volumePtr *int64

			switch i {
			case 1:
				pricePtr = &snapshot.AskPrice1
				volumePtr = &snapshot.AskVolume1
			case 2:
				pricePtr = &snapshot.AskPrice2
				volumePtr = &snapshot.AskVolume2
			case 3:
				pricePtr = &snapshot.AskPrice3
				volumePtr = &snapshot.AskVolume3
			case 4:
				pricePtr = &snapshot.AskPrice4
				volumePtr = &snapshot.AskVolume4
			case 5:
				pricePtr = &snapshot.AskPrice5
				volumePtr = &snapshot.AskVolume5
			case 6:
				pricePtr = &snapshot.AskPrice6
				volumePtr = &snapshot.AskVolume6
			case 7:
				pricePtr = &snapshot.AskPrice7
				volumePtr = &snapshot.AskVolume7
			case 8:
				pricePtr = &snapshot.AskPrice8
				volumePtr = &snapshot.AskVolume8
			case 9:
				pricePtr = &snapshot.AskPrice9
				volumePtr = &snapshot.AskVolume9
			case 10:
				pricePtr = &snapshot.AskPrice10
				volumePtr = &snapshot.AskVolume10
			}

			_ = parseFloat64Field(fields, headerIndex, priceField, pricePtr)

			_ = parseInt64Field(fields, headerIndex, volumeField, volumePtr)

			lineNum++

		}

		// 解析10档买盘数据
		for i := 1; i <= 10; i++ {
			priceField := fmt.Sprintf("BidPrice%d", i)
			volumeField := fmt.Sprintf("BidVolume%d", i)

			var pricePtr *float64
			var volumePtr *int64

			switch i {
			case 1:
				pricePtr = &snapshot.BidPrice1
				volumePtr = &snapshot.BidVolume1
			case 2:
				pricePtr = &snapshot.BidPrice2
				volumePtr = &snapshot.BidVolume2
			case 3:
				pricePtr = &snapshot.BidPrice3
				volumePtr = &snapshot.BidVolume3
			case 4:
				pricePtr = &snapshot.BidPrice4
				volumePtr = &snapshot.BidVolume4
			case 5:
				pricePtr = &snapshot.BidPrice5
				volumePtr = &snapshot.BidVolume5
			case 6:
				pricePtr = &snapshot.BidPrice6
				volumePtr = &snapshot.BidVolume6
			case 7:
				pricePtr = &snapshot.BidPrice7
				volumePtr = &snapshot.BidVolume7
			case 8:
				pricePtr = &snapshot.BidPrice8
				volumePtr = &snapshot.BidVolume8
			case 9:
				pricePtr = &snapshot.BidPrice9
				volumePtr = &snapshot.BidVolume9
			case 10:
				pricePtr = &snapshot.BidPrice10
				volumePtr = &snapshot.BidVolume10
			}

			_ = parseFloat64Field(fields, headerIndex, priceField, pricePtr)

			_ = parseInt64Field(fields, headerIndex, volumeField, volumePtr)

			lineNum++
		}

		// 解析买卖盘订单数
		for i := 1; i <= 10; i++ {
			buyOrdersField := fmt.Sprintf("NumOrdersB%d", i)
			sellOrdersField := fmt.Sprintf("NumOrdersS%d", i)

			var buyOrdersPtr *int
			var sellOrdersPtr *int

			switch i {
			case 1:
				buyOrdersPtr = &snapshot.NumOrdersB1
				sellOrdersPtr = &snapshot.NumOrdersS1
			case 2:
				buyOrdersPtr = &snapshot.NumOrdersB2
				sellOrdersPtr = &snapshot.NumOrdersS2
			case 3:
				buyOrdersPtr = &snapshot.NumOrdersB3
				sellOrdersPtr = &snapshot.NumOrdersS3
			case 4:
				buyOrdersPtr = &snapshot.NumOrdersB4
				sellOrdersPtr = &snapshot.NumOrdersS4
			case 5:
				buyOrdersPtr = &snapshot.NumOrdersB5
				sellOrdersPtr = &snapshot.NumOrdersS5
			case 6:
				buyOrdersPtr = &snapshot.NumOrdersB6
				sellOrdersPtr = &snapshot.NumOrdersS6
			case 7:
				buyOrdersPtr = &snapshot.NumOrdersB7
				sellOrdersPtr = &snapshot.NumOrdersS7
			case 8:
				buyOrdersPtr = &snapshot.NumOrdersB8
				sellOrdersPtr = &snapshot.NumOrdersS8
			case 9:
				buyOrdersPtr = &snapshot.NumOrdersB9
				sellOrdersPtr = &snapshot.NumOrdersS9
			case 10:
				buyOrdersPtr = &snapshot.NumOrdersB10
				sellOrdersPtr = &snapshot.NumOrdersS10
			}

			_ = parseIntField(fields, headerIndex, buyOrdersField, buyOrdersPtr)

			_ = parseIntField(fields, headerIndex, sellOrdersField, sellOrdersPtr)
		}

		snapshot.LocalTime = strings.TrimSpace(fields[headerIndex["LocalTime"]])

		if err := parseInt64Field(fields, headerIndex, "SeqNo", &snapshot.SeqNo); err != nil {
			logger.Warn("警告: 第 %d 行 SeqNo 解析错误: %v，跳过该行", lineNum, err)
			lineNum++
			continue
		}

		// 将解析成功的快照添加到结果列表
		snapshots = append(snapshots, snapshot)
		lineNum++
	}

	return snapshots, nil
}

func SzRawSnapshot2Snapshot(date string, v *model.SzRawSnapshot) (*model.Snapshot, error) {
	updateTimestamp, err := utils.TimeToNano(date, v.UpdateTime)
	if err != nil {
		return nil, errorx.NewError("timeToNano(%s %s) error: %v", date, v.UpdateTime, err)
	}

	localTimestamp, err := utils.TimeToNano(date, v.LocalTime)
	if err != nil {
		return nil, errorx.NewError("timeToNano(%s %s) error: %v", date, v.LocalTime, err)
	}

	if utils.IsChinaStockCodeFirstChar(v.SecurityID) == false {
		return nil, nil
	}

	res := &model.Snapshot{
		InstrumentId:    fmt.Sprintf("%s.SZ", v.SecurityID),
		UpdateTimestamp: updateTimestamp,
		Last:            v.LastPrice,
		PreClose:        v.PreCloPrice,
		Open:            v.OpenPrice,
		High:            v.HighPrice,
		Low:             v.LowPrice,
		Close:           0.0,
		TradeNumber:     v.TurnNum,
		TradeVolume:     v.Volume,
		TradeTurnover:   v.Turnover,
		HighLimit:       v.HighLimitPrice,
		LowLimit:        v.LowLimitPrice,
		BidVolumeList: []int64{
			v.BidVolume1, v.BidVolume2, v.BidVolume3, v.BidVolume4, v.BidVolume5,
			v.BidVolume6, v.BidVolume7, v.BidVolume8, v.BidVolume9, v.BidVolume10,
		},
		BidPriceList: []float64{
			v.BidPrice1, v.BidPrice2, v.BidPrice3, v.BidPrice4, v.BidPrice5,
			v.BidPrice6, v.BidPrice7, v.BidPrice8, v.BidPrice9, v.BidPrice10,
		},
		AskVolumeList: []int64{
			v.AskVolume1, v.AskVolume2, v.AskVolume3, v.AskVolume4, v.AskVolume5,
			v.AskVolume6, v.AskVolume7, v.AskVolume8, v.AskVolume9, v.AskVolume10,
		},
		AskPriceList: []float64{
			v.AskPrice1, v.AskPrice2, v.AskPrice3, v.AskPrice4, v.AskPrice5,
			v.AskPrice6, v.AskPrice7, v.AskPrice8, v.AskPrice9, v.AskPrice10,
		},
		LocalTimestamp: localTimestamp,
	}
	return res, nil
}

func SzRawSnapshot2SnapshotList(date string, rawList []*model.SzRawSnapshot) ([]*model.Snapshot, error) {
	var res []*model.Snapshot
	for _, v := range rawList {
		item, err := SzRawSnapshot2Snapshot(date, v)
		if err != nil {
			return nil, err
		}
		if item == nil { // 说明不是所需要的数据，但也不应该报 error
			continue
		}

		res = append(res, item)
	}

	return res, nil
}

// ==== 合并 Snapshot

func MergeRawSnapshot(srcDir string, dstDir string, date string) error {
	// 刷新一下 turshare 数据
	if err := UpdateTuShareDailyLimit(date); err != nil {
		return err
	}

	dstDir = filepath.Join(dstDir, constdef.DataTypeSnapshot, date)

	shFilepath := filepath.Join(srcDir, date, fmt.Sprintf("%s_MarketData.csv.zip", date))
	szFilepath := filepath.Join(srcDir, date, fmt.Sprintf("%s_mdl_6_28_0.csv.zip", date))

	// 读取和处理上海数据
	logger.Info("Read Sh Raw Snapshot Begin")
	shRawList, err := ManualReadShRawSnapshot(shFilepath)
	if err != nil {
		return errorx.NewError("ReadShRaw(%s) error: %s", shFilepath, err)
	}
	logger.Info("Read Sh Raw Snapshot End")

	shList, err := ShRawSnapshot2SnapshotList(date, shRawList)
	if err != nil {
		return errorx.NewError("ShRawSnapshot2SnapshotList(%s) error: %s", shFilepath, err)
	}
	logger.Info("Convert Sh Raw Snapshot End")

	// 读取和处理深圳数据
	logger.Info("Read Sz Raw Snapshot Begin")
	szRawList, err := ManualReadSzRawSnapshot(szFilepath)
	if err != nil {
		return errorx.NewError("ManualReadSzRawSnapshot(%s) error: %s", szFilepath, err)
	}
	logger.Info("Read Sz Raw Snapshot End")

	szList, err := SzRawSnapshot2SnapshotList(date, szRawList)
	if err != nil {
		return errorx.NewError("SzRawSnapshot2SnapshotList(%s) error: %s", szFilepath, err)
	}
	logger.Info("Convert Sz Raw Snapshot End")

	// 排序
	list := SortSnapshotRaw(shList, szList)
	logger.Info("Convert All Raw Snapshot End")

	snapshotMap := GetMapSnapshot(list)

	// 写入
	//logger.Info("Write StockSnapshot.gz Begin")
	//if err := WriteSnapshotGz(dstDir, date, snapshotMap); err != nil {
	//	return errorx.NewError("WriteTrade(%s) date(%s) error: %v", dstDir, date, err)
	//}
	//logger.Info("Write StockSnapshot.gz End")

	logger.Info("Write StockSnapshot.parquet Begin")
	if err := WriteSnapshotParquet(dstDir, date, snapshotMap); err != nil {
		return errorx.NewError("WriteParquet(%s) date(%s) error: %v", dstDir, date, err)
	}
	logger.Info("Write StockSnapshot.parquet End")
	return nil
}

func SortSnapshotRaw(a []*model.Snapshot, b []*model.Snapshot) []*model.Snapshot {
	// 分别对 a 和 b 排序
	if config.Cfg.Sort {
		sort.SliceStable(a, func(i, j int) bool {
			return a[i].LocalTimestamp < a[j].LocalTimestamp
		})
		sort.SliceStable(b, func(i, j int) bool {
			return b[i].LocalTimestamp < b[j].LocalTimestamp
		})
	}

	// 双指针合并有序切片
	result := make([]*model.Snapshot, 0, len(a)+len(b))
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

func GetMapSnapshot(list []*model.Snapshot) map[string][]*model.Snapshot {
	res := make(map[string][]*model.Snapshot, 0)

	for _, v := range list {
		res[v.InstrumentId] = append(res[v.InstrumentId], v)
	}

	return res
}

func WriteSnapshotParquet(dstDir string, date string, mapSnapshot map[string][]*model.Snapshot) error {
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return errorx.NewError("MkdirAll(%s) error: %v", dstDir, err)
	}

	for instrumentId, list := range mapSnapshot {
		filePath := filepath.Join(dstDir, fmt.Sprintf("%s_snapshot_%s.parquet", date, instrumentId))

		//创建写入器
		pw, err := NewParquetWriter(filePath, new(model.Snapshot))
		if err != nil {
			return errorx.NewError("NewParquetWriter error: %s", err)
		}

		defer func() {
			if err := pw.Close(); err != nil {
				logger.Error("关闭Parquet写入器时出错: %v", err)
			}
		}()

		for _, v := range list {
			if v == nil {
				continue
			}

			if err := pw.Write(v); err != nil {
				logger.Error("WriteSnapshotParquet InstrumentId(%s) error: %v", instrumentId, err)
			}
		}
	}
	return nil
}

func WriteSnapshotGz(dstDir string, date string, mapSnapshot map[string][]*model.Snapshot) error {
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return errorx.NewError("MkdirAll(%s) error: %v", dstDir, err)
	}

	for instrumentId, list := range mapSnapshot {
		filePath := filepath.Join(dstDir, fmt.Sprintf("%s_snapshot_%s.csv.gz", date, instrumentId))

		file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			return errorx.NewError("open file(%s): %v", filePath, err)
		}
		defer file.Close()

		gzWriter := gzip.NewWriter(file)
		defer gzWriter.Close()

		if err := gocsv.Marshal(&list, gzWriter); err != nil {
			return errorx.NewError("filePath(%s) gocsv.Marshal error: %v", filePath, err)
		}
	}

	return nil
}
