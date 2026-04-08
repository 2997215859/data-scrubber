package service

import (
	"data-scrubber/biz/constdef"
	"data-scrubber/biz/errorx"
	"data-scrubber/biz/model"
	"data-scrubber/biz/utils"
	"data-scrubber/config"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	logger "github.com/2997215859/golog"
	"github.com/dromara/carbon/v2"
)

// ==== 沪市新格式转换（复用 ShRawTrade 结构体，Type=A/D）

func ShRawTrade2Order(date string, v *model.ShRawTrade) (*model.Order, error) {
	// 只处理委托记录（A=新增, D=撤单）
	if v.Type != "A" && v.Type != "D" {
		return nil, nil
	}

	orderTimestamp, err := utils.TimeToNano(date, v.TickTime)
	if err != nil {
		return nil, errorx.NewError("timeToNano(%s %s) error: %v", date, v.TickTime, err)
	}

	localTimestamp, err := utils.TimeToNano(date, v.LocalTime)
	if err != nil {
		return nil, errorx.NewError("timeToNano(%s %s) error: %v", date, v.LocalTime, err)
	}

	orderType := constdef.OrderTypeAdd
	if v.Type == "D" {
		orderType = constdef.OrderTypeCancel
	}

	direction := ShRaw2Direction(v.TickBSFlag)

	res := &model.Order{
		InstrumentId:   fmt.Sprintf("%s.SH", v.SecurityID),
		OrderTimestamp: orderTimestamp,
		OrderId:        v.BizIndex,
		OrderType:      orderType,
		Direction:      direction,
		Price:          v.Price,
		Volume:         v.Qty,
		SeqNo:          v.SeqNo,
		LocalTimestamp: localTimestamp,
	}

	return res, nil
}

func ShRawTrade2OrderList(date string, rawList []*model.ShRawTrade) ([]*model.Order, error) {
	var res []*model.Order
	for _, v := range rawList {
		order, err := ShRawTrade2Order(date, v)
		if err != nil {
			return nil, err
		}
		if order == nil {
			continue
		}
		res = append(res, order)
	}
	return res, nil
}

// ==== 沪市旧格式转换

func OldShRawOrder2Order(date string, v *model.OldShRawOrder) (*model.Order, error) {
	// 只处理委托记录（A=新增, D=撤单）
	if v.OrderType != "A" && v.OrderType != "D" {
		return nil, nil
	}

	orderTimestamp, err := utils.TimeToNano(date, v.OrderTime)
	if err != nil {
		return nil, errorx.NewError("timeToNano(%s %s) error: %v", date, v.OrderTime, err)
	}

	localTimestamp, err := utils.TimeToNano(date, v.LocalTime)
	if err != nil {
		return nil, errorx.NewError("timeToNano(%s %s) error: %v", date, v.LocalTime, err)
	}

	orderType := constdef.OrderTypeAdd
	if v.OrderType == "D" {
		orderType = constdef.OrderTypeCancel
	}

	direction := ShRaw2Direction(v.OrderBSFlag)

	res := &model.Order{
		InstrumentId:   fmt.Sprintf("%s.SH", v.SecurityID),
		OrderTimestamp: orderTimestamp,
		OrderId:        v.BizIndex,
		OrderType:      orderType,
		Direction:      direction,
		Price:          v.OrderPrice,
		Volume:         int64(v.Balance),
		SeqNo:          v.SeqNo,
		LocalTimestamp: localTimestamp,
	}

	return res, nil
}

func OldShRawOrder2OrderList(date string, rawList []*model.OldShRawOrder) ([]*model.Order, error) {
	var res []*model.Order
	for _, v := range rawList {
		order, err := OldShRawOrder2Order(date, v)
		if err != nil {
			return nil, err
		}
		if order == nil {
			continue
		}
		res = append(res, order)
	}
	return res, nil
}

// ==== 深市转换

func SzRaw2OrderDirection(side int) string {
	if side == 49 { // '1' = 买
		return constdef.DirectionBuy
	}
	if side == 50 { // '2' = 卖
		return constdef.DirectionSell
	}
	return constdef.DirectionUnknown
}

func SzRawOrder2Order(date string, v *model.SzRawOrder) (*model.Order, error) {
	orderTimestamp, err := utils.TimeToNano(date, v.TransactTime)
	if err != nil {
		return nil, errorx.NewError("timeToNano(%s %s) error: %v", date, v.TransactTime, err)
	}

	localTimestamp, err := utils.TimeToNano(date, v.LocalTime)
	if err != nil {
		return nil, errorx.NewError("timeToNano(%s %s) error: %v", date, v.LocalTime, err)
	}

	direction := SzRaw2OrderDirection(v.Side)

	res := &model.Order{
		InstrumentId:   fmt.Sprintf("%s.SZ", v.SecurityID),
		OrderTimestamp: orderTimestamp,
		OrderId:        v.ApplSeqNum,
		OrderType:      constdef.OrderTypeAdd,
		Direction:      direction,
		Price:          v.Price,
		Volume:         v.OrderQty,
		SeqNo:          v.SeqNo,
		LocalTimestamp: localTimestamp,
	}

	return res, nil
}

func SzRawOrder2OrderList(date string, rawList []*model.SzRawOrder) ([]*model.Order, error) {
	var res []*model.Order
	for _, v := range rawList {
		order, err := SzRawOrder2Order(date, v)
		if err != nil {
			return nil, err
		}
		if order == nil {
			continue
		}
		res = append(res, order)
	}
	return res, nil
}

// ==== 深市撤单转换（从逐笔成交 mdl_6_36_0 中提取 ExecType=52 的撤单记录）

// SzRawTrade2CancelOrder 将深市逐笔成交中的撤单记录（ExecType=52）转换为 Order
// 撤单记录中 BidApplSeqNum/OfferApplSeqNum 非零的一方指向原始委托，用于判断买卖方向
func SzRawTrade2CancelOrder(date string, v *model.SzRawTrade) (*model.Order, error) {
	// 只处理撤单记录，ExecType=52 即 ASCII '4'
	if v.ExecType != 52 {
		return nil, nil
	}

	orderTimestamp, err := utils.TimeToNano(date, v.TransactTime)
	if err != nil {
		return nil, errorx.NewError("timeToNano(%s %s) error: %v", date, v.TransactTime, err)
	}

	localTimestamp, err := utils.TimeToNano(date, v.LocalTime)
	if err != nil {
		return nil, errorx.NewError("timeToNano(%s %s) error: %v", date, v.LocalTime, err)
	}

	// 判断买卖方向：非零的一方为原始委托方向
	// BidApplSeqNum != 0 表示买方撤单，OfferApplSeqNum != 0 表示卖方撤单
	direction := constdef.DirectionUnknown
	orderId := v.ApplSeqNum
	if v.BidApplSeqNum != 0 && v.OfferApplSeqNum == 0 {
		direction = constdef.DirectionBuy
		orderId = v.BidApplSeqNum
	} else if v.OfferApplSeqNum != 0 && v.BidApplSeqNum == 0 {
		direction = constdef.DirectionSell
		orderId = v.OfferApplSeqNum
	}

	res := &model.Order{
		InstrumentId:   fmt.Sprintf("%s.SZ", v.SecurityID),
		OrderTimestamp: orderTimestamp,
		OrderId:        orderId,
		OrderType:      constdef.OrderTypeCancel,
		Direction:      direction,
		Price:          v.LastPx,
		Volume:         v.LastQty,
		SeqNo:          v.SeqNo,
		LocalTimestamp: localTimestamp,
	}

	return res, nil
}

func SzRawTrade2CancelOrderList(date string, rawList []*model.SzRawTrade) ([]*model.Order, error) {
	var res []*model.Order
	for _, v := range rawList {
		order, err := SzRawTrade2CancelOrder(date, v)
		if err != nil {
			return nil, err
		}
		if order == nil {
			continue
		}
		res = append(res, order)
	}
	return res, nil
}

// ==== 合并 order

func MergeRawOrder(srcDir string, dstDir string, date string) error {
	if config.Cfg.IsPerDay() {
		dstDir = filepath.Join(dstDir, constdef.DataTypeOrder)
	} else {
		dstDir = filepath.Join(dstDir, constdef.DataTypeOrder, date)
	}

	szFilepath := filepath.Join(srcDir, date, fmt.Sprintf("%s_mdl_6_33_0.csv.zip", date))

	// 读取和处理上海数据
	currentDate := carbon.Parse(date).StartOfDay()
	if currentDate.IsInvalid() {
		return errorx.NewError("date(%s) is invalid", date)
	}

	// 沪市逐笔委托数据从 20210607 起才有，此前跳过沪市部分
	var shOrderList []*model.Order
	if currentDate.Lt(shOrderStartDay) {
		logger.Info("Skip Sh Order: date(%s) < 20210607, no data available", date)
	} else if currentDate.Lt(shNewTradeStartDay) {
		// 旧格式：独立的逐笔委托文件 mdl_4_19_0
		shFilepath := filepath.Join(srcDir, date, fmt.Sprintf("%s_mdl_4_19_0.csv.zip", date))

		logger.Info("Read Old Sh Raw Order Begin")
		oldShRawOrderList, err := ManualReadOldShRawOrder(shFilepath)
		if err != nil {
			return errorx.NewError("ManualReadOldShRawOrder(%s) error: %s", shFilepath, err)
		}
		logger.Info("Read Old Sh Raw Order End")

		shOrderList, err = OldShRawOrder2OrderList(date, oldShRawOrderList)
		if err != nil {
			return errorx.NewError("OldShRawOrder2OrderList(%s) error: %s", shFilepath, err)
		}
		logger.Info("Convert Old Sh Raw Order End")
	} else {
		// 新格式：与逐笔成交共用文件 mdl_4_24_0，通过 Type 字段区分
		shFilepath := filepath.Join(srcDir, date, fmt.Sprintf("%s_mdl_4_24_0.csv.zip", date))

		logger.Info("Read Sh Raw Order Begin (from mdl_4_24_0)")
		shRawTradeList, err := ManualReadShRawTrade(shFilepath)
		if err != nil {
			return errorx.NewError("ManualReadShRawTrade(%s) error: %s", shFilepath, err)
		}
		logger.Info("Read Sh Raw Order End")

		shOrderList, err = ShRawTrade2OrderList(date, shRawTradeList)
		if err != nil {
			return errorx.NewError("ShRawTrade2OrderList(%s) error: %s", shFilepath, err)
		}
		logger.Info("Convert Sh Raw Order End")
	}

	// 读取和处理深圳委托数据（mdl_6_33_0，仅包含新增委托）
	logger.Info("Read Sz Raw Order Begin")
	szRawOrderList, err := ManualReadSzRawOrder(szFilepath)
	if err != nil {
		return errorx.NewError("ManualReadSzRawOrder(%s) error: %s", szFilepath, err)
	}
	logger.Info("Read Sz Raw Order End")

	szOrderList, err := SzRawOrder2OrderList(date, szRawOrderList)
	if err != nil {
		return errorx.NewError("SzRawOrder2OrderList(%s) error: %s", szFilepath, err)
	}
	logger.Info("Convert Sz Raw Order End, add count=%d", len(szOrderList))

	// 读取深圳逐笔成交数据（mdl_6_36_0），提取撤单记录（ExecType=52）
	szTradeFilepath := filepath.Join(srcDir, date, fmt.Sprintf("%s_mdl_6_36_0.csv.zip", date))
	logger.Info("Read Sz Raw Trade for Cancel Orders Begin")
	szRawTradeList, err := ManualReadSzRawTrade(szTradeFilepath)
	if err != nil {
		return errorx.NewError("ManualReadSzRawTrade(%s) error: %s", szTradeFilepath, err)
	}
	logger.Info("Read Sz Raw Trade End")

	szCancelOrderList, err := SzRawTrade2CancelOrderList(date, szRawTradeList)
	if err != nil {
		return errorx.NewError("SzRawTrade2CancelOrderList(%s) error: %s", szTradeFilepath, err)
	}
	logger.Info("Convert Sz Cancel Order End, cancel count=%d", len(szCancelOrderList))

	// 合并深市新增委托 + 撤单委托
	szOrderList = append(szOrderList, szCancelOrderList...)

	// 排序
	orderList := SortOrderRaw(shOrderList, szOrderList)
	logger.Info("Convert All Raw Order End")

	// 根据 output_mode 选择写入方式
	if config.Cfg.IsPerDay() {
		logger.Info("Write AllOrder.parquet Begin")
		if err := WriteAllOrderParquet(dstDir, date, orderList); err != nil {
			return errorx.NewError("WriteAllOrderParquet(%s) date(%s) error: %v", dstDir, date, err)
		}
		logger.Info("Write AllOrder.parquet End")
	} else {
		orderMap := GetMapOrder(orderList)

		logger.Info("Write StockOrder.parquet Begin")
		if err := WriteStockOrderParquet(dstDir, date, orderMap); err != nil {
			return errorx.NewError("WriteStockOrderParquet(%s) date(%s) error: %v", dstDir, date, err)
		}
		logger.Info("Write StockOrder.parquet End")
	}

	return nil
}

func SortOrderRaw(a []*model.Order, b []*model.Order) []*model.Order {
	if config.Cfg.Sort {
		sort.SliceStable(a, func(i, j int) bool {
			return a[i].LocalTimestamp < a[j].LocalTimestamp
		})
		sort.SliceStable(b, func(i, j int) bool {
			return b[i].LocalTimestamp < b[j].LocalTimestamp
		})
	}

	// 双指针合并有序切片
	result := make([]*model.Order, 0, len(a)+len(b))
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

	result = append(result, a[i:]...)
	result = append(result, b[j:]...)

	return result
}

func GetMapOrder(orderList []*model.Order) map[string][]*model.Order {
	res := make(map[string][]*model.Order)

	for _, v := range orderList {
		res[v.InstrumentId] = append(res[v.InstrumentId], v)
	}

	return res
}

func WriteAllOrderParquet(dstDir string, date string, orderList []*model.Order) error {
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return errorx.NewError("MkdirAll(%s) error: %v", dstDir, err)
	}

	filePath := filepath.Join(dstDir, fmt.Sprintf("%s_order.parquet", date))

	pw, err := NewParquetWriter(filePath, new(model.Order))
	if err != nil {
		return errorx.NewError("NewParquetWriter error: %s", err)
	}

	defer func() {
		if err := pw.Close(); err != nil {
			logger.Error("关闭Parquet写入器时出错: %v", err)
		}
	}()

	for _, order := range orderList {
		if order == nil {
			continue
		}

		if err := pw.Write(order); err != nil {
			logger.Error("WriteAllOrderParquet error: %v", err)
		}
	}
	return nil
}

func WriteStockOrderParquet(dstDir string, date string, mapOrder map[string][]*model.Order) error {
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return errorx.NewError("MkdirAll(%s) error: %v", dstDir, err)
	}

	for instrumentId, orderList := range mapOrder {
		filePath := filepath.Join(dstDir, fmt.Sprintf("%s_order_%s.parquet", date, instrumentId))

		pw, err := NewParquetWriter(filePath, new(model.Order))
		if err != nil {
			return errorx.NewError("NewParquetWriter error: %s", err)
		}

		defer func() {
			if err := pw.Close(); err != nil {
				logger.Error("关闭Parquet写入器时出错: %v", err)
			}
		}()

		for _, order := range orderList {
			if order == nil {
				continue
			}

			if err := pw.Write(order); err != nil {
				logger.Error("WriteStockParquet InstrumentId(%s) error: %v", instrumentId, err)
			}
		}
	}
	return nil
}
