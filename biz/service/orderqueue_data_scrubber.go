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
)

// RawOrderQueue2OrderQueue 将原始委托队列转换为统一格式
// suffix: "SH" 或 "SZ"
func RawOrderQueue2OrderQueue(date string, v *model.RawOrderQueue, suffix string) (*model.OrderQueue, error) {
	timestamp, err := utils.TimeToNano(date, v.Timestamp)
	if err != nil {
		return nil, errorx.NewError("timeToNano(%s %s) error: %v", date, v.Timestamp, err)
	}

	localTimestamp, err := utils.TimeToNano(date, v.LocalTime)
	if err != nil {
		return nil, errorx.NewError("timeToNano(%s %s) error: %v", date, v.LocalTime, err)
	}

	// Direction: "B" → "buy", "S" → "sell"
	direction := constdef.DirectionUnknown
	if v.Side == "B" {
		direction = constdef.DirectionBuy
	} else if v.Side == "S" {
		direction = constdef.DirectionSell
	}

	// OrderQtyList: float64 转 []int64，取 NoOrders 个有效值
	orderQtyList := make([]int64, 0, len(v.OrderQtyList))
	for _, qty := range v.OrderQtyList {
		orderQtyList = append(orderQtyList, int64(qty))
	}

	res := &model.OrderQueue{
		InstrumentId:   fmt.Sprintf("%s.%s", v.SecurityID, suffix),
		UpdateTimestamp: timestamp,
		Direction:      direction,
		Price:          v.Price,
		Volume:         int64(v.Volume),
		NumOrders:      int64(v.NumOrders),
		OrderQtyList:   orderQtyList,
		LocalTimestamp: localTimestamp,
	}

	return res, nil
}

func RawOrderQueue2OrderQueueList(date string, rawList []*model.RawOrderQueue, suffix string) ([]*model.OrderQueue, error) {
	var res []*model.OrderQueue
	for _, v := range rawList {
		oq, err := RawOrderQueue2OrderQueue(date, v, suffix)
		if err != nil {
			return nil, err
		}
		if oq == nil {
			continue
		}
		res = append(res, oq)
	}
	return res, nil
}

// SortOrderQueueRaw 归并多个有序列表，按 LocalTimestamp 排序
func SortOrderQueueRaw(a []*model.OrderQueue, b []*model.OrderQueue) []*model.OrderQueue {
	if config.Cfg.Sort {
		sort.SliceStable(a, func(i, j int) bool {
			return a[i].LocalTimestamp < a[j].LocalTimestamp
		})
		sort.SliceStable(b, func(i, j int) bool {
			return b[i].LocalTimestamp < b[j].LocalTimestamp
		})
	}

	// 双指针合并有序切片
	result := make([]*model.OrderQueue, 0, len(a)+len(b))
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

func GetMapOrderQueue(list []*model.OrderQueue) map[string][]*model.OrderQueue {
	res := make(map[string][]*model.OrderQueue)
	for _, v := range list {
		res[v.InstrumentId] = append(res[v.InstrumentId], v)
	}
	return res
}

func WriteStockOrderQueueParquet(dstDir string, date string, mapOQ map[string][]*model.OrderQueue) error {
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return errorx.NewError("MkdirAll(%s) error: %v", dstDir, err)
	}

	for instrumentId, oqList := range mapOQ {
		filePath := filepath.Join(dstDir, fmt.Sprintf("%s_orderqueue_%s.parquet", date, instrumentId))

		pw, err := NewParquetWriter(filePath, new(model.OrderQueue))
		if err != nil {
			return errorx.NewError("NewParquetWriter error: %s", err)
		}

		defer func() {
			if err := pw.Close(); err != nil {
				logger.Error("关闭Parquet写入器时出错: %v", err)
			}
		}()

		for _, oq := range oqList {
			if oq == nil {
				continue
			}

			if err := pw.Write(oq); err != nil {
				logger.Error("WriteStockParquet InstrumentId(%s) error: %v", instrumentId, err)
			}
		}
	}
	return nil
}

// MergeRawOrderQueue 委托队列清洗主入口
func MergeRawOrderQueue(srcDir string, dstDir string, date string) error {
	dstDir = filepath.Join(dstDir, constdef.DataTypeOrderQueue, date)

	// 沪市：OrderQueue.csv.zip
	shFilepath := filepath.Join(srcDir, date, fmt.Sprintf("%s_OrderQueue.csv.zip", date))

	logger.Info("Read Sh OrderQueue Begin")
	shRawList, err := ManualReadOrderQueue(shFilepath, "SH")
	if err != nil {
		return errorx.NewError("ManualReadOrderQueue(%s) error: %s", shFilepath, err)
	}
	logger.Info("Read Sh OrderQueue End, count=%d", len(shRawList))

	shList, err := RawOrderQueue2OrderQueueList(date, shRawList, "SH")
	if err != nil {
		return errorx.NewError("RawOrderQueue2OrderQueueList(SH) error: %s", err)
	}
	logger.Info("Convert Sh OrderQueue End, count=%d", len(shList))

	// 深市卖：mdl_6_28_1.csv.zip
	szSellFilepath := filepath.Join(srcDir, date, fmt.Sprintf("%s_mdl_6_28_1.csv.zip", date))

	logger.Info("Read Sz Sell OrderQueue Begin")
	szSellRawList, err := ManualReadOrderQueue(szSellFilepath, "SZ")
	if err != nil {
		return errorx.NewError("ManualReadOrderQueue(%s) error: %s", szSellFilepath, err)
	}
	logger.Info("Read Sz Sell OrderQueue End, count=%d", len(szSellRawList))

	szSellList, err := RawOrderQueue2OrderQueueList(date, szSellRawList, "SZ")
	if err != nil {
		return errorx.NewError("RawOrderQueue2OrderQueueList(SZ sell) error: %s", err)
	}
	logger.Info("Convert Sz Sell OrderQueue End, count=%d", len(szSellList))

	// 深市买：mdl_6_28_2.csv.zip
	szBuyFilepath := filepath.Join(srcDir, date, fmt.Sprintf("%s_mdl_6_28_2.csv.zip", date))

	logger.Info("Read Sz Buy OrderQueue Begin")
	szBuyRawList, err := ManualReadOrderQueue(szBuyFilepath, "SZ")
	if err != nil {
		return errorx.NewError("ManualReadOrderQueue(%s) error: %s", szBuyFilepath, err)
	}
	logger.Info("Read Sz Buy OrderQueue End, count=%d", len(szBuyRawList))

	szBuyList, err := RawOrderQueue2OrderQueueList(date, szBuyRawList, "SZ")
	if err != nil {
		return errorx.NewError("RawOrderQueue2OrderQueueList(SZ buy) error: %s", err)
	}
	logger.Info("Convert Sz Buy OrderQueue End, count=%d", len(szBuyList))

	// 合并 SZ 买+卖 为一个列表
	szList := append(szSellList, szBuyList...)

	// 归并排序 SH + SZ
	oqList := SortOrderQueueRaw(shList, szList)
	logger.Info("Sort OrderQueue End, count=%d", len(oqList))

	// 按 InstrumentId 分组
	oqMap := GetMapOrderQueue(oqList)

	// 写入 Parquet
	logger.Info("Write StockOrderQueue.parquet Begin")
	if err := WriteStockOrderQueueParquet(dstDir, date, oqMap); err != nil {
		return errorx.NewError("WriteStockOrderQueueParquet(%s) date(%s) error: %v", dstDir, date, err)
	}
	logger.Info("Write StockOrderQueue.parquet End")

	return nil
}
