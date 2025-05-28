package service

import (
	"data-scrubber/biz/errorx"
	"data-scrubber/biz/model"
	"fmt"
	logger "github.com/2997215859/golog"
	"os"
	"path/filepath"
	"sort"
)

// ==== sh 处理

// todo
func ManualReadShRawSnapshot(filepath string) ([]*model.ShRawSnapshot, error) {
	return nil, nil
}

func ShRawSnapshot2Snapshot(date string, v *model.ShRawSnapshot) (*model.Snapshot, error) {
	return nil, nil
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

// todo
func ManualReadSzRawSnapshot(filepath string) ([]*model.SzRawSnapshot, error) {
	return nil, nil
}

func SzRawSnapshot2Snapshot(date string, v *model.SzRawSnapshot) (*model.Snapshot, error) {
	return nil, nil
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

	// 写入

	snapshotMap := GetMapSnapshot(list)
	logger.Info("Write StockSnapshot.parquet Begin")
	if err := WriteSnapshotParquet(dstDir, date, snapshotMap); err != nil {
		return errorx.NewError("WriteParquet(%s) date(%s) error: %v", dstDir, date, err)
	}
	logger.Info("Write StockSnapshot.parquet End")
	return nil
}

func SortSnapshotRaw(a []*model.Snapshot, b []*model.Snapshot) []*model.Snapshot {
	// 分别对 a 和 b 排序
	sort.Slice(a, func(i, j int) bool {
		return a[i].LocalTimestamp < a[j].LocalTimestamp
	})
	sort.Slice(b, func(i, j int) bool {
		return b[i].LocalTimestamp < b[j].LocalTimestamp
	})

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
	dstDir = filepath.Join(dstDir, date)
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
