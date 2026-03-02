package service

import (
	"data-scrubber/biz/constdef"
	"data-scrubber/biz/model"
	"os"
	"path/filepath"
	"testing"
)

// 测试沪市 OrderQueue 读取
func TestManualReadOrderQueue_SH(t *testing.T) {
	fp := filepath.Join("..", "..", "testdata", "mock_src", "20220708", "20220708_OrderQueue.csv.zip")
	records, err := ManualReadOrderQueue(fp, "SH")
	if err != nil {
		t.Fatalf("ManualReadOrderQueue(SH) error: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("读取到0条记录")
	}
	t.Logf("读取到 %d 条沪市委托队列记录", len(records))

	first := records[0]
	if first.SecurityID == "" {
		t.Error("SecurityID 为空")
	}
	if first.Side != "B" && first.Side != "S" {
		t.Errorf("第一条 Side=%s 不合法", first.Side)
	}
	t.Logf("第一条: SecurityID=%s, Timestamp=%s, Side=%s, Price=%.3f, Volume=%.0f, NoOrders=%d, OrderQtyList=%v",
		first.SecurityID, first.Timestamp, first.Side, first.Price, first.Volume, first.NoOrders, first.OrderQtyList)
}

// 测试深市卖方 OrderQueue 读取
func TestManualReadOrderQueue_SZ(t *testing.T) {
	fp := filepath.Join("..", "..", "testdata", "mock_src", "20220708", "20220708_mdl_6_28_1.csv.zip")
	records, err := ManualReadOrderQueue(fp, "SZ")
	if err != nil {
		t.Fatalf("ManualReadOrderQueue(SZ) error: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("读取到0条记录")
	}
	t.Logf("读取到 %d 条深市卖方委托队列记录", len(records))

	first := records[0]
	if first.SecurityID == "" {
		t.Error("SecurityID 为空")
	}
	if first.Side != "S" {
		t.Errorf("深市卖方文件第一条 Side=%s, 期望 S", first.Side)
	}
	t.Logf("第一条: SecurityID=%s, Timestamp=%s, Side=%s, Price=%.4f, Volume=%.0f, NoOrders=%d, OrderQtyList=%v",
		first.SecurityID, first.Timestamp, first.Side, first.Price, first.Volume, first.NoOrders, first.OrderQtyList)
}

// 测试沪市转换
func TestRawOrderQueue2OrderQueueList_SH(t *testing.T) {
	fp := filepath.Join("..", "..", "testdata", "mock_src", "20220708", "20220708_OrderQueue.csv.zip")
	rawList, err := ManualReadOrderQueue(fp, "SH")
	if err != nil {
		t.Fatalf("ManualReadOrderQueue error: %v", err)
	}

	oqList, err := RawOrderQueue2OrderQueueList("20220708", rawList, "SH")
	if err != nil {
		t.Fatalf("RawOrderQueue2OrderQueueList error: %v", err)
	}
	if len(oqList) == 0 {
		t.Fatal("转换后0条记录")
	}
	t.Logf("转换后 %d 条记录", len(oqList))

	for i, oq := range oqList {
		if oq.InstrumentId == "" {
			t.Errorf("第 %d 条 InstrumentId 为空", i)
		}
		if oq.Direction != constdef.DirectionBuy && oq.Direction != constdef.DirectionSell {
			t.Errorf("第 %d 条 Direction=%s 不合法", i, oq.Direction)
		}
		if oq.UpdateTimestamp <= 0 {
			t.Errorf("第 %d 条 UpdateTimestamp=%d 不合法", i, oq.UpdateTimestamp)
		}
		if i < 3 {
			t.Logf("  [%d] %s Direction=%s Price=%.3f Volume=%d NumOrders=%d OrderQtyList=%v",
				i, oq.InstrumentId, oq.Direction, oq.Price, oq.Volume, oq.NumOrders, oq.OrderQtyList)
		}
	}
}

// 测试深市转换
func TestRawOrderQueue2OrderQueueList_SZ(t *testing.T) {
	fp := filepath.Join("..", "..", "testdata", "mock_src", "20220708", "20220708_mdl_6_28_1.csv.zip")
	rawList, err := ManualReadOrderQueue(fp, "SZ")
	if err != nil {
		t.Fatalf("ManualReadOrderQueue error: %v", err)
	}

	oqList, err := RawOrderQueue2OrderQueueList("20220708", rawList, "SZ")
	if err != nil {
		t.Fatalf("RawOrderQueue2OrderQueueList error: %v", err)
	}
	if len(oqList) == 0 {
		t.Fatal("转换后0条记录")
	}
	t.Logf("转换后 %d 条记录", len(oqList))

	for i, oq := range oqList {
		if oq.Direction != constdef.DirectionSell {
			t.Errorf("第 %d 条 Direction=%s, 期望 sell", i, oq.Direction)
		}
		if i < 3 {
			t.Logf("  [%d] %s Direction=%s Price=%.4f Volume=%d NumOrders=%d OrderQtyList=%v",
				i, oq.InstrumentId, oq.Direction, oq.Price, oq.Volume, oq.NumOrders, oq.OrderQtyList)
		}
	}
}

// 测试归并排序
func TestSortOrderQueueRaw(t *testing.T) {
	a := []*model.OrderQueue{
		{InstrumentId: "600001.SH", LocalTimestamp: 100},
		{InstrumentId: "600001.SH", LocalTimestamp: 300},
	}
	b := []*model.OrderQueue{
		{InstrumentId: "000001.SZ", LocalTimestamp: 200},
		{InstrumentId: "000001.SZ", LocalTimestamp: 400},
	}
	result := SortOrderQueueRaw(a, b)
	if len(result) != 4 {
		t.Fatalf("合并后数量=%d, 期望4", len(result))
	}
	for i := 1; i < len(result); i++ {
		if result[i].LocalTimestamp < result[i-1].LocalTimestamp {
			t.Errorf("排序错误: [%d].LocalTimestamp=%d < [%d].LocalTimestamp=%d",
				i, result[i].LocalTimestamp, i-1, result[i-1].LocalTimestamp)
		}
	}
}

// 测试按股票分组
func TestGetMapOrderQueue(t *testing.T) {
	oqs := []*model.OrderQueue{
		{InstrumentId: "600001.SH"},
		{InstrumentId: "000001.SZ"},
		{InstrumentId: "600001.SH"},
	}
	m := GetMapOrderQueue(oqs)
	if len(m) != 2 {
		t.Errorf("分组数=%d, 期望2", len(m))
	}
	if len(m["600001.SH"]) != 2 {
		t.Errorf("600001.SH 数量=%d, 期望2", len(m["600001.SH"]))
	}
	if len(m["000001.SZ"]) != 1 {
		t.Errorf("000001.SZ 数量=%d, 期望1", len(m["000001.SZ"]))
	}
}

// 端到端测试 20220708
func TestMergeRawOrderQueue_20220708(t *testing.T) {
	srcDir := filepath.Join("..", "..", "testdata", "mock_src")
	dstDir := filepath.Join("..", "..", "testdata", "mock_dst")

	outDir := filepath.Join(dstDir, constdef.DataTypeOrderQueue, "20220708")
	os.RemoveAll(outDir)

	err := MergeRawOrderQueue(srcDir, dstDir, "20220708")
	if err != nil {
		t.Fatalf("MergeRawOrderQueue error: %v", err)
	}

	entries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatalf("读取输出目录失败: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("输出目录为空，未生成 parquet 文件")
	}
	t.Logf("生成了 %d 个 parquet 文件", len(entries))
	for i, e := range entries {
		if i < 5 {
			t.Logf("  %s", e.Name())
		}
	}
}

// 端到端测试 20260213
func TestMergeRawOrderQueue_20260213(t *testing.T) {
	srcDir := filepath.Join("..", "..", "testdata", "mock_src")
	dstDir := filepath.Join("..", "..", "testdata", "mock_dst")

	outDir := filepath.Join(dstDir, constdef.DataTypeOrderQueue, "20260213")
	os.RemoveAll(outDir)

	err := MergeRawOrderQueue(srcDir, dstDir, "20260213")
	if err != nil {
		t.Fatalf("MergeRawOrderQueue error: %v", err)
	}

	entries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatalf("读取输出目录失败: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("输出目录为空，未生成 parquet 文件")
	}
	t.Logf("生成了 %d 个 parquet 文件", len(entries))
	for i, e := range entries {
		if i < 5 {
			t.Logf("  %s", e.Name())
		}
	}
}
