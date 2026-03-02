package service

import (
	"data-scrubber/biz/constdef"
	"data-scrubber/biz/model"
	"data-scrubber/config"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang-module/carbon/v2"
)

func init() {
	carbon.SetDefault(carbon.Default{
		Layout:       carbon.RFC3339Layout,
		Timezone:     carbon.PRC,
		WeekStartsAt: carbon.Sunday,
		Locale:       "zh-CN",
	})
	// 初始化配置，避免 config.Cfg 为 nil
	config.Cfg = &config.Config{
		Sort: true,
	}
}

// 测试沪市旧格式（20220708, mdl_4_19_0）读取
func TestManualReadOldShRawOrder(t *testing.T) {
	fp := filepath.Join("..", "..", "testdata", "mock_src", "20220708", "20220708_mdl_4_19_0.csv.zip")
	orders, err := ManualReadOldShRawOrder(fp)
	if err != nil {
		t.Fatalf("ManualReadOldShRawOrder error: %v", err)
	}
	if len(orders) == 0 {
		t.Fatal("读取到0条记录")
	}
	t.Logf("读取到 %d 条沪市旧格式委托记录", len(orders))

	// 验证第一条记录
	first := orders[0]
	if first.SecurityID == "" {
		t.Error("SecurityID 为空")
	}
	if first.OrderType != "A" && first.OrderType != "D" {
		// 旧格式文件中可能还有其他类型，但 mock 数据中应该有 A 和 D
		t.Logf("第一条记录 OrderType=%s", first.OrderType)
	}
	t.Logf("第一条: SecurityID=%s, OrderTime=%s, OrderType=%s, OrderBSFlag=%s, Balance=%.0f, BizIndex=%d",
		first.SecurityID, first.OrderTime, first.OrderType, first.OrderBSFlag, first.Balance, first.BizIndex)
}

// 测试沪市旧格式转换为统一 Order
func TestOldShRawOrder2OrderList(t *testing.T) {
	fp := filepath.Join("..", "..", "testdata", "mock_src", "20220708", "20220708_mdl_4_19_0.csv.zip")
	rawOrders, err := ManualReadOldShRawOrder(fp)
	if err != nil {
		t.Fatalf("ManualReadOldShRawOrder error: %v", err)
	}

	orderList, err := OldShRawOrder2OrderList("20220708", rawOrders)
	if err != nil {
		t.Fatalf("OldShRawOrder2OrderList error: %v", err)
	}
	if len(orderList) == 0 {
		t.Fatal("转换后0条记录")
	}
	t.Logf("转换后 %d 条委托记录", len(orderList))

	// 验证字段
	for i, o := range orderList {
		if o.InstrumentId == "" {
			t.Errorf("第 %d 条 InstrumentId 为空", i)
		}
		if o.OrderType != constdef.OrderTypeAdd && o.OrderType != constdef.OrderTypeCancel {
			t.Errorf("第 %d 条 OrderType=%s 不合法", i, o.OrderType)
		}
		if o.OrderTimestamp <= 0 {
			t.Errorf("第 %d 条 OrderTimestamp=%d 不合法", i, o.OrderTimestamp)
		}
		if i < 3 {
			t.Logf("  [%d] %s OrderType=%s Direction=%s Price=%.3f Volume=%d",
				i, o.InstrumentId, o.OrderType, o.Direction, o.Price, o.Volume)
		}
	}
}

// 测试深市（mdl_6_33_0）读取
func TestManualReadSzRawOrder(t *testing.T) {
	fp := filepath.Join("..", "..", "testdata", "mock_src", "20220708", "20220708_mdl_6_33_0.csv.zip")
	orders, err := ManualReadSzRawOrder(fp)
	if err != nil {
		t.Fatalf("ManualReadSzRawOrder error: %v", err)
	}
	if len(orders) == 0 {
		t.Fatal("读取到0条记录")
	}
	t.Logf("读取到 %d 条深市委托记录", len(orders))

	first := orders[0]
	if first.SecurityID == "" {
		t.Error("SecurityID 为空")
	}
	// Side 应该是 49(买) 或 50(卖)
	if first.Side != 49 && first.Side != 50 {
		t.Errorf("第一条 Side=%d 不合法", first.Side)
	}
	t.Logf("第一条: SecurityID=%s, TransactTime=%s, Side=%d, Price=%.4f, OrderQty=%d",
		first.SecurityID, first.TransactTime, first.Side, first.Price, first.OrderQty)
}

// 测试深市转换为统一 Order
func TestSzRawOrder2OrderList(t *testing.T) {
	fp := filepath.Join("..", "..", "testdata", "mock_src", "20220708", "20220708_mdl_6_33_0.csv.zip")
	rawOrders, err := ManualReadSzRawOrder(fp)
	if err != nil {
		t.Fatalf("ManualReadSzRawOrder error: %v", err)
	}

	orderList, err := SzRawOrder2OrderList("20220708", rawOrders)
	if err != nil {
		t.Fatalf("SzRawOrder2OrderList error: %v", err)
	}
	if len(orderList) == 0 {
		t.Fatal("转换后0条记录")
	}
	t.Logf("转换后 %d 条委托记录", len(orderList))

	for i, o := range orderList {
		if o.InstrumentId == "" {
			t.Errorf("第 %d 条 InstrumentId 为空", i)
		}
		if o.Direction != constdef.DirectionBuy && o.Direction != constdef.DirectionSell && o.Direction != constdef.DirectionUnknown {
			t.Errorf("第 %d 条 Direction=%s 不合法", i, o.Direction)
		}
		if i < 3 {
			t.Logf("  [%d] %s Direction=%s Price=%.4f Volume=%d",
				i, o.InstrumentId, o.Direction, o.Price, o.Volume)
		}
	}
}

// 测试沪市新格式（20260213, mdl_4_24_0）读取并转换为 Order
func TestShRawTrade2OrderList(t *testing.T) {
	fp := filepath.Join("..", "..", "testdata", "mock_src", "20260213", "20260213_mdl_4_24_0.csv.zip")
	rawList, err := ManualReadShRawTrade(fp)
	if err != nil {
		t.Fatalf("ManualReadShRawTrade error: %v", err)
	}
	t.Logf("读取到 %d 条沪市新格式记录（含成交+委托）", len(rawList))

	// 统计各 Type 数量
	typeCounts := make(map[string]int)
	for _, v := range rawList {
		typeCounts[v.Type]++
	}
	t.Logf("Type 分布: %v", typeCounts)

	orderList, err := ShRawTrade2OrderList("20260213", rawList)
	if err != nil {
		t.Fatalf("ShRawTrade2OrderList error: %v", err)
	}
	if len(orderList) == 0 {
		t.Fatal("转换后0条委托记录")
	}
	t.Logf("过滤后 %d 条委托记录（A+D）", len(orderList))

	// 验证所有记录的 OrderType 只有 add 或 cancel
	addCount, cancelCount := 0, 0
	for _, o := range orderList {
		switch o.OrderType {
		case constdef.OrderTypeAdd:
			addCount++
		case constdef.OrderTypeCancel:
			cancelCount++
		default:
			t.Errorf("非法 OrderType=%s", o.OrderType)
		}
	}
	t.Logf("add=%d, cancel=%d", addCount, cancelCount)

	if addCount+cancelCount != len(orderList) {
		t.Error("add + cancel 数量不等于总数")
	}
}

// 测试排序合并
func TestSortOrderRaw(t *testing.T) {
	a := []*model.Order{
		{InstrumentId: "600001.SH", LocalTimestamp: 100},
		{InstrumentId: "600001.SH", LocalTimestamp: 300},
	}
	b := []*model.Order{
		{InstrumentId: "000001.SZ", LocalTimestamp: 200},
		{InstrumentId: "000001.SZ", LocalTimestamp: 400},
	}
	result := SortOrderRaw(a, b)
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
func TestGetMapOrder(t *testing.T) {
	orders := []*model.Order{
		{InstrumentId: "600001.SH"},
		{InstrumentId: "000001.SZ"},
		{InstrumentId: "600001.SH"},
	}
	m := GetMapOrder(orders)
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

// 测试端到端写 Parquet（20220708 旧格式）
func TestMergeRawOrder_OldFormat(t *testing.T) {
	srcDir := filepath.Join("..", "..", "testdata", "mock_src")
	dstDir := filepath.Join("..", "..", "testdata", "mock_dst")

	// 清理输出目录
	outDir := filepath.Join(dstDir, constdef.DataTypeOrder, "20220708")
	os.RemoveAll(outDir)

	err := MergeRawOrder(srcDir, dstDir, "20220708")
	if err != nil {
		t.Fatalf("MergeRawOrder error: %v", err)
	}

	// 检查输出目录是否有 parquet 文件
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

// 测试端到端写 Parquet（20260213 新格式）
func TestMergeRawOrder_NewFormat(t *testing.T) {
	srcDir := filepath.Join("..", "..", "testdata", "mock_src")
	dstDir := filepath.Join("..", "..", "testdata", "mock_dst")

	// 清理输出目录
	outDir := filepath.Join(dstDir, constdef.DataTypeOrder, "20260213")
	os.RemoveAll(outDir)

	err := MergeRawOrder(srcDir, dstDir, "20260213")
	if err != nil {
		t.Fatalf("MergeRawOrder error: %v", err)
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
