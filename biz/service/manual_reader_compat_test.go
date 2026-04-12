package service

import (
	"archive/zip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func writeTestZipCSV(t *testing.T, name string, content string) string {
	t.Helper()

	fp := filepath.Join(t.TempDir(), name+".zip")
	file, err := os.Create(fp)
	if err != nil {
		t.Fatalf("create zip file: %v", err)
	}

	zipWriter := zip.NewWriter(file)
	writer, err := zipWriter.Create(name)
	if err != nil {
		t.Fatalf("create csv in zip: %v", err)
	}
	if _, err := writer.Write([]byte(content)); err != nil {
		t.Fatalf("write csv content: %v", err)
	}
	if err := zipWriter.Close(); err != nil {
		t.Fatalf("close zip writer: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close zip file: %v", err)
	}

	return fp
}

func TestManualReadSzRawOrder_AllowsMissingLocalTimeAndSeqNo(t *testing.T) {
	content := strings.Join([]string{
		"ChannelNo,ApplSeqNum,MDStreamID,SecurityID,SecurityIDSource,Price,OrderQty,Side,TransactTime,OrdType,LocalTime,SeqNo",
		"2012,1,011,002813,102 ,35.0100,62600,49,09:15:00.000,50,,",
	}, "\n") + "\n"

	orders, err := ManualReadSzRawOrder(writeTestZipCSV(t, "order.csv", content))
	if err != nil {
		t.Fatalf("ManualReadSzRawOrder error: %v", err)
	}
	if len(orders) != 1 {
		t.Fatalf("records=%d, want 1", len(orders))
	}
	if orders[0].LocalTime != "" {
		t.Fatalf("LocalTime=%q, want empty", orders[0].LocalTime)
	}
	if orders[0].SeqNo != 0 {
		t.Fatalf("SeqNo=%d, want 0", orders[0].SeqNo)
	}

	converted, err := SzRawOrder2Order("20170330", orders[0])
	if err != nil {
		t.Fatalf("SzRawOrder2Order error: %v", err)
	}
	if converted.LocalTimestamp != 0 {
		t.Fatalf("LocalTimestamp=%d, want 0", converted.LocalTimestamp)
	}
}

func TestManualReadOldShRawOrder_AllowsMissingLocalTimeAndSeqNo(t *testing.T) {
	content := strings.Join([]string{
		"DataStatus,OrderIndex,OrderChannel,SecurityID,OrderTime,OrderType,OrderNO,OrderPrice,Balance,OrderBSFlag,BizIndex,LocalTime,SeqNo",
		"0,1,3,603758,09:15:00.260,A,979,10.180,1800.000,S,1,,",
	}, "\n") + "\n"

	orders, err := ManualReadOldShRawOrder(writeTestZipCSV(t, "old_sh_order.csv", content))
	if err != nil {
		t.Fatalf("ManualReadOldShRawOrder error: %v", err)
	}
	if len(orders) != 1 {
		t.Fatalf("records=%d, want 1", len(orders))
	}
	if orders[0].LocalTime != "" {
		t.Fatalf("LocalTime=%q, want empty", orders[0].LocalTime)
	}
	if orders[0].SeqNo != 0 {
		t.Fatalf("SeqNo=%d, want 0", orders[0].SeqNo)
	}

	converted, err := OldShRawOrder2Order("20170330", orders[0])
	if err != nil {
		t.Fatalf("OldShRawOrder2Order error: %v", err)
	}
	if converted.LocalTimestamp != 0 {
		t.Fatalf("LocalTimestamp=%d, want 0", converted.LocalTimestamp)
	}
}

func TestManualReadShRawTrade_AllowsMissingLocalTimeAndSeqNo(t *testing.T) {
	content := strings.Join([]string{
		"BizIndex,Channel,SecurityID,TickTime,Type,BuyOrderNO,SellOrderNO,Price,Qty,TradeMoney,TickBSFlag,LocalTime,SeqNo",
		"249552,1,603518,09:30:00.000,A,120,0,11.300,737300,0.000,B,,",
	}, "\n") + "\n"

	trades, err := ManualReadShRawTrade(writeTestZipCSV(t, "sh_trade.csv", content))
	if err != nil {
		t.Fatalf("ManualReadShRawTrade error: %v", err)
	}
	if len(trades) != 1 {
		t.Fatalf("records=%d, want 1", len(trades))
	}
	if trades[0].LocalTime != "" {
		t.Fatalf("LocalTime=%q, want empty", trades[0].LocalTime)
	}
	if trades[0].SeqNo != 0 {
		t.Fatalf("SeqNo=%d, want 0", trades[0].SeqNo)
	}

	converted, err := ShRawTrade2Order("20260213", trades[0])
	if err != nil {
		t.Fatalf("ShRawTrade2Order error: %v", err)
	}
	if converted.LocalTimestamp != 0 {
		t.Fatalf("LocalTimestamp=%d, want 0", converted.LocalTimestamp)
	}
}

func TestManualReadSzRawTrade_AllowsMissingLocalTimeAndSeqNo(t *testing.T) {
	content := strings.Join([]string{
		"ChannelNo,ApplSeqNum,MDStreamID,BidApplSeqNum,OfferApplSeqNum,SecurityID,SecurityIDSource,LastPx,LastQty,ExecType,TransactTime,LocalTime,SeqNo",
		"2022,22166,011,22164,17576,159915,102 ,1.7030,58700,52,09:30:00.000,,",
	}, "\n") + "\n"

	trades, err := ManualReadSzRawTrade(writeTestZipCSV(t, "trade.csv", content))
	if err != nil {
		t.Fatalf("ManualReadSzRawTrade error: %v", err)
	}
	if len(trades) != 1 {
		t.Fatalf("records=%d, want 1", len(trades))
	}
	if trades[0].LocalTime != "" {
		t.Fatalf("LocalTime=%q, want empty", trades[0].LocalTime)
	}
	if trades[0].SeqNo != 0 {
		t.Fatalf("SeqNo=%d, want 0", trades[0].SeqNo)
	}

	converted, err := SzRawTrade2CancelOrder("20170330", trades[0])
	if err != nil {
		t.Fatalf("SzRawTrade2CancelOrder error: %v", err)
	}
	if converted.LocalTimestamp != 0 {
		t.Fatalf("LocalTimestamp=%d, want 0", converted.LocalTimestamp)
	}
}

func TestManualReadOrderQueue_AllowsMissingLocalTimeAndSeqNo(t *testing.T) {
	headers := []string{
		"DataTimeStamp", "SecurityID", "ImageStatus", "Side", "NoPriceLevel",
		"PrcLvlOperator", "Price", "Volume", "NumOrders", "NoOrders",
	}
	row := []string{
		"09:15:00.000", "000001", "1", "B", "1",
		"0", "10.0000", "1000", "1", "50",
	}
	for i := 1; i <= 50; i++ {
		headers = append(headers, "OrderQty"+strconv.Itoa(i))
		row = append(row, strconv.Itoa(i*100))
	}
	headers = append(headers, "LocalTime", "SeqNo")
	row = append(row, "", "")

	content := strings.Join([]string{
		strings.Join(headers, ","),
		strings.Join(row, ","),
	}, "\n") + "\n"

	records, err := ManualReadOrderQueue(writeTestZipCSV(t, "orderqueue.csv", content), "SZ")
	if err != nil {
		t.Fatalf("ManualReadOrderQueue error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("records=%d, want 1", len(records))
	}
	if records[0].LocalTime != "" {
		t.Fatalf("LocalTime=%q, want empty", records[0].LocalTime)
	}
	if records[0].SeqNo != 0 {
		t.Fatalf("SeqNo=%d, want 0", records[0].SeqNo)
	}

	converted, err := RawOrderQueue2OrderQueue("20170330", records[0], "SZ")
	if err != nil {
		t.Fatalf("RawOrderQueue2OrderQueue error: %v", err)
	}
	if converted.LocalTimestamp != 0 {
		t.Fatalf("LocalTimestamp=%d, want 0", converted.LocalTimestamp)
	}
	if len(converted.OrderQtyList) != 50 {
		t.Fatalf("OrderQtyList len=%d, want 50", len(converted.OrderQtyList))
	}

	headers[0] = "UpdateTime"
	records, err = ManualReadOrderQueue(writeTestZipCSV(t, "sh_orderqueue.csv", content), "SH")
	if err == nil {
		t.Fatal("ManualReadOrderQueue(SH) should reject mismatched time header")
	}

	content = strings.Join([]string{
		strings.Join(headers, ","),
		strings.Join(row, ","),
	}, "\n") + "\n"
	records, err = ManualReadOrderQueue(writeTestZipCSV(t, "sh_orderqueue.csv", content), "SH")
	if err != nil {
		t.Fatalf("ManualReadOrderQueue(SH) error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("SH records=%d, want 1", len(records))
	}
	converted, err = RawOrderQueue2OrderQueue("20170330", records[0], "SH")
	if err != nil {
		t.Fatalf("RawOrderQueue2OrderQueue(SH) error: %v", err)
	}
	if converted.LocalTimestamp != 0 {
		t.Fatalf("SH LocalTimestamp=%d, want 0", converted.LocalTimestamp)
	}
}
