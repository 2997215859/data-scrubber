package model

// ==== 合并数据格式

//type Trade struct {
//	InstrumentId   string  `parquet:"name=InstrumentId, type=BYTE_ARRAY, convertedtype=UTF8"`
//	TradeTimestamp int64   `parquet:"name=TradeTimestamp, type=INT64"`
//	TradeId        int64   `parquet:"name=TradeId, type=INT64"`
//	Price          float64 `parquet:"name=Price, type=DOUBLE"`
//	Volume         int64   `parquet:"name=Volume, type=INT64"`
//	Turnover       float64 `parquet:"name=Turnover, type=DOUBLE"`
//	Direction      string  `parquet:"name=Direction, type=BYTE_ARRAY, convertedtype=UTF8"`
//	BuyOrderId     int64   `parquet:"name=BuyOrderId, type=INT64"`
//	SellOrderId    int64   `parquet:"name=SellOrderId, type=INT64"`
//	LocalTimestamp int64   `parquet:"name=LocalTimestamp, type=INT64"`
//}

type Trade struct {
	InstrumentId   string  `parquet:"name=InstrumentId, type=BYTE_ARRAY, convertedtype=UTF8"`
	TradeTimestamp int64   `parquet:"name=TradeTimestamp, type=INT64"`
	TradeId        int64   `parquet:"name=TradeId, type=INT64"`
	Price          float64 `parquet:"name=Price, type=DOUBLE"`
	Volume         int64   `parquet:"name=Volume, type=INT64"`
	Turnover       float64 `parquet:"name=Turnover, type=DOUBLE"`
	Direction      string  `parquet:"name=Direction, type=BYTE_ARRAY, convertedtype=UTF8"`
	BuyOrderId     int64   `parquet:"name=BuyOrderId, type=INT64"`
	SellOrderId    int64   `parquet:"name=SellOrderId, type=INT64"`
	LocalTimestamp int64   `parquet:"name=LocalTimestamp, type=INT64"`
}

type Snapshot struct {
	InstrumentId   string `parquet:"name=InstrumentId, type=BYTE_ARRAY, convertedtype=UTF8"`
	LocalTimestamp int64  `parquet:"name=LocalTimestamp, type=INT64"`
}
