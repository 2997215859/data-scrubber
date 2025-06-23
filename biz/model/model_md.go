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
	InstrumentId    string  `parquet:"name=InstrumentId, type=BYTE_ARRAY, convertedtype=UTF8"`
	UpdateTimestamp int64   `parquet:"name=UpdateTimestamp, type=INT64"`
	Last            float64 `parquet:"name=Last, type=DOUBLE"`

	PreClose float64 `parquet:"name=PreClose, type=DOUBLE"`
	Open     float64 `parquet:"name=Open, type=DOUBLE"`
	High     float64 `parquet:"name=High, type=DOUBLE"`
	Low      float64 `parquet:"name=Low, type=DOUBLE"`
	Close    float64 `parquet:"name=Close, type=DOUBLE"`

	TradeNumber   int64   `parquet:"name=TradeNumber, type=INT64"`    // 成交笔数
	TradeVolume   int64   `parquet:"name=TradeVolume, type=INT64"`    // 成交总量
	TradeTurnover float64 `parquet:"name=TradeTurnover, type=DOUBLE"` // 成交总金额

	HighLimit float64 `parquet:"name=HighLimit, type=DOUBLE"`
	LowLimit  float64 `parquet:"name=LowLimit, type=DOUBLE"`

	BidVolumeList []int64   `parquet:"name=BidVolumeList, type=MAP, convertedtype=LIST, valuetype=INT64"`
	BidPriceList  []float64 `parquet:"name=BidPriceList, type=MAP, convertedtype=LIST, valuetype=DOUBLE"`
	AskVolumeList []int64   `parquet:"name=AskVolumeList, type=MAP, convertedtype=LIST, valuetype=INT64"`
	AskPriceList  []float64 `parquet:"name=AskPriceList, type=MAP, convertedtype=LIST, valuetype=DOUBLE"`

	LocalTimestamp int64 `parquet:"name=LocalTimestamp, type=INT64"`
}
