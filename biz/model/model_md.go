package model

// ==== 合并数据格式

type Trade struct {
	InstrumentId   string
	TradeTimestamp int64
	TradeId        int64
	Price          float64
	Volume         int64
	Turnover       float64
	Direction      string
	BuyOrderId     int64
	SellOrderId    int64
	LocalTimestamp int64
}
