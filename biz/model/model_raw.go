package model

// ==== 原生数据格式

type ShRawSnapshot struct {
	// todo
}

type SzRawSnapshot struct {
	// todo
}

/*
exp 上海的逐笔成交是 4.24
BizIndex,Channel,SecurityID,TickTime,Type,BuyOrderNO,SellOrderNO,Price,Qty,TradeMoney,TickBSFlag,LocalTime,SeqNo
249552,1,603518,09:30:00.000,D,120,0,11.300,737300,0.000,B,09:30:00.183,1264046
249553,1,603518,09:30:00.000,D,134,0,11.300,703300,0.000,B,09:30:00.183,1264047
*/
type ShRawTrade struct {
	BizIndex    int64
	Channel     int64
	SecurityID  string
	TickTime    string
	Type        string
	BuyOrderNo  int64
	SellOrderNo int64
	Price       float64
	Qty         int64
	TradeMoney  float64
	TickBSFlag  string
	LocalTime   string
	SeqNo       int64
}

/*
exp 深圳的逐笔成交是 6.36
ChannelNo,ApplSeqNum,MDStreamID,BidApplSeqNum,OfferApplSeqNum,SecurityID,SecurityIDSource,LastPx,LastQty,ExecType,TransactTime,LocalTime,SeqNo
2022,22166,011,22164,17576,159915,102 ,1.7030,58700,70,09:30:00.000,09:30:00.047,281886,
2022,22167,011,22164,17578,159915,102 ,1.7030,58700,70,09:30:00.000,09:30:00.047,281887,
*/
type SzRawTrade struct {
	ChannelNo        int64
	ApplSeqNum       int64
	MDStreamID       string
	BidApplSeqNum    int64
	OfferApplSeqNum  int64
	SecurityID       string
	SecurityIDSource int64
	LastPx           float64
	LastQty          int64
	ExecType         int
	TransactTime     string
	LocalTime        string
	SeqNo            int64
}
