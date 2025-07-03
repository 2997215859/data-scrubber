package model

// ==== 原生数据格式

/*
*
exp 上海的快照是 MarketData.csv.zip
UpdateTime,SecurityID,ImageStatus,PreCloPrice,OpenPrice,HighPrice,LowPrice,LastPrice,ClosePrice,InstruStatus,TradNumber,TradVolume,Turnover,TotalBidVol,WAvgBidPri,AltWAvgBidPri,TotalAskVol,WAvgAskPri,AltWAvgAskPri,EtfBuyNumber,EtfBuyVolume,EtfBuyMoney,EtfSellNumber,EtfSellVolume,ETFSellMoney,YieldToMatu,TotWarExNum,WarLowerPri,WarUpperPri,WiDBuyNum,WiDBuyVol,WiDBuyMon,WiDSellNum,WiDSellVol,WiDSellMon,TotBidNum,TotSellNum,MaxBidDur,MaxSellDur,BidNum,SellNum,IOPV,AskPrice1,AskVolume1,AskPrice2,AskVolume2,AskPrice3,AskVolume3,AskPrice4,AskVolume4,AskPrice5,AskVolume5,AskPrice6,AskVolume6,AskPrice7,AskVolume7,AskPrice8,AskVolume8,AskPrice9,AskVolume9,AskPrice10,AskVolume10,BidPrice1,BidVolume1,BidPrice2,BidVolume2,BidPrice3,BidVolume3,BidPrice4,BidVolume4,BidPrice5,BidVolume5,BidPrice6,BidVolume6,BidPrice7,BidVolume7,BidPrice8,BidVolume8,BidPrice9,BidVolume9,BidPrice10,BidVolume10,NumOrdersB1,NumOrdersB2,NumOrdersB3,NumOrdersB4,NumOrdersB5,NumOrdersB6,NumOrdersB7,NumOrdersB8,NumOrdersB9,NumOrdersB10,NumOrdersS1,NumOrdersS2,NumOrdersS3,NumOrdersS4,NumOrdersS5,NumOrdersS6,NumOrdersS7,NumOrdersS8,NumOrdersS9,NumOrdersS10,LocalTime,SeqNo
09:39:34.000,600479,1,10.370,10.340,10.440,10.310,10.430,0.000,TRADE,1133,477700.000,4960839.00000,320800.000,10.193,0.000,576800.000,10.836,0.000,0,0.000,0.00000,0,0.000,0.00000,0.0000,0.000,0.000,0.00000,421,262400.000,2730547.00000,227,143700.000,1476063.00000,325,261,237,205,58,73,0.000,10.430,100.000,10.440,6800.000,10.450,3900.000,10.460,10700.000,10.470,20800.000,10.480,6200.000,10.490,29500.000,10.500,27700.000,10.510,14900.000,10.520,9400.000,10.420,2500.000,10.410,18800.000,10.400,14400.000,10.390,1600.000,10.380,4700.000,10.370,2100.000,10.360,16000.000,10.350,2100.000,10.340,1100.000,10.330,5900.000,7,48,12,4,9,2,4,4,1,8,1,8,5,7,8,5,10,9,3,7,09:39:34.027,1099999,
09:39:34.000,600581,1,3.500,3.490,3.500,3.430,3.460,0.000,TRADE,1815,1958800.000,6765855.00000,1246600.000,3.409,0.000,2066620.000,3.596,0.000,0,0.000,0.00000,0,0.000,0.00000,0.0000,0.000,0.000,0.00000,173,721800.000,2482509.00000,843,1859700.000,6516812.00000,322,266,215,202,18,37,0.000,3.460,71900.000,3.470,87200.000,3.480,121700.000,3.490,78500.000,3.500,81200.000,3.510,45700.000,3.520,120720.000,3.530,426400.000,3.540,97100.000,3.550,97600.000,3.450,52500.000,3.440,309700.000,3.430,271800.000,3.420,179800.000,3.410,160600.000,3.400,105000.000,3.390,18600.000,3.380,43000.000,3.370,2600.000,3.360,6300.000,12,105,56,35,25,42,8,12,2,3,15,14,11,11,19,9,11,27,10,12,09:39:34.027,1100000,
*/
type ShRawSnapshot struct {
	UpdateTime    string
	SecurityID    string
	ImageStatus   int
	PreCloPrice   float64
	OpenPrice     float64
	HighPrice     float64
	LowPrice      float64
	LastPrice     float64
	ClosePrice    float64
	InstruStatus  string
	TradNumber    int64
	TradVolume    float64
	Turnover      float64
	TotalBidVol   float64
	WAvgBidPri    float64
	AltWAvgBidPri float64
	TotalAskVol   float64
	WAvgAskPri    float64
	AltWAvgAskPri float64
	EtfBuyNumber  int
	EtfBuyVolume  float64
	EtfBuyMoney   float64
	EtfSellNumber int
	EtfSellVolume float64
	ETFSellMoney  float64
	YieldToMatu   float64
	TotWarExNum   float64
	WarLowerPri   float64
	WarUpperPri   float64
	WiDBuyNum     int
	WiDBuyVol     float64
	WiDBuyMon     float64
	WiDSellNum    int
	WiDSellVol    float64
	WiDSellMon    float64
	TotBidNum     int
	TotSellNum    int
	MaxBidDur     int
	MaxSellDur    int
	BidNum        int
	SellNum       int
	IOPV          float64
	AskPrice1     float64
	AskVolume1    float64
	AskPrice2     float64
	AskVolume2    float64
	AskPrice3     float64
	AskVolume3    float64
	AskPrice4     float64
	AskVolume4    float64
	AskPrice5     float64
	AskVolume5    float64
	AskPrice6     float64
	AskVolume6    float64
	AskPrice7     float64
	AskVolume7    float64
	AskPrice8     float64
	AskVolume8    float64
	AskPrice9     float64
	AskVolume9    float64
	AskPrice10    float64
	AskVolume10   float64
	BidPrice1     float64
	BidVolume1    float64
	BidPrice2     float64
	BidVolume2    float64
	BidPrice3     float64
	BidVolume3    float64
	BidPrice4     float64
	BidVolume4    float64
	BidPrice5     float64
	BidVolume5    float64
	BidPrice6     float64
	BidVolume6    float64
	BidPrice7     float64
	BidVolume7    float64
	BidPrice8     float64
	BidVolume8    float64
	BidPrice9     float64
	BidVolume9    float64
	BidPrice10    float64
	BidVolume10   float64
	NumOrdersB1   int
	NumOrdersB2   int
	NumOrdersB3   int
	NumOrdersB4   int
	NumOrdersB5   int
	NumOrdersB6   int
	NumOrdersB7   int
	NumOrdersB8   int
	NumOrdersB9   int
	NumOrdersB10  int
	NumOrdersS1   int
	NumOrdersS2   int
	NumOrdersS3   int
	NumOrdersS4   int
	NumOrdersS5   int
	NumOrdersS6   int
	NumOrdersS7   int
	NumOrdersS8   int
	NumOrdersS9   int
	NumOrdersS10  int
	LocalTime     string
	SeqNo         int64
}

/*
*
exp 深圳的快照是 6_28_0
UpdateTime,MDStreamID,SecurityID,SecurityIDSource,TradingPhaseCode,PreCloPrice,TurnNum,Volume,Turnover,LastPrice,OpenPrice,HighPrice,LowPrice,DifPrice1,DifPrice2,PE1,PE2,PreCloseIOPV,IOPV,TotalBidQty,WeightedAvgBidPx,TotalOfferQty,WeightedAvgOfferPx,HighLimitPrice,LowLimitPrice,OpenInt,OptPremiumRatio,AskPrice1,AskVolume1,AskPrice2,AskVolume2,AskPrice3,AskVolume3,AskPrice4,AskVolume4,AskPrice5,AskVolume5,AskPrice6,AskVolume6,AskPrice7,AskVolume7,AskPrice8,AskVolume8,AskPrice9,AskVolume9,AskPrice10,AskVolume10,BidPrice1,BidVolume1,BidPrice2,BidVolume2,BidPrice3,BidVolume3,BidPrice4,BidVolume4,BidPrice5,BidVolume5,BidPrice6,BidVolume6,BidPrice7,BidVolume7,BidPrice8,BidVolume8,BidPrice9,BidVolume9,BidPrice10,BidVolume10,NumOrdersB1,NumOrdersB2,NumOrdersB3,NumOrdersB4,NumOrdersB5,NumOrdersB6,NumOrdersB7,NumOrdersB8,NumOrdersB9,NumOrdersB10,NumOrdersS1,NumOrdersS2,NumOrdersS3,NumOrdersS4,NumOrdersS5,NumOrdersS6,NumOrdersS7,NumOrdersS8,NumOrdersS9,NumOrdersS10,LocalTime,SeqNo
09:41:36.000,010,300319,102 ,T0      ,8.7500,2406,1586600,13856440.0000,8.720000,8.740000,8.790000,8.680000,-0.030000,0.000000,37.810000,0.000000,0.000000,0.000000,452200,8.510000,772400,9.630000,10.500000,7.000000,0,0.000000,8.730000,3700,8.740000,16500,8.750000,13200,8.760000,2000,8.770000,4100,8.780000,19300,8.790000,59400,8.800000,11200,8.810000,3600,8.820000,13700,8.720000,3700,8.710000,7000,8.700000,10200,8.690000,24200,8.680000,35300,8.670000,17200,8.660000,43600,8.650000,47200,8.640000,14900,8.630000,12200,1,14,10,13,13,10,17,13,3,8,6,27,7,4,4,2,16,7,2,4,09:41:37.203,1099999,
09:41:36.000,010,300322,102 ,T0      ,9.8400,2474,2100800,20461549.0000,9.780000,9.830000,9.830000,9.690000,-0.060000,0.000000,0.000000,0.000000,0.000000,0.000000,662200,9.570000,1243622,10.880000,11.810000,7.870000,0,0.000000,9.780000,13700,9.790000,10100,9.800000,13900,9.810000,100,9.820000,2000,9.830000,14300,9.840000,14100,9.850000,8700,9.860000,1100,9.870000,5000,9.770000,12200,9.760000,4200,9.750000,34500,9.740000,1900,9.730000,500,9.720000,12400,9.710000,20200,9.700000,78300,9.690000,59200,9.680000,56100,2,7,2,4,1,4,9,37,23,17,15,23,3,1,1,2,6,3,2,1,09:41:37.203,1100000,
*/
type SzRawSnapshot struct {
	UpdateTime         string
	MDStreamID         string
	SecurityID         string
	SecurityIDSource   string
	TradingPhaseCode   string
	PreCloPrice        float64
	TurnNum            int64
	Volume             int64
	Turnover           float64
	LastPrice          float64
	OpenPrice          float64
	HighPrice          float64
	LowPrice           float64
	DifPrice1          float64
	DifPrice2          float64
	PE1                float64
	PE2                float64
	PreCloseIOPV       float64
	IOPV               float64
	TotalBidQty        int64
	WeightedAvgBidPx   float64
	TotalOfferQty      int64
	WeightedAvgOfferPx float64
	HighLimitPrice     float64
	LowLimitPrice      float64
	OpenInt            int64
	OptPremiumRatio    float64
	AskPrice1          float64
	AskVolume1         int64
	AskPrice2          float64
	AskVolume2         int64
	AskPrice3          float64
	AskVolume3         int64
	AskPrice4          float64
	AskVolume4         int64
	AskPrice5          float64
	AskVolume5         int64
	AskPrice6          float64
	AskVolume6         int64
	AskPrice7          float64
	AskVolume7         int64
	AskPrice8          float64
	AskVolume8         int64
	AskPrice9          float64
	AskVolume9         int64
	AskPrice10         float64
	AskVolume10        int64
	BidPrice1          float64
	BidVolume1         int64
	BidPrice2          float64
	BidVolume2         int64
	BidPrice3          float64
	BidVolume3         int64
	BidPrice4          float64
	BidVolume4         int64
	BidPrice5          float64
	BidVolume5         int64
	BidPrice6          float64
	BidVolume6         int64
	BidPrice7          float64
	BidVolume7         int64
	BidPrice8          float64
	BidVolume8         int64
	BidPrice9          float64
	BidVolume9         int64
	BidPrice10         float64
	BidVolume10        int64
	NumOrdersB1        int
	NumOrdersB2        int
	NumOrdersB3        int
	NumOrdersB4        int
	NumOrdersB5        int
	NumOrdersB6        int
	NumOrdersB7        int
	NumOrdersB8        int
	NumOrdersB9        int
	NumOrdersB10       int
	NumOrdersS1        int
	NumOrdersS2        int
	NumOrdersS3        int
	NumOrdersS4        int
	NumOrdersS5        int
	NumOrdersS6        int
	NumOrdersS7        int
	NumOrdersS8        int
	NumOrdersS9        int
	NumOrdersS10       int
	LocalTime          string
	SeqNo              int64
}

/*
exp 上海的逐笔成交是 4_24
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
exp 深圳的逐笔成交是 6_36
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

/**
Transaction.csv.zip

DataStatus,TradeIndex,TradeChan,SecurityID,TradTime,TradPrice,TradVolume,TradeMoney,TradeBuyNo,TradeSellNo,TradeBSFlag,BizIndex,LocalTime,SeqNo
0,1,1,601360,09:25:00.000,9.340,900.000,8406.00000,186085,203555,N,2767,09:25:00.176,1,
0,2,1,601360,09:25:00.000,9.340,100.000,934.00000,194706,203555,N,2768,09:25:00.176,2,
0,3,1,601360,09:25:00.000,9.340,700.000,6538.00000,256622,203555,N,2769,09:25:00.176,3,
0,4,1,601360,09:25:00.000,9.340,2800.000,26152.00000,73787,203555,N,2770,09:25:00.176,4,
0,5,1,601360,09:25:00.000,9.340,800.000,7472.00000,73787,212050,N,2771,09:25:00.176,5,
*/

// 在 20231204 之后（含）是新数据格式，在此之前是旧格式
type OldShRawTrade struct {
	DataStatus  int     // 数据状态
	TradeIndex  int64   // 交易索引
	TradeChan   int     // 交易通道
	SecurityID  string  // 证券代码
	TradTime    string  // 交易时间
	TradPrice   float64 // 交易价格
	TradVolume  float64 // 交易成交量
	TradeMoney  float64 // 交易金额
	TradeBuyNo  int64   // 买方委托号
	TradeSellNo int64   // 卖方委托号
	TradeBSFlag string  // 买卖标志(N可能表示中性或其他)
	BizIndex    int64   // 业务索引
	LocalTime   string  // 本地时间
	SeqNo       int64   // 序列号
}
