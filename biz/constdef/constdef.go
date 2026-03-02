package constdef

const (
	DirectionBuy     = "buy"
	DirectionSell    = "sell"
	DirectionUnknown = "unknown"
)

const (
	DataTypeSnapshot   = "snapshot"
	DataTypeTrade      = "trade"
	DataTypeOrder      = "order"
	DataTypeOrderQueue = "orderqueue"
)

// 委托类型常量
const (
	OrderTypeAdd    = "add"    // 新增委托
	OrderTypeCancel = "cancel" // 撤单
)
