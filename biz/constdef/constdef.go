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

// 输出模式常量
const (
	OutputModePerStock = "per_stock" // 每天每个票一个文件
	OutputModePerDay   = "per_day"   // 每天所有票一个文件
)
