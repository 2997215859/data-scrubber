# 需求：输出模式配置 + 补充 Status/SeqNo 字段

## 背景
目前清洗后的数据按每天每个票一个文件存储。需要增加灵活的输出模式配置，同时补充行情中丢失的 Status 和 SeqNo 字段。

## 需求描述

### 1. 输出模式配置
- 新增配置项 `output_mode`，支持两种值：
  - `per_stock`（默认）：每天每个票一个文件（当前行为）
  - `per_day`：每天所有票在一个文件中
- per_day 模式文件名格式：`{date}_{datatype}.parquet`（如 `20220708_snapshot.parquet`）

### 2. 补充字段
- **所有数据类型**（snapshot/trade/order/orderqueue）增加 `SeqNo` 字段
  - 注意：通联历史数据可能因盘中断掉后补数据，缺少 SeqNo 和 LocalTime 字段，需兼容处理（缺失时默认为 0）
- **Snapshot** 增加 `Status` 字段（string 类型）
  - 沪市映射自 `InstruStatus`（如 "TRADE"）
  - 深市映射自 `TradingPhaseCode`（如 "T0"）
