# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Go application (`data-scrubber`) that reads raw tick-level market data (snapshots and trades) from Shanghai (SH) and Shenzhen (SZ) stock exchanges, normalizes them into a unified format, and writes per-stock Parquet files. Runs as a daily cron job or batch backfill tool.

## Build & Run

```shell
# Build (outputs to dest/, cross-compiles for linux/amd64 with version ldflags)
bash scripts/build.sh

# Run with a specific config
./data-scrubber --config_file=conf/config.dev.json
./data-scrubber -c conf/config.dev.json

# Daily cron mode (auto-updates dates in config to today)
bash scripts/run.sh -d
```

## Testing

```shell
go test ./...                 # all tests
go test ./biz/utils/          # specific package
go test ./biz/service/        # service tests (some require TuShare API access)
```

Tests are sparse and mostly integration-style using Go's standard `testing` package. No mocks or test framework.

## Architecture

### Data Flow

1. **main.go** — loads JSON config, inits TuShare client, iterates over date range, calls `RunDaily()` per date
2. **RunDaily** dispatches to snapshot and/or trade pipelines based on `data_type_list` in config
3. **Snapshot pipeline** (`biz/service/snapshot_data_scrubber.go: MergeRawSnapshot`):
   - Reads SH snapshots from `{date}_MarketData.csv.zip`, SZ from `{date}_mdl_6_28_0.csv.zip`
   - Converts raw structs → unified `model.Snapshot`, fetches SH price limits from TuShare API
   - Merge-sorts SH+SZ by timestamp, groups by instrument, writes per-stock Parquet files
4. **Trade pipeline** (`biz/service/trade_data_scrubber.go: MergeRawTrade`):
   - SH trades: different source formats pre/post 20231204
   - SZ trades from `{date}_mdl_6_36_0.csv.zip`
   - Converts to unified `model.Trade`, determines buy/sell direction
   - Merge-sorts, groups by instrument, writes per-stock Parquet files

### Package Structure

- `config/` — Config struct (JSON loader) and version info (build-time ldflags)
- `conf/` — JSON config files for dev/prod/daily environments
- `biz/constdef/` — Constants (DirectionBuy/Sell, DataTypeSnapshot/Trade)
- `biz/errorx/` — Custom error types with business error codes
- `biz/model/` — Raw exchange-specific structs (`model_raw.go`) and normalized output structs with parquet tags (`model_md.go`)
- `biz/service/` — Core business logic: data reading, conversion, merge-sorting, Parquet writing
- `biz/utils/` — Time conversion, file existence checks, stock code helpers, retry wrapper
- `biz/upstream/gotushare/` — Vendored TuShare API SDK (fetches price limits, trade calendar)

### Data Source (通联 MDL)

Raw data comes from 通联数据 MDL 客户端, documented at `/mnt/d/store/yjh/通联/通联数据-MDL客户端使用说明-202411.pdf`.

MDL writes CSV files named `mdl_<ServiceID>_<MessageID>_<Index>.csv`. This project consumes:

| Data Type | File Pattern | ServiceID.MessageID | Description |
|-----------|-------------|---------------------|-------------|
| SH L2 快照 | `MarketData.csv.zip` (= mdl_4_4_0) | 4.4 | 沪 L2 十档行情 |
| SZ L2 快照 | `mdl_6_28_0.csv.zip` | 6.28 | 深 L2 十档行情 |
| SH L2 逐笔 | `mdl_4_24_0.csv.zip` (新) / `Transaction.csv.zip` (旧) | 4.24 | 沪 L2 逐笔合并行情 |
| SZ L2 逐笔 | `mdl_6_36_0.csv.zip` | 6.36 | 深 L2 逐笔成交 |



### Key Format Details

- SH trade format changed on 20231204: new format (`mdl_4_24_0`) vs old format (`Transaction.csv.zip` with `OldShRawTrade`). Old format also lacks BizIndex before 20210426.
- CSV parsing uses both `gocsv.Unmarshal` and manual `splitLine` parser (for trailing-comma formats)
- Output: Parquet with Snappy compression, 128MB row groups, one file per stock per date per data type

## Conventions

- Commit messages in Chinese with conventional prefixes (`feat:`, `fix:`, `chore:`)
- Comments throughout codebase are in Chinese
- Config is JSON, path passed via `--config_file` / `-c` flag (default: `conf/config.dev.json`)
- Go module name: `data-scrubber`, Go 1.23.0

## Key Dependencies

- `github.com/xitongsys/parquet-go` — Parquet writing
- `github.com/golang-module/carbon/v2` — Date/time (PRC timezone)
- `github.com/gocarina/gocsv` — CSV deserialization
- `github.com/spf13/pflag` — CLI flags
- `github.com/2997215859/golog` — Logging
