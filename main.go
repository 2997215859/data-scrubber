package main

import (
	"data-scrubber/biz/constdef"
	"data-scrubber/biz/service"
	"data-scrubber/biz/utils"
	"data-scrubber/config"
	logger "github.com/2997215859/golog"
	"github.com/golang-module/carbon/v2"
	"github.com/spf13/pflag"
	"path/filepath"
	"slices"
)

func init() {
	carbon.SetDefault(carbon.Default{
		Layout:       carbon.RFC3339Layout,
		Timezone:     carbon.PRC,
		WeekStartsAt: carbon.Sunday,
		Locale:       "zh-CN",
	})
}

func GetConfigFilePath() string {
	// 定义命令行参数
	var configFile string
	pflag.StringVarP(&configFile, "config_file", "c", "conf/config.dev.json", "配置文件路径(支持绝对路径和相对路径)")
	pflag.Parse()
	return configFile
}

func main() {
	cfg := config.InitConfig(GetConfigFilePath())

	config.PrintVersionInfo()

	startDate := carbon.Parse(cfg.DateStart).StartOfDay()
	if startDate.IsInvalid() {
		logger.Error("cfg.DateStart(%s) is invalid", cfg.DateStart)
		return
	}
	endDate := carbon.Parse(cfg.DateEnd).StartOfDay()
	if endDate.IsInvalid() {
		logger.Error("cfg.DateEnd(%s) is invalid", cfg.DateEnd)
		return
	}

	for currentDate := startDate; currentDate.Lte(endDate); currentDate = currentDate.AddDay() {
		date := currentDate.Format("Ymd")
		// 检查当前天是否存在
		dateDir := filepath.Join(cfg.SrcDir, date)
		if !utils.Exists(dateDir) {
			logger.Warn("date(%s) not exists", date)
			continue
		}

		if slices.Contains(cfg.DataTypeList, constdef.DataTypeSnapshot) {
			snapshotDstDir := filepath.Join(cfg.SrcDir, date, constdef.DataTypeSnapshot)
			logger.Info("Process Date(%s) Snapshot Begin", date)
			if err := service.MergeRawSnapshot(cfg.SrcDir, snapshotDstDir, date); err != nil {
				logger.Error("date(%s) MergeRawSnapshot error: %v", date, err)
			}
			logger.Info("Process Date(%s) Snapshot End", date)
		}

		if slices.Contains(cfg.DataTypeList, constdef.DataTypeTrade) {
			tradeDstDir := filepath.Join(cfg.SrcDir, date, constdef.DataTypeTrade)
			logger.Info("Process Date(%s) Trade Begin", date)
			if err := service.MergeRawTrade(cfg.SrcDir, tradeDstDir, date); err != nil {
				logger.Error("date(%s) MergeRawTrade error: %v", date, err)
			}
			logger.Info("Process Date(%s) Trade End", date)
		}
	}

	//service.ExampleUsage()

}
