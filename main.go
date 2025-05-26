package main

import (
	"data-scrubber/biz/service"
	"data-scrubber/config"
	logger "github.com/2997215859/golog"
	"github.com/spf13/pflag"
)

//TIP <p>To run your code, right-click the code and select <b>Run</b>.</p> <p>Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.</p>

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

	for _, date := range cfg.DateList {
		logger.Info("Process Date(%s) Begin", date)
		if err := service.MergeRawTrade(cfg.SrcDir, cfg.DstDir, date); err != nil {
			logger.Error("date(%s) MergeRawTrade error: %v", date, err)
		}
		logger.Info("Process Date(%s) End", date)
	}

	//service.ExampleUsage()

}
