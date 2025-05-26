package main

import (
	"data-scrubber/biz/service"
	"data-scrubber/config"
	logger "github.com/2997215859/golog"
)

//TIP <p>To run your code, right-click the code and select <b>Run</b>.</p> <p>Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.</p>

func main() {
	cfg := config.InitConfig()

	config.PrintVersionInfo()

	for _, date := range cfg.DateList {
		if err := service.MergeRawTrade(cfg.SrcDir, cfg.DstDir, date); err != nil {
			logger.Error("date(%s) MergeRawTrade error: %v", date, err)
		}
	}

}
