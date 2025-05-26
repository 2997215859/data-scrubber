package config

import (
	"fmt"
	env "github.com/2997215859/goenv"
	logger "github.com/2997215859/golog"
	"github.com/spf13/viper"
)

type Config struct {
	SrcDir   string   `json:"src_dir"`
	DstDir   string   `json:"dst_dir"`
	DateList []string `json:"date_list"`
}

var Cfg *Config

func ReadConfig() *Config {
	// 根据 RUNTIME_ENV 这个环境变量来读取不同的配置
	runtimeEnv := env.ENV()

	configFile := fmt.Sprintf("config.%s", runtimeEnv)
	viper.SetConfigName(configFile)
	viper.SetConfigType("json")

	viper.AddConfigPath("./conf")

	logger.Info("config_file(%s)", configFile)

	if err := viper.ReadInConfig(); err != nil {
		logger.Fatal("viper.ReadInConfig(%s) error: %s", configFile, err)
	}

	config := &Config{}
	if err := viper.Unmarshal(&config); err != nil {
		logger.Fatal(err.Error())
	}
	Cfg = config
	return config
}

func InitConfig() *Config {
	config := ReadConfig()

	logger.Info("config: %+v", config)

	return config
}
