package config

import (
	logger "github.com/2997215859/golog"
	"github.com/spf13/viper"
	"os"
)

type Config struct {
	SrcDir   string   `json:"src_dir"`
	DstDir   string   `json:"dst_dir"`
	DateList []string `json:"date_list"`
}

var Cfg *Config

func ReadConfig(filepath string) *Config {
	// 根据 RUNTIME_ENV 这个环境变量来读取不同的配置
	//runtimeEnv := env.ENV()

	// 检查配置文件是否存在
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		logger.Fatal("config_file(%s) not exist: %s", filepath)
	}

	//configFile := fmt.Sprintf("config.%s", runtimeEnv)
	//viper.SetConfigName(configFile)
	viper.SetConfigFile(filepath)

	viper.SetConfigType("json")

	//viper.AddConfigPath("./conf")

	logger.Info("config_file(%s)", filepath)

	if err := viper.ReadInConfig(); err != nil {
		logger.Fatal("viper.ReadInConfig(%s) error: %s", filepath, err)
	}

	config := &Config{}
	if err := viper.Unmarshal(&config); err != nil {
		logger.Fatal(err.Error())
	}
	Cfg = config
	return config
}

func InitConfig(filepath string) *Config {
	config := ReadConfig(filepath)

	logger.Info("config: %+v", config)

	return config
}
