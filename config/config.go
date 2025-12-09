package config

import (
	"encoding/json"
	"os"

	logger "github.com/2997215859/golog"
)

type Config struct {
	SrcDir       string   `json:"src_dir"`
	DstDir       string   `json:"dst_dir"`
	DateStart    string   `json:"date_start"`
	DateEnd      string   `json:"date_end"`
	DateList     []string `json:"date_list"`
	DataTypeList []string `json:"data_type_list"`
	Sort         bool     `json:"sort"`
}

var Cfg *Config

func ReadConfig(filepath string) *Config {
	// 根据 RUNTIME_ENV 这个环境变量来读取不同的配置
	//runtimeEnv := env.ENV()

	// 检查配置文件是否存在
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		logger.Fatal("config_file(%s) not exist: %s", filepath)
	}

	logger.Info("config_file(%s)", filepath)

	// 读取配置文件内容
	data, err := os.ReadFile(filepath)
	if err != nil {
		logger.Fatal("os.ReadFile(%s) error: %v", filepath, err)
	}

	// 解析 JSON 数据到 Config 结构体
	config := &Config{}
	if err := json.Unmarshal(data, &config); err != nil {
		logger.Fatal("json.Unmarshal(%s) error: %v", filepath, err)
	}

	Cfg = config
	return config
}

func InitConfig(filepath string) *Config {
	config := ReadConfig(filepath)

	logger.Info("config: %+v", config)

	return config
}
