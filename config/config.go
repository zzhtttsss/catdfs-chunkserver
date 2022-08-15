package config

import (
	"fmt"
	"github.com/spf13/viper"
	"os"
)

func Config() {
	pwd, _ := os.Getwd()
	viper.AddConfigPath(pwd + "\\config\\")
	viper.SetConfigName("config")
	err := viper.ReadInConfig() // 查找并读取配置文件
	if err != nil {             // 处理读取配置文件的错误
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
}
