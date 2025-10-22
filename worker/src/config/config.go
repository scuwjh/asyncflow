package config

import (
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

var Conf *TomlConfig

// TomlConfig 配置
type TomlConfig struct {
	FlowsvrAddr       string `toml:"flowsvr_addr"`        // flowsvr的地址
	RedisLockAddr     string `toml:"redis_lock_addr"`     // redis分布式锁的地址
	RedisLockPassword string `toml:"redis_lock_password"` // redis分布式锁的密码
}

// LoadConfig 导入配置
func (c *TomlConfig) LoadConfig() {

	if _, err := os.Stat(GetConfigPath()); err != nil {
		panic(err)
	}

	if _, err := toml.DecodeFile(GetConfigPath(), &c); err != nil {
		panic(err)
	}
}

func Init() {
	// 初始化配置
	initConf()
}

// InitConf 初始化配置
func initConf() {
	Conf = new(TomlConfig)
	Conf.LoadConfig()
}

// 项目主目录
var rootDir string

func GetConfigPath() string {
	return rootDir + "/config/config.toml"
}

func init() {
	inferRootDir()
	// 初始化配置
}

// 推断 Root目录（copy就行）
func inferRootDir() {
	pwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	var infer func(string) string
	infer = func(dir string) string {
		if exists(dir + "/main.go") {
			return dir
		}

		// 查看dir的父目录
		parent := filepath.Dir(dir)
		return infer(parent)
	}

	rootDir = infer(pwd)
}

func exists(dir string) bool {
	// 查找主机是不是存在 dir
	_, err := os.Stat(dir)
	return err == nil || os.IsExist(err)
}
