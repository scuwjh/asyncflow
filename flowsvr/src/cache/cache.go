package cache

import (
	"github.com/niuniumart/asyncflow/flowsvr/src/config"
	"github.com/niuniumart/gosdk/goredis"
	"github.com/niuniumart/gosdk/martlog"
	"time"
)

var (
	rdb        *goredis.RedisCli
	prefix     = "asyncflow_zhugeqing_"
	expireTime = time.Hour * 24 // 默认为24小时
)

func InitCache() error {
	goredis.Factory.MaxIdleConn = config.Conf.Redis.MaxIdle
	goredis.Factory.IdleTimeout = time.Second * time.Duration(config.Conf.Redis.IdleTimeout)
	goredis.Factory.MaxConn = config.Conf.Redis.MaxActive

	redisCli, err := goredis.Factory.CreateRedisCli(config.Conf.Redis.Auth, config.Conf.Redis.Url)
	if err != nil {
		martlog.Errorf("Redis connection error: %v", err)
		return err
	}
	rdb = redisCli

	if config.Conf.Redis.CacheTimeoutDay != 0 {
		// 设置为config.Conf.Redis.CacheTimeoutDay 天的过期时间
		expireTime = expireTime * time.Duration(config.Conf.Redis.CacheTimeoutDay)
	}

	return nil
}
