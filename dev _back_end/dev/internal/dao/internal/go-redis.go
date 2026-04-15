package internal

import (
	"context"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/redis/go-redis/v9"
)

type RedisDao struct {
	rdb     *redis.Client
	limiter *redis_rate.Limiter
}

var (
	rdb = redis.NewClient(&redis.Options{
		Addr: g.Cfg().MustGet(context.TODO(), "redis.host").String() + ":" + g.Cfg().MustGet(context.TODO(), "redis.port").String(),
	})
	limiter = redis_rate.NewLimiter(rdb)
	prefix  = g.Cfg().MustGet(context.TODO(), "redis.prefix").String()
)

func NewRedisDao() *RedisDao {
	return &RedisDao{
		rdb:     rdb,
		limiter: limiter,
	}
}

func (dao *RedisDao) Set(ctx context.Context, key string, value any, duration time.Duration) error {
	_, err := dao.rdb.Set(ctx, prefix+key, value, duration).Result()
	return err
}

func (dao *RedisDao) Get(ctx context.Context, key string) (any, error) {
	return dao.rdb.Get(ctx, prefix+key).Result()
}

func (dao *RedisDao) IsExist(ctx context.Context, key string) (bool, error) {
	res, err := dao.rdb.Exists(ctx, prefix+key).Result()
	return res == 1, err
}

func (dao *RedisDao) Del(ctx context.Context, keys ...any) error {
	for _, key := range keys {
		_, err := dao.rdb.Del(ctx, prefix+key.(string)).Result()
		if err != nil {
			return err
		}
	}
	return nil
}

func (dao *RedisDao) Close() error {
	return dao.rdb.Close()
}

// BL-09修复：Clear方法添加作用域限制，支持按pattern删除
// scope参数示例："cache"只清除缓存，"token"只清除token，"*"清除所有
func (dao *RedisDao) Clear(ctx context.Context, scope string) {
	go func() {
		for {
			time.Sleep(time.Hour * 24)

			var pattern string
			switch scope {
			case "cache":
				pattern = prefix + "cache:*"
			case "token":
				pattern = prefix + "token:*"
			case "dedup":
				pattern = prefix + "mqtt:dedup:*"
			case "*":
				pattern = prefix + "*"
			default:
				pattern = prefix + "*"
			}

			g.Log().Info(ctx, "开始清理Redis keys, pattern:", pattern)
			var cursor uint64
			var totalDeleted int64

			for {
				keys, newCursor, err := dao.rdb.Scan(ctx, cursor, pattern, 100).Result()
				if err != nil {
					g.Log().Error(ctx, "Scan失败", err)
					break
				}

				if len(keys) > 0 {
					deleted, err := dao.rdb.Del(ctx, keys...).Result()
					if err != nil {
						g.Log().Error(ctx, "Del失败", err)
					} else {
						totalDeleted += deleted
					}
				}

				cursor = newCursor
				if cursor == 0 {
					break
				}
			}
			g.Log().Info(ctx, "Redis清理完成，共删除", totalDeleted, "个keys")
		}
	}()
}

func (dao *RedisDao) Limit(ctx context.Context, tokenString string) (bool, error) {
	rateLimitPerSecond := g.Cfg().MustGet(ctx, "redis.limiter").Int()
	if rateLimitPerSecond <= 0 {
		rateLimitPerSecond = 100
	}
	res, err := dao.limiter.Allow(ctx, "rate_limit:global", redis_rate.PerSecond(rateLimitPerSecond))
	if err != nil {
		g.Log().Error(ctx, "限流检查失败", err)
		return false, err
	}
	if res.Allowed <= 0 {
		g.Log().Warning(ctx, "请求过于频繁，限流生效")
		return false, nil
	}
	return true, nil
}
