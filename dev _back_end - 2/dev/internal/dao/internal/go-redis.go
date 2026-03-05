package internal

import (
	"context"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/redis/go-redis/v9"
)

// 内存存储作为Redis的备选方案
type memoryStoreItem struct {
	value    any
	expireAt time.Time
}

var memoryStore = make(map[string]*memoryStoreItem)

func initMemoryStoreCleanup() {
	go func() {
		for {
			time.Sleep(time.Minute * 10)
			now := time.Now()
			for key, item := range memoryStore {
				if now.After(item.expireAt) {
					delete(memoryStore, key)
				}
			}
		}
	}()
}

type RedisDao struct {
	rdb       *redis.Client
	limiter   *redis_rate.Limiter
	useMemory bool
}

var (
	rdb = redis.NewClient(&redis.Options{
		Addr: g.Cfg().MustGet(context.TODO(), "redis.host").String() + ":" + g.Cfg().MustGet(context.TODO(), "redis.port").String(),
	})
	limiter = redis_rate.NewLimiter(rdb)
	prefix  = g.Cfg().MustGet(context.TODO(), "redis.prefix").String()
)

func NewRedisDao() *RedisDao {
	// 测试Redis连接
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := rdb.Ping(ctx).Result()
	useMemory := err != nil

	if useMemory {
		g.Log().Error(context.TODO(), "Redis connection failed, using memory store as fallback")
		initMemoryStoreCleanup()
	} else {
		g.Log().Info(context.TODO(), "Redis connection established")
	}

	return &RedisDao{
		rdb:       rdb,
		limiter:   limiter,
		useMemory: useMemory,
	}
}

func (dao *RedisDao) Set(ctx context.Context, key string, value any, duration time.Duration) error {
	fullKey := prefix + key

	if dao.useMemory {
		memoryStore[fullKey] = &memoryStoreItem{
			value:    value,
			expireAt: time.Now().Add(duration),
		}
		return nil
	}

	_, err := dao.rdb.Set(ctx, fullKey, value, duration).Result()
	if err != nil {
		g.Log().Error(ctx, "Redis Set failed, falling back to memory store", err)
		memoryStore[fullKey] = &memoryStoreItem{
			value:    value,
			expireAt: time.Now().Add(duration),
		}
	}
	return nil
}

func (dao *RedisDao) Get(ctx context.Context, key string) (any, error) {
	fullKey := prefix + key

	// 先检查内存存储
	if item, exists := memoryStore[fullKey]; exists {
		if time.Now().Before(item.expireAt) {
			return item.value, nil
		}
		delete(memoryStore, fullKey)
	}

	if dao.useMemory {
		return nil, redis.Nil
	}

	value, err := dao.rdb.Get(ctx, fullKey).Result()
	if err == redis.Nil {
		return nil, err
	} else if err != nil {
		g.Log().Error(ctx, "Redis Get failed", err)
		return nil, err
	}
	return value, nil
}

func (dao *RedisDao) IsExist(ctx context.Context, key string) (bool, error) {
	fullKey := prefix + key

	// 先检查内存存储
	if item, exists := memoryStore[fullKey]; exists {
		if time.Now().Before(item.expireAt) {
			return true, nil
		}
		delete(memoryStore, fullKey)
	}

	if dao.useMemory {
		return false, nil
	}

	res, err := dao.rdb.Exists(ctx, fullKey).Result()
	if err != nil {
		g.Log().Error(ctx, "Redis Exists failed", err)
		return false, err
	}
	return res == 1, nil
}

func (dao *RedisDao) Del(ctx context.Context, keys ...any) error {
	for _, key := range keys {
		fullKey := prefix + key.(string)

		// 从内存存储中删除
		delete(memoryStore, fullKey)

		if !dao.useMemory {
			_, err := dao.rdb.Del(ctx, fullKey).Result()
			if err != nil {
				g.Log().Error(ctx, "Redis Del failed", err)
			}
		}
	}
	return nil
}

func (dao *RedisDao) Close() error {
	if !dao.useMemory {
		return dao.rdb.Close()
	}
	return nil
}

func (dao *RedisDao) Clear(ctx context.Context) {
	go func() {
		for {
			time.Sleep(time.Hour * 24)
			// 清理内存存储
			now := time.Now()
			for key, item := range memoryStore {
				if now.After(item.expireAt) {
					delete(memoryStore, key)
				}
			}

			// 清理Redis
			if !dao.useMemory {
				keys, _, err := dao.rdb.Scan(ctx, 0, prefix+"*", 0).Result()
				if err != nil {
					g.Log().Error(ctx, err)
				}

				for _, key := range keys {
					if err := dao.rdb.Del(ctx, key).Err(); err != nil {
						g.Log().Error(ctx, err)
					}
				}
			}
		}
	}()
}

func (dao *RedisDao) Limit(ctx context.Context, tokenString string) (bool, error) {
	if dao.useMemory {
		// 内存存储模式下，默认允许所有请求
		return true, nil
	}

	res, err := dao.limiter.Allow(ctx, tokenString, redis_rate.PerHour(g.Cfg().MustGet(ctx, "redis.limiter").Int()))
	if err != nil {
		g.Log().Error(ctx, "Redis rate limit failed, allowing request", err)
		return true, nil
	}
	if res.Allowed <= 0 {
		return false, nil
	}
	return true, nil
}
