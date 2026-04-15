package logic

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/glog"
	jwt "github.com/golang-jwt/jwt/v5"

	"dev/internal/dao"
	"dev/internal/model"
	"dev/internal/service"
)

type sToken struct {
}

var (
	jwtSigningKey []byte
	jwtKeyOnce    sync.Once
)

func init() {
	service.RegisterToken(Newtoken())
	jwtKeyOnce.Do(func() {
		ctx := context.Background()
		secret := g.Cfg().MustGet(ctx, "jwt.secret").String()
		if secret == "" {
			key := make([]byte, 32)
			if _, err := rand.Read(key); err != nil {
				g.Log().Fatal(ctx, "JWT密钥生成失败:", err)
			}
			secret = hex.EncodeToString(key)
			g.Log().Warning(ctx, "未配置jwt.secret，已自动生成随机密钥（服务重启后Token将失效）")
		}
		if len(secret) < 32 {
			g.Log().Fatal(ctx, "JWT密钥长度不足32字符，当前长度:", len(secret))
		}
		jwtSigningKey = []byte(secret)
		g.Log().Info(ctx, "JWT签名密钥加载完成")
	})
}

func Newtoken() *sToken {
	return &sToken{}
}

func (s *sToken) GenToken(ctx context.Context, username string, expire time.Duration) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Issuer:    "gdtzSLMDP",
		Subject:   username,
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		NotBefore: jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(expire)),
	})

	tokenString, err := token.SignedString(jwtSigningKey)
	if err != nil {
		glog.Error(ctx, "token generate error")
		return "", err
	}

	s.AddToken(tokenString, &model.TokenInfo{
		SigningKey: nil,
		Username:   username,
	}, expire)

	g.Log().Info(ctx, "token生成成功")
	return tokenString, err
}

func (s *sToken) generateRandomKey(length int) ([]byte, error) {
	key := make([]byte, length)
	_, err := rand.Read(key)
	if err != nil {
		return nil, err
	}
	return key, nil
}

func (s *sToken) ValidateToken(ctx context.Context, tokenString string) (bool, error) {
	exist, err := dao.Redis.IsExist(ctx, tokenString)
	if err != nil {
		return false, err
	}
	if !exist {
		return false, gerror.New("token已失效或不存在")
	}

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (any, error) {
		if token.Method.Alg() != "HS256" {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return jwtSigningKey, nil
	})
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			_ = s.DelToken(tokenString)
			g.Log().Info(ctx, "token已过期，已清理")
		}
		return false, err
	}
	if !token.Valid {
		return false, nil
	}
	return true, nil
}

func (s *sToken) AddToken(tokenString string, in *model.TokenInfo, expire time.Duration) error {
	err := dao.Redis.Set(context.Background(), in.Username, tokenString, expire)
	if err != nil {
		return err
	}
	err = dao.Redis.Set(context.Background(), tokenString, in.Username, expire)
	if err != nil {
		return err
	}
	return nil
}

// GetSigningKey 获取token对应的签发密钥
func (s *sToken) GetSigningKey(tokenString string) ([]byte, error) {
	g.Log().Debug(context.Background(), "enter GetSigningKey")
	exist, err := dao.Redis.IsExist(context.Background(), tokenString)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, gerror.New("token does not exist")
	}

	siginingKey, err := dao.Redis.Get(context.Background(), tokenString)

	if err != nil {
		return nil, err
	}
	return []byte(siginingKey.(string)), nil
}

// DelToken 删除缓存中的token
func (s *sToken) DelToken(tokenString string) error {
	username, err := s.GetUser(tokenString)
	if err != nil {
		return err
	}
	err = dao.Redis.Del(context.Background(), username, tokenString)
	if err != nil {
		return err
	}
	return nil
}

func (s *sToken) GetUser(tokenString string) (username string, err error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (any, error) {
		if token.Method.Alg() != "HS256" {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return jwtSigningKey, nil
	})
	if err != nil {
		return "", err
	}
	username, err = token.Claims.GetSubject()
	if err != nil {
		return "", err
	}
	return username, err
}

// RepeatLogin 处理重复登录
func (s *sToken) RepeatLogin(ctx context.Context, username string) error {
	g.Log().Debug(ctx, "enter CheckLogin")
	exist, err := dao.Redis.IsExist(context.Background(), username)
	if err != nil {
		return err
	}
	if !exist {
		return nil
	}
	oldTokenString, err := dao.Redis.Get(context.Background(), username)
	g.Log().Debug(ctx, "oldTokenString: ", oldTokenString)
	if err != nil {
		return err
	}
	err = dao.Redis.Del(context.Background(), username, oldTokenString)
	g.Log().Debug(ctx, "Del error?")
	if err != nil {
		return err
	}
	return nil
}

// NoRepeatLogin 不处理重复登录
func (s *sToken) NoRepeatLogin(ctx context.Context, username string) (string, error) {
	g.Log().Debug(ctx, "enter CheckLogin")
	exist, err := dao.Redis.IsExist(context.Background(), username)
	if err != nil {
		return "", err
	}
	if !exist {
		return "", nil
	}
	tokenString, err := dao.Redis.Get(context.Background(), username)
	g.Log().Debug(ctx, "TokenString: ", tokenString)
	if err != nil {
		return "", err
	}
	return tokenString.(string), nil
}
