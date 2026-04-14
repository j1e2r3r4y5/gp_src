package main

import (
	"context"
	"dev/internal/cmd"
	"dev/internal/dao"
	_ "dev/internal/packed"
	"time"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gctx"
	"github.com/gogf/gf/v2/os/gtime"
	_ "github.com/influxdata/influxdb-client-go/v2/api/write"

	_ "dev/internal/logic"

	_ "github.com/gogf/gf/contrib/drivers/mysql/v2"
)

func main() {
	ctx := gctx.GetInitCtx()
	g.Log().Info(ctx, "Starting backend server...")

	go func() {
		ticker := time.NewTicker(time.Minute / 2)
		defer ticker.Stop()
		for {
			<-ticker.C
			ctx := context.Background()
			_, err := dao.Dev.Ctx(ctx).
				Where(dao.Dev.Columns().LatestOnline+" < ?", gtime.Now().Add(-5*time.Minute)).
				Data(g.Map{dao.Dev.Columns().DevStatus: 0}).Update()
			// g.Log().Debug(ctx, "批量设备离线", err)
			if err != nil {
				g.Log().Error(ctx, "批量设备离线失败", err)
			}
		}
	}()

	g.Log().Info(ctx, "Running main command...")
	cmd.Main.Run(ctx)
	g.Log().Info(ctx, "Server stopped")
}
