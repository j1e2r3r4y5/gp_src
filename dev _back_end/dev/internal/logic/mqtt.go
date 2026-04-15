package logic

import (
	"context"
	"crypto/md5"
	"dev/internal/dao"
	"dev/internal/model"
	"dev/internal/service"
	"fmt"
	"regexp"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/frame/g"
)

var (
	opts       = mqtt.NewClientOptions()
	BrokerURI  = g.Cfg().MustGet(context.Background(), "mqtt.brokerURI").String()
	mqttClient mqtt.Client

	SubscribedDevIds = make([]uint64, 0)

	reconnectAttempts    = 0
	maxReconnectAttempts = 10
	baseReconnectDelay   = time.Second
)

type sMqtt struct {
}

func init() {
	service.RegisterMqtt(New_Mqtt())
}
func New_Mqtt() *sMqtt {
	return &sMqtt{}
}

// Init 初始化
func (s *sMqtt) Init() {
	opts.AddBroker(BrokerURI)
	opts.SetClientID(g.Cfg().MustGet(context.Background(), "mqtt.clientID").String())
	opts.SetCleanSession(g.Cfg().MustGet(context.Background(), "mqtt.cleanSession").Bool())
	opts.SetUsername(g.Cfg().MustGet(context.Background(), "mqtt.username").String())
	opts.SetPassword(g.Cfg().MustGet(context.Background(), "mqtt.password").String())
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(5 * time.Second)
	opts.OnConnect = func(c mqtt.Client) {
		fmt.Println("MQTT连接成功")
		reconnectAttempts = 0
		s.subscribeAll(context.Background())
	}
	opts.OnConnectionLost = func(c mqtt.Client, err error) {
		g.Log().Warning(context.Background(), "MQTT连接断开: ", err)
		go s.handleReconnect()
	}
	mqttClient = mqtt.NewClient(opts)

	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		g.Log().Panic(context.Background(), token.Error())
	}
}

// BL-04修复：实现自动重连机制
func (s *sMqtt) handleReconnect() {
	for reconnectAttempts < maxReconnectAttempts {
		reconnectAttempts++
		delay := baseReconnectDelay * time.Duration(1<<uint(reconnectAttempts))
		if delay > time.Minute {
			delay = time.Minute
		}
		g.Log().Info(context.Background(), "尝试MQTT重连 (", reconnectAttempts, "/", maxReconnectAttempts, "), 等待 ", delay)
		time.Sleep(delay)

		if mqttClient.IsConnected() {
			g.Log().Info(context.Background(), "MQTT重连成功")
			return
		}

		if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
			g.Log().Error(context.Background(), "MQTT重连失败: ", token.Error())
		} else {
			g.Log().Info(context.Background(), "MQTT重连成功")
			return
		}
	}
	g.Log().Error(context.Background(), "MQTT重连次数已达上限，请检查网络或服务器状态")
}

// getUPTopic 获得指定设备的上行主题
func (s *sMqtt) getUPTopic(device string) string {
	return fmt.Sprintf("/dtu/%s/up", device)
}

// subscribeTopic 订阅指定主题
func (s *sMqtt) SubscribeTopic(ctx context.Context, device string) {
	topic := s.getUPTopic(device)
	fmt.Printf("add topic %s\n", topic)
	if token := mqttClient.Subscribe(topic, 2, func(c mqtt.Client, msg mqtt.Message) {
		if err := s.messageHandler(ctx, c, msg); err != nil {
			g.Log().Error(ctx, "mqtt messageHandler error", err)
		}
	}); token.Wait() && token.Error() != nil {
		g.Log().Error(ctx, token.Error())
	}
}
func (s *sMqtt) subscribeAll(ctx context.Context) error {
	var list []*model.Device
	err := dao.Dev.Ctx(ctx).WhereNotNull(dao.Dev.Columns().DevSerial).Scan(&list)
	if err != nil {
		g.Log().Error(ctx, "Failed to get device list", err)
	}
	for _, dev := range list {
		s.SubscribeTopic(ctx, dev.DevSerial)
	}
	return err
}

func (s *sMqtt) messageHandler(ctx context.Context, c mqtt.Client, msg mqtt.Message) error {
	fmt.Printf("[%s] 主题: %s | 消息: %2x\n",
		time.Now().Format("15:04:05"),
		msg.Topic(),
		string(msg.Payload()))

	// BL-05修复：实现消息去重，基于消息内容和时间窗口
	payloadHash := fmt.Sprintf("%x", md5.Sum(msg.Payload()))
	dedupKey := fmt.Sprintf("mqtt:dedup:%s:%s", msg.Topic(), payloadHash)
	exists, _ := dao.Redis.IsExist(ctx, dedupKey)
	if exists {
		g.Log().Debug(ctx, "重复消息，忽略")
		return nil
	}
	dao.Redis.Set(ctx, dedupKey, "1", time.Minute*5)

	serial, err := s.fetchDeviceSerial(msg)
	if err != nil {
		g.Log().Error(ctx, "无法提取设备序列号", err)
		return err
	}
	isRepeat, Featurescode, DevUpdate, LogUpdate, err := service.Payload().PayloadHandler(ctx, serial, msg.Payload())
	if err != nil {
		g.Log().Error(ctx, "PayloadHandler error", err)
		return err
	}
	g.Log().Debug(ctx, "Featurescode", Featurescode)
	g.Log().Debug(ctx, "devUpdateItem", DevUpdate)
	g.Log().Debug(ctx, "logUpdateItem", LogUpdate)

	if isRepeat {
		g.Log().Debug(ctx, "重复上报，忽略")
		return nil
	}
	if DevUpdate != nil {
		_, err = dao.Dev.Ctx(ctx).OmitEmpty().Where(dao.Dev.Columns().DevSerial, DevUpdate.DevSerial).Data(g.Map{
			dao.Dev.Columns().DevStatus:    DevUpdate.DevStatus,
			dao.Dev.Columns().LatestOnline: DevUpdate.LatestOnline,
			dao.Dev.Columns().Sendmodel:    DevUpdate.Sendmodel,
			dao.Dev.Columns().Configdata:   DevUpdate.Configdata,
			dao.Dev.Columns().Baud:         DevUpdate.Baud,
		}).Update()
		// service.PayloadChan <- &v1.PayloadResponse{
		// 	// Featurescode: Featurescode,
		// 	DevUpdate: DevUpdate,
		// }
	}

	if err != nil {
		g.Log().Error(ctx, "更新设备状态失败", err)
	}
	// s.SendMessage(c, msg, msg.Topic())
	return nil
}

func (s *sMqtt) SendMessage(client mqtt.Client, msg mqtt.Message, topic string) {
	fmt.Printf("正在转发消息到主题: %s,消息：%02x\n", topic, string(msg.Payload()))

	token := client.Publish(topic, 1, false, msg.Payload())
	token.Wait()

	if token.Error() != nil {
		fmt.Printf("转发失败: %v\n", token.Error())
	} else {
		fmt.Printf("转发成功到主题: %s\n", topic)
	}
}

// fetchDeviceSerial 获取设备序列号
func (s *sMqtt) fetchDeviceSerial(msg mqtt.Message) (string, error) {
	devSerial, err := s.parseTopic(msg.Topic())
	return devSerial, err
}

// parseTopic 正则解析获取topic中的设备序列号
func (s *sMqtt) parseTopic(topic string) (string, error) {

	re := regexp.MustCompile(`/dtu/([0-9A-Fa-f]+)`)
	match := re.FindStringSubmatch(topic)
	if len(match) >= 2 {
		result := match[1]
		return result, nil
	}
	return "", gerror.New("无法解析设备序列号")
}
