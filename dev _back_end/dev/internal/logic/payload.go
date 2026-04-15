package logic

import (
	"context"
	"dev/internal/dao"
	"dev/internal/model"
	"dev/internal/service"
	scanner "dev/utility"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
)

type sPayload struct {
}

func init() {
	service.RegisterPayload(Newpayload())
	InitInfluxClient()
}
func Newpayload() *sPayload {
	return &sPayload{}
}

var InfluxClient influxdb2.Client

func GetInfluxClient() influxdb2.Client {
	return InfluxClient
}
func InitInfluxClient() {
	cfg := g.Cfg()
	url := cfg.MustGet(context.Background(), "influxdb.url").String()
	token := cfg.MustGet(context.Background(), "influxdb.token").String()
	InfluxClient = influxdb2.NewClient(url, token)
}

// 解析设备上发的消息然后做处理,isRepeat表示是否是重复上报
// isHeartBeat表示是否是心跳包，DevUpdate是设备状态更新，LogUpdate是日志更新
// 返回值中DevUpdate和LogUpdate可能为nil，表示没有更新
func (s *sPayload) PayloadHandler(ctx context.Context, devSerial string, payload []byte) (isRepeat bool, Featurescode byte, DevUpdate *model.DevUpdateItem, LogUpdate *model.LogUpdateItem, err error) {
	g.Log().Debug(ctx, "devicePayload", hex.EncodeToString(payload))
	org := g.Cfg().MustGet(ctx, "influxdb.org").String()
	bucket := g.Cfg().MustGet(ctx, "influxdb.bucket").String()
	scanner := scanner.New(payload)
	// 记录原始上报数据
	// rawPayload := &model.Payload{
	// 	DevSerial: devSerial,
	// 	Code:      payload,
	// }
	// 判断包类型，心跳包还是数据包
	funcCode, err := scanner.Next(1)
	if err != nil {
		return
	}
	funcCodeStr := s.BytesToString(funcCode)
	switch funcCodeStr {
	case "00": // 心跳包
		Featurescode = 0x00
		devStatus := 1
		g.Log().Debug(ctx, "心跳包？", Featurescode)
		DevUpdate = &model.DevUpdateItem{
			DevSerial:    devSerial,
			DevStatus:    devStatus,
			LatestOnline: gtime.Now(),
		}
		// BL-01修复：心跳包不再写入InfluxDB，减少数据膨胀
		// 心跳状态已保存在MySQL的dev表中，InfluxDB主要用于存储设备采集的时序数据
		// WriteDevUpdateToInflux(ctx, org, bucket, DevUpdate, Featurescode)
		fmt.Printf("心跳包: %s, 状态: %d\n", devSerial, devStatus)
		return false, Featurescode, DevUpdate, nil, nil
	case "01": // 上发模组配置
		Featurescode = 0x01
		sendmodel, err := scanner.Next(1)
		if err != nil {
			g.Log().Error(ctx, "解析模组配置失败", err)
			return false, 0, nil, nil, err
		}
		configdata, err := scanner.Next(2)
		if err != nil {
			g.Log().Error(ctx, "解析模组配置失败", err)
			return false, 0, nil, nil, err
		}
		baud, err := scanner.Next(1)
		if err != nil {
			g.Log().Error(ctx, "解析波特率失败", err)
			return false, 0, nil, nil, err
		}
		DevUpdate = &model.DevUpdateItem{
			Featurescode: fmt.Sprintf("%d", Featurescode),
			DevSerial:    devSerial,
			DevStatus:    1,
			LatestOnline: gtime.Now(),
			Sendmodel:    s.BytesToString(sendmodel),
			Configdata:   s.BytesToString(configdata),
			Baud:         s.BytesToString(baud),
		}
		g.Log().Info(ctx, "查询设备配置")
		_, err = dao.Dev.Ctx(ctx).Where(dao.Dev.Columns().DevSerial, DevUpdate.DevSerial).Data(g.Map{
			"Sendmodel":  DevUpdate.Sendmodel,
			"Configdata": DevUpdate.Configdata,
			"Baud":       DevUpdate.Baud,
		}).Update()
		if err != nil {
			g.Log().Error(ctx, "更新设备配置失败", err)
		}
		WriteDevUpdateToInflux(ctx, org, bucket, DevUpdate, Featurescode)
		return false, Featurescode, DevUpdate, nil, nil
	case "02": // 上发设备配置
		Featurescode = 0x02
		Success, err := scanner.Next(1)
		if err != nil {
			g.Log().Error(ctx, "解析设备配置失败", err)
			return false, 0, nil, nil, err
		}
		DevUpdate = &model.DevUpdateItem{
			Featurescode: fmt.Sprintf("%d", Featurescode),
			DevSerial:    devSerial,
			DevStatus:    1,
			LatestOnline: gtime.Now(),
			Success:      s.BytesToString(Success),
		}

		WriteDevUpdateToInflux(ctx, org, bucket, DevUpdate, Featurescode)
		switch {
		case Success[0] == 0x00: // 成功
			g.Log().Info(ctx, "设备配置下发成功")
		case Success[0] == 0x01:
			g.Log().Error(ctx, "打开存储失败")
		case Success[0] == 0x02:
			g.Log().Error(ctx, "写入存储失败")
		case Success[0] == 0x03:
			g.Log().Error(ctx, "触发模式错误")
		case Success[0] == 0x04:
			g.Log().Error(ctx, "数据接收长度错误")
		case Success[0] == 0x05:
			g.Log().Error(ctx, "波特率配置错误")
		}
		return false, Featurescode, DevUpdate, nil, nil
	case "03": // 上发数据配置
		Featurescode = 0x03
		lenBytes, err := scanner.Next(2)
		if err != nil {
			g.Log().Error(ctx, "解析数据区总长度失败", err)
			return false, 0, nil, nil, err
		}
		if len(lenBytes) < 2 {
			g.Log().Error(ctx, "数据区长度字节不足")
			return false, 0, nil, nil, fmt.Errorf("数据区长度字节不足")
		}
		totalLen := int(lenBytes[0])<<8 | int(lenBytes[1])
		g.Log().Debug(ctx, "数据区总长度", totalLen)

		if totalLen < 3 {
			g.Log().Error(ctx, "数据区总长度非法，太小")
			return false, 0, nil, nil, fmt.Errorf("数据区总长度非法")
		}
		dataItemBytes := totalLen - 3
		readBytes := 0
		index := 0
		for readBytes+6 <= dataItemBytes {
			slaveAddr, err := scanner.Next(1)
			if err != nil || len(slaveAddr) < 1 {
				g.Log().Error(ctx, "解析从站地址失败", err)
				break
			}
			dataType, err := scanner.Next(1)
			if err != nil || len(dataType) < 1 {
				g.Log().Error(ctx, "解析数据类型失败", err)
				break
			}
			dataAddr, err := scanner.Next(2)
			if err != nil || len(dataAddr) < 2 {
				g.Log().Error(ctx, "解析数据地址失败", err)
				break
			}
			dataLen, err := scanner.Next(2)
			if err != nil || len(dataLen) < 2 {
				g.Log().Error(ctx, "解析数据长度失败", err)
				break
			}
			readBytes += 6

			g.Log().Debug(ctx, "数据配置项", index,
				"从站地址:", slaveAddr[0],
				"类型:", dataType[0],
				"数据地址:", int(dataAddr[0])<<8|int(dataAddr[1]),
				"数据长度:", int(dataLen[0])<<8|int(dataLen[1]),
			)
			index++
		}
		DevUpdate = &model.DevUpdateItem{
			Featurescode: fmt.Sprintf("%d", Featurescode),
			DevSerial:    devSerial,
			DevStatus:    1,
			LatestOnline: gtime.Now(),
		}
		return false, Featurescode, DevUpdate, nil, nil
	case "04": // 上发设备日志
		Featurescode = 0x04
		Success, err := scanner.Next(1)
		if err != nil {
			g.Log().Error(ctx, "解析设备配置失败", err)
			return false, 0, nil, nil, err
		}
		DevUpdate = &model.DevUpdateItem{
			Featurescode: fmt.Sprintf("%d", Featurescode),
			DevSerial:    devSerial,
			DevStatus:    1,
			LatestOnline: gtime.Now(),
			Success:      s.BytesToString(Success),
		}
		WriteDevUpdateToInflux(ctx, org, bucket, DevUpdate, Featurescode)
		switch {
		case Success[0] == 0x00: // 成功
			g.Log().Info(ctx, "设备配置下发成功")
			_, err = dao.Dev.Ctx(ctx).Where(dao.Dev.Columns().DevSerial, devSerial).Data(g.Map{
				"Changeflag": 0,
			}).Update()
			if err != nil {
				g.Log().Error(ctx, "更新设备标志失败", err)
			}
			devId, err := dao.Dev.Ctx(ctx).Where(dao.Dev.Columns().DevSerial, devSerial).Value(dao.Dev.Columns().Id)
			if err != nil {
				g.Log().Error(ctx, "获取设备ID失败", err)
			}
			// 清空缓存表该设备的所有变量
			dao.Caching.Ctx(ctx).Where(dao.Caching.Columns().DevID, devId).Data().Delete()
			// 获取该设备的所有变量存到机构体里
			var variableList []*model.Variables
			err = dao.Variables.Ctx(ctx).Where(dao.Variables.Columns().DevID, devId).Scan(&variableList)
			if err != nil {
				g.Log().Error(ctx, "获取变量列表失败", err)
				return false, Featurescode, DevUpdate, nil, err
			}
			// 遍历用变量表更新缓存表
			for _, v := range variableList {
				_, err := dao.Caching.Ctx(ctx).Data(g.Map{
					"DevID":         v.DevID,
					"VarName":       v.VarName,
					"DataType":      v.DataType,
					"ModbusType":    v.ModbusType,
					"ModbusDevice":  v.ModbusDevice,
					"ModbusAddr":    v.ModbusAddr,
					"DataLen":       v.DataLen,
					"StringLen":     v.StringLen,
					"DecimalDigits": v.DecimalDigits,
				}).Save()
				if err != nil {
					g.Log().Error(ctx, "写入缓存表失败", err)
				}
			}
		case Success[0] == 0x01:
			g.Log().Error(ctx, "打开存储失败")
		case Success[0] == 0x02:
			g.Log().Error(ctx, "写入存储失败")
		case Success[0] == 0x03:
			g.Log().Error(ctx, "触发模式错误")
		case Success[0] == 0x04:
			g.Log().Error(ctx, "数据接收长度错误")
		case Success[0] == 0x05:
			g.Log().Error(ctx, "波特率配置错误")
		}
		// DevUpdate = &model.DevUpdateItem{
		// 	Featurescode: fmt.Sprintf("%d", Featurescode),
		// 	DevSerial:    devSerial,
		// 	DevStatus:    1,
		// 	LatestOnline: gtime.Now(),
		// 	Success:      s.BytesToString(Success),
		// }
		return false, Featurescode, DevUpdate, nil, nil
	case "05": // 上发数据
		Featurescode = 0x05
		lenBytes, err := scanner.Next(2)
		if err != nil {
			g.Log().Error(ctx, "解析数据区总长度失败", err)
			return false, 0, nil, nil, err
		}
		if len(lenBytes) < 2 {
			g.Log().Error(ctx, "数据区长度字节不足")
			return false, 0, nil, nil, fmt.Errorf("数据区长度字节不足")
		}
		totalLen := int(lenBytes[0])<<8 | int(lenBytes[1])
		g.Log().Debug(ctx, "数据区总长度", totalLen)

		if totalLen < 3 {
			g.Log().Error(ctx, "数据区总长度非法，太小")
			return false, 0, nil, nil, fmt.Errorf("数据区总长度非法")
		}
		// BL-02修复：添加数据长度边界检查
		dataItemBytes := totalLen - 3
		readBytes := 0
		for readBytes+6 <= dataItemBytes {
			slaveAddr, err := scanner.Next(1)
			if err != nil || len(slaveAddr) < 1 {
				g.Log().Error(ctx, "解析从站地址失败", err)
				break
			}
			ModbusType, err := scanner.Next(1)
			if err != nil || len(ModbusType) < 1 {
				g.Log().Error(ctx, "解析Modbus类型失败", err)
				break
			}
			dataAddr, err := scanner.Next(2)
			if err != nil || len(dataAddr) < 2 {
				g.Log().Error(ctx, "解析数据地址失败", err)
				break
			}
			dataLen, err := scanner.Next(2)
			if err != nil || len(dataLen) < 2 {
				g.Log().Error(ctx, "解析数据长度失败", err)
				break
			}
			length := int(dataLen[0])<<8 | int(dataLen[1])

			var valueLen int
			switch ModbusType[0] {
			case 1, 2:
				valueLen = (length + 7) / 8
			case 3, 4:
				valueLen = length * 2
			default:
				g.Log().Error(ctx, "未知的Modbus类型", ModbusType[0])
				break
			}

			if valueLen <= 0 || valueLen > 256 {
				g.Log().Error(ctx, "计算的数据值长度非法", valueLen)
				break
			}

			dataValue, err := scanner.Next(valueLen)
			if err != nil || len(dataValue) < valueLen {
				g.Log().Error(ctx, "解析数据值失败", err)
				break
			}
			readBytes += 6 + valueLen

			baseAddr := int(dataAddr[0])<<8 | int(dataAddr[1])

			if ModbusType[0] == 1 || ModbusType[0] == 2 {
				if length > 2048 {
					g.Log().Error(ctx, "线圈数量过大", length)
					break
				}
				for i := 0; i < length; i++ {
					byteIndex := i / 8
					bitOffset := i % 8
					if byteIndex < len(dataValue) {
						bitVal := (dataValue[byteIndex] >> bitOffset) & 0x01
						DataItem := &model.DataItem{
							DevSerial:  devSerial,
							SlaveAddr:  int(slaveAddr[0]),
							ModbusType: int(ModbusType[0]),
							DataAddr:   baseAddr + i,
							DataLeng:   1,
							DataValue:  fmt.Sprintf("%d", bitVal),
						}
						WritdataToInflux(ctx, org, bucket, DataItem, Featurescode)
					}
				}
			} else if ModbusType[0] == 3 || ModbusType[0] == 4 {
				if length > 128 {
					g.Log().Error(ctx, "寄存器数量过大", length)
					break
				}
				for i := 0; i < length; i++ {
					offset := i * 2
					if offset+1 < len(dataValue) {
						regVal := int(dataValue[offset])<<8 | int(dataValue[offset+1])
						DataItem := &model.DataItem{
							DevSerial:  devSerial,
							SlaveAddr:  int(slaveAddr[0]),
							ModbusType: int(ModbusType[0]),
							DataAddr:   baseAddr + i,
							DataLeng:   1,
							DataValue:  fmt.Sprintf("%d", regVal),
						}
						WritdataToInflux(ctx, org, bucket, DataItem, Featurescode)
					}
				}
			}
		}
		DevUpdate = &model.DevUpdateItem{
			Featurescode: fmt.Sprintf("%d", Featurescode),
			DevSerial:    devSerial,
			DevStatus:    1,
			LatestOnline: gtime.Now(),
		}
		return false, Featurescode, DevUpdate, nil, nil
	}
	return false, 0, nil, nil, fmt.Errorf("未处理: %s", funcCodeStr)
}

// 下发功能码
func (s *sPayload) DownPayloadHandler(ctx context.Context, topic string, code []byte) (err error) {
	g.Log().Info(ctx, "下发功能码", topic, "code:", code)
	scanner := scanner.New(code)
	funcCode, err := scanner.Next(1)
	funcCodeStr := s.BytesToString(funcCode)
	if err != nil {
		g.Log().Error(ctx, "解析下发功能码失败", err)
		return nil
	}
	switch funcCodeStr {
	case "01": // 下发模组配置
		g.Log().Info(ctx, "下发功能码", topic, "code:", funcCodeStr)
		mqttClient.Publish(topic, 0, false, code)
	case "02": // 下发设备配置
		g.Log().Info(ctx, "下发设备配置")
		mqttClient.Publish(topic, 0, false, code)
		return nil
	case "03": // 下发设备状态
		g.Log().Info(ctx, "下发设备状态")
		mqttClient.Publish(topic, 0, false, code)
	case "04": // 下发设备日志
		g.Log().Info(ctx, "下发设备日志")
		if len(code) < 2 {
			g.Log().Error(ctx, "下发的日志内容为空")
			return fmt.Errorf("下发的日志内容为空")
		}
		logContent := s.BytesToString([]byte{code[1]})
		g.Log().Debug(ctx, "设备日志内容", logContent)
		mqttClient.Publish(topic, 0, false, code)
		// BL-06修复：topic解析添加越界检查
		devSerial, err := s.parseTopicSafe(topic)
		if err != nil {
			g.Log().Error(ctx, "解析设备序列号失败", err)
			return err
		}
		_, err = dao.Dev.Ctx(ctx).Where(dao.Dev.Columns().DevSerial, devSerial).Data(g.Map{
			"SuccessFlag": 1,
		}).Update()
		if err != nil {
			g.Log().Error(ctx, "成功标志更改失败", err)
		}
	case "06": // 下发设备重启命令
		g.Log().Info(ctx, "下发设备重启命令")
		mqttClient.Publish(topic, 0, false, code)
	}
	return
}

// BL-06修复：安全的topic解析，使用正则表达式
func (s *sPayload) parseTopicSafe(topic string) (string, error) {
	if topic == "" {
		return "", fmt.Errorf("topic为空")
	}
	re := regexp.MustCompile(`/dtu/([0-9A-Fa-f]+)`)
	match := re.FindStringSubmatch(topic)
	if len(match) < 2 {
		return "", fmt.Errorf("无法解析设备序列号: %s", topic)
	}
	return match[1], nil
}

// BytesToUpperCaseString bytes转大写string
func (s *sPayload) BytesToString(data []byte) string {
	hexData := hex.EncodeToString(data)
	str := strings.ToUpper(hexData)
	return str
}

// IsHeartBeat 判断是否为心跳包，并返回设备状态
// BL-07修复：修正返回值逻辑，返回(bool, byte)表示(是否为心跳, 功能码)
func (s *sPayload) IsHeartBeat(data []byte) (bool, byte) {
	if len(data) < 1 {
		return false, 0
	}
	if s.BytesToString(data) == "00" {
		return true, 0x00
	}
	return false, data[0]
}

// HexStringToBytes 十六进制字符串转[]byte
func (s *sPayload) HexStringToBytes(hexStr string) ([]byte, error) {
	return hex.DecodeString(hexStr)
}

// 最早版本时序存功能码
func WriteDevUpdateToInflux(ctx context.Context, org, bucket string, devUpdate *model.DevUpdateItem, featuresCode byte) error {
	writeAPI := InfluxClient.WriteAPIBlocking(org, bucket)
	tags := map[string]string{
		"dev_serial":    devUpdate.DevSerial,
		"features_code": fmt.Sprintf("%d", featuresCode),
	}
	var fields map[string]interface{}
	switch featuresCode {
	case 0x00:
		fields = map[string]interface{}{
			"dev_status":    devUpdate.DevStatus,
			"latest_online": devUpdate.LatestOnline.String(),
			"send_model":    devUpdate.Sendmodel,
			"config_data":   devUpdate.Configdata,
			"baud":          devUpdate.Baud,
			"success":       devUpdate.Success,
		}
	case 0x01:
		fields = map[string]interface{}{
			"send_model":    devUpdate.Sendmodel,
			"config_data":   devUpdate.Configdata,
			"baud":          devUpdate.Baud,
			"latest_online": devUpdate.LatestOnline.String(),
			"dev_status":    devUpdate.DevStatus,
			"success":       devUpdate.Success,
		}
	case 0x02:
		fields = map[string]interface{}{
			"success":       devUpdate.Success,
			"dev_status":    devUpdate.DevStatus,
			"send_model":    devUpdate.Sendmodel,
			"config_data":   devUpdate.Configdata,
			"latest_online": devUpdate.LatestOnline.String(),
		}
	default:
		fields = map[string]interface{}{
			"dev_status": devUpdate.DevStatus,
		}
	}

	point := write.NewPoint("Featurescode", tags, fields, time.Now())
	// BL-10修复：添加错误处理和重试机制
	err := writeAPI.WritePoint(ctx, point)
	if err != nil {
		g.Log().Error(ctx, "写入InfluxDB失败，重试中...", err)
		time.Sleep(100 * time.Millisecond)
		err = writeAPI.WritePoint(ctx, point)
		if err != nil {
			g.Log().Error(ctx, "写入InfluxDB重试失败", err)
			return fmt.Errorf("写入InfluxDB失败: %w", err)
		}
	}
	return nil
}

// BL-10修复：WritdataToInflux添加完整的错误处理机制
func WritdataToInflux(ctx context.Context, org, bucket string, DataItem *model.DataItem, featuresCode byte) error {
	writeAPI := InfluxClient.WriteAPIBlocking(org, bucket)
	tags := map[string]string{
		"dev_serial": fmt.Sprintf(DataItem.DevSerial),
		"slave_addr": fmt.Sprintf("%d", DataItem.SlaveAddr),
		"data_type":  fmt.Sprintf("%d", DataItem.ModbusType),
		"data_addr":  fmt.Sprintf("%d", DataItem.DataAddr),
	}
	fields := map[string]interface{}{
		"datavalue": DataItem.DataValue,
	}
	point := write.NewPoint("DataItem", tags, fields, time.Now())

	// BL-10修复：添加错误处理和重试机制
	err := writeAPI.WritePoint(ctx, point)
	if err != nil {
		g.Log().Error(ctx, "写入InfluxDB DataItem失败，重试中...", err)
		time.Sleep(100 * time.Millisecond)
		err = writeAPI.WritePoint(ctx, point)
		if err != nil {
			g.Log().Error(ctx, "写入InfluxDB DataItem重试失败", err)
			return fmt.Errorf("写入InfluxDB DataItem失败: %w", err)
		}
	}
	g.Log().Debug(ctx, "写入时序数据库成功", DataItem)
	return nil
}

// // 查询数据表
// func (s *sPayload) QueryLastDataItemFromInflux(ctx context.Context, org, bucket string, DataItem *model.DataItem) error {
// 	queryAPI := InfluxClient.QueryAPI(org)
// 	query := fmt.Sprintf(`
//         from(bucket: "%s")
//         |> range(start: -1m)
//         |> filter(fn: (r) => r._measurement == "DataItem"
//             and r.dev_serial == "%s"
//             and r.slave_addr == "%d"
//             and r.data_type == "%d"
//             and r.data_addr == "%d")
//         |> sort(columns: ["_time"], desc: true)
//         |> limit(n:1)
//     `, bucket, DataItem.DevSerial, DataItem.SlaveAddr, DataItem.DataType, DataItem.DataAddr)

// 	result, err := queryAPI.Query(ctx, query)
// 	if err != nil {
// 		g.Log().Error(ctx, "查询时序数据库失败", err)
// 		return err
// 	}
// 	for result.Next() {
// 		g.Log().Info(ctx, "查询到的数据", result.Record().Values())
// 	}
// 	if result.Err() != nil {
// 		g.Log().Error(ctx, "查询结果错误", result.Err())
// 		return result.Err()
// 	}
// 	return nil
// }

// func QueryDevupdata(ctx context.Context, org, bucket string, devSerial string, featuresCode byte) (err error) {
// 	queryAPI := InfluxClient.QueryAPI(org)
// 	query := fmt.Sprintf(`
// 	    from(bucket: "%s")
// 	    |> range(start: -1m)
// 	    |> filter(fn: (r) => r._measurement == "Featurescode"
// 	        and r.dev_serial == "%s"
// 	        and r.features_code == "%s")
// 	    |> sort(columns: ["_time"], desc: true)
// 	    |> limit(n:1)
// 	`, bucket, devSerial, fmt.Sprintf("%d", featuresCode))

// 	result, err := queryAPI.Query(ctx, query)
// 	if err != nil {
// 		g.Log().Error(ctx, "查询时序数据库失败", err)
// 		return
// 	}
// 	for result.Next() {
// 		g.Log().Info(ctx, "查询到的数据", result.Record().Values())
// 	}
// 	if result.Err() != nil {
// 		g.Log().Error(ctx, "查询结果错误", result.Err())
// 		return
// 	}
// 	return
// }
