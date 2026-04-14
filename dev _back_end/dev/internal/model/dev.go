package model

type Device struct {
	ID           int    `json:"id"`
	Devname      string `json:"name"`
	DevSerial    string `json:"serial"`
	DevLocation  string `json:"location"`
	DevStatus    int    `json:"status"`
	LatestOnline string `json:"latest_online"`
	Sendmodel    string `json:"sendmodel"`
	Configdata   string `json:"configdata"`
	Baud         string `json:"baud"`
	Changeflag   int    `json:"chengeFlag"`
	SuccessFlag  int    `json:"successFlag"`
}
type AddDevice struct {
	Devname      string `json:"name"`
	DevSerial    string `json:"serial"`
	DevLocation  string `json:"location"`
	DevStatus    int    `json:"status"`
	LatestOnline string `json:"latest_online"`
	Sendmodel    string `json:"sendmodel"`
	Configdata   string `json:"configdata"`
	Baud         string `json:"baud"`
	Changeflag   int    `json:"chengeFlag"`
	SuccessFlag  int    `json:"successFlag"`
}
type Failed struct {
	DevName   string `json:"devName"`
	DevSerial string `json:"devSerial"`
	Reason    string `json:"reason"`
}
type FailedList struct {
	FailureList []*Failed `json:"failureList" dc:"返回创建失败的对应信息，包括序列号，名称，以及原因"`
}
type DeviceCreateListInput struct {
	Devices []*AddDevice `json:"devices"`
}
type ModifyDeviceInput struct {
	ID           int    `json:"id"`
	Devname      string `json:"name"`
	DevSerial    string `json:"serial"`
	DevLocation  string `json:"location"`
	DevStatus    int    `json:"status"`
	LatestOnline string `json:"latest_online"`
	Sendmodel    string `json:"sendmodel"`
	Configdata   string `json:"configdata"`
	Baud         string `json:"baud"`
}
type RemoveDeviceInput struct {
	Idlist []int `json:"idlist"` //设备ID列表
}
type LogUpdateItem struct {
	CheckData
	DevInfo
}
type CheckData struct {
	ChainAngle    float64 `json:"chainAngle"`
	ChainDist1    float64 `json:"chainDist1"`
	ImpactFactor1 float64 `json:"impactFactor1"`
	ChainDist2    float64 `json:"chainDist2"`
	ImpactFactor2 float64 `json:"impactFactor2"`
	ChainDist3    float64 `json:"chainDist3"`
	ImpactFactor3 float64 `json:"impactFactor3"`
}
type DevInfo struct {
	DevId            int     `json:"devId"         ` //
	CheckLocation    string  `json:"checkLocation" ` //
	CheckTemperature float64 `json:"checkTemperature" `
	CheckHumidity    float64 `json:"checkHumidity" `
	DevSerial        string  `json:"devSerial"     ` //
	DevName          string  `json:"devName"       ` //
}
