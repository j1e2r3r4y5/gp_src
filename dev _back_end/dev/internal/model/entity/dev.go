// =================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// =================================================================================

package entity

import (
	"github.com/gogf/gf/v2/os/gtime"
)

// Dev is the golang structure for table dev.
type Dev struct {
	Id           int         `json:"id"           orm:"Id"           description:""`          //
	Devname      string      `json:"devname"      orm:"Devname"      description:""`          //
	DevSerial    string      `json:"devSerial"    orm:"DevSerial"    description:""`          //
	DevLocation  string      `json:"devLocation"  orm:"DevLocation"  description:""`          //
	DevStatus    int         `json:"devStatus"    orm:"DevStatus"    description:"0=离线,1=在线"` //
	LatestOnline *gtime.Time `json:"latestOnline" orm:"LatestOnline" description:""`          //
	Sendmodel    string      `json:"sendmodel"    orm:"sendmodel"    description:""`          //
	Configdata   string      `json:"configdata"   orm:"configdata"   description:""`          //
	Baud         string      `json:"baud"         orm:"Baud"         description:""`          //
	Changeflag   int         `json:"changeflag"   orm:"Changeflag"   description:""`          //
	Successflag  int         `json:"successflag"  orm:"successflag"  description:""`          //
}
