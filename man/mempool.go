package man

import (
	"manindexer/common"
	"manindexer/pin"
	"strings"
	"time"
)

func handleMempoolPin(pinNode *pin.PinInscription) {
	if pinNode.Operation == "modify" || pinNode.Operation == "revoke" {
		pinNode.OriginalPath = GetModifyPath(pinNode.Path)
		pinNode.OriginalId = strings.Replace(pinNode.Path, "@", "", -1)
		handlePathAndOperation(&[]*pin.PinInscription{pinNode})
	}
	pinNode.Timestamp = time.Now().Unix()
	pinNode.Number = -1
	pinNode.ContentTypeDetect = common.DetectContentType(&pinNode.ContentBody)
	//增加到pebble数据库
	PebbleStore.Database.SetMempool(pinNode)
	//增加PIN相关数据
	PebbleStore.Database.SetAllPins(-1, []*pin.PinInscription{pinNode}, 20000)
	//通知
	handNotifcation(pinNode)
}
