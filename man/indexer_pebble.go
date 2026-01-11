package man

import (
	"fmt"
	"strconv"

	"manindexer/common"
	"manindexer/pebblestore"

	"manindexer/pin"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bytedance/sonic"
)

type PebbleData struct {
	Database *pebblestore.Database
}

func (pd *PebbleData) Init(shardNum int) (err error) {
	dbPath := filepath.Join("./man_base_data_pebble")
	err = os.MkdirAll(dbPath, 0755)
	if err != nil {
		return
	}
	pd.Database, err = pebblestore.NewDataBase(dbPath, shardNum)
	return
}

func (pd *PebbleData) DoIndexerRun(chainName string, height int64, reIndex bool) (err error) {
	if !reIndex {
		MaxHeight[chainName] = height
	}
	txInList := &[]string{}
	pinList := &[]*pin.PinInscription{}
	pinList, txInList, _ = IndexerAdapter[chainName].CatchPins(height)
	//保存PIN数据
	if len(*pinList) > 0 {
		//fmt.Println("SetAllPins start height:", height, " Num:", len(pinList))
		pd.Database.SetAllPins(height, *pinList, 20000)
		tmp := (*pinList)[0]
		blockKey := fmt.Sprintf("blocktime_%s_%d", chainName, height)
		pd.Database.CountSet(blockKey, tmp.Timestamp)
	}

	//处理modify/revoke操作
	handlePathAndOperation(pinList)
	//处理metaid信息更新，注意要先处理path(modify)操作
	handleMetaIdInfo(pinList)
	//处理通知（区块确认后）
	for _, pinNode := range *pinList {
		handNotifcation(pinNode)
	}
	//处理转移操作
	if common.Config.Sync.IsFullNode {
		pd.handleTransfer(chainName, *txInList, height)
	}

	pinList = nil
	txInList = nil
	if FirstCompleted {
		DeleteMempoolData(height, chainName)
	}
	return
}

// Set PinId from block data
func (pd *PebbleData) SetPinIdList(chainName string, height int64) (err error) {
	pins, _, _ := IndexerAdapter[chainName].CatchPins(height)
	var pinIdList []string
	if len(*pins) <= 0 {
		return
	}
	for _, pinNode := range *pins {
		pinIdList = append(pinIdList, pinNode.Id)
	}
	blockTime := (*pins)[0].Timestamp
	publicKeyStr := common.ConcatBytesOptimized([]string{fmt.Sprintf("%010d", blockTime), "&", chainName, "&", fmt.Sprintf("%010d", height)}, "")
	pd.Database.InsertBlockTxs(publicKeyStr, strings.Join(pinIdList, ","))
	pinIdList = nil
	fmt.Println(">> SetPinIdList done for height:", chainName, height)
	return
}

func (pd *PebbleData) handleTransfer(chainName string, outputList []string, blockHeight int64) {
	defer func() {
		outputList = outputList[:0]
	}()
	transferCheck, err := pd.Database.GetPinListByIdList(outputList, 1000, true)
	if err == nil && len(transferCheck) > 0 {
		idMap := make(map[string]string)
		for _, t := range transferCheck {
			idMap[t.Output] = t.Address
		}
		transferMap := IndexerAdapter[chainName].CatchTransfer(idMap)
		pd.Database.UpdateTransferPin(transferMap)
		var transferHistoryList []*pin.PinTransferHistory
		transferTime := time.Now().Unix()
		for pinid, info := range transferMap {
			transferHistoryList = append(transferHistoryList, &pin.PinTransferHistory{
				PinId:          strings.ReplaceAll(pinid, ":", "i"),
				TransferTime:   transferTime,
				TransferHeight: blockHeight,
				TransferTx:     info.Location,
				ChainName:      chainName,
				FromAddress:    info.FromAddress,
				ToAddress:      info.Address,
			})
		}
		pd.Database.BatchInsertTransferPins(&transferHistoryList)
		idMap = nil
		transferMap = nil
		transferHistoryList = transferHistoryList[:0]
		transferHistoryList = nil
	}
}

func (pd *PebbleData) GetPinById(pinid string) (pinNode pin.PinInscription, err error) {
	result, err := pd.Database.GetPinByKey(pinid)
	if err != nil {
		return
	}
	err = sonic.Unmarshal(result, &pinNode)
	return
}

func (pd *PebbleData) GetPinByMetaIdAndPathPageList(metaid, path string, cursor string, size int64) (pinList []*pin.PinInscription, total int64, nextCursor string, err error) {
	// AddressDB: key是metaid&path&blockTime&chainName&height&pinId
	//c8e01f2e5a8aa4558290f72ced9c8acd474800cd1eb9ab33030b626796586838&929493665e09c2d991b894f15417fd4811357e8cda1360b676ff0f7e9f155c50&1761510013&btc&0000000440
	db := pd.Database.AddressDB
	pathHash := common.GetMetaIdByAddress(path)
	prefix := metaid + "&" + pathHash + "&"
	prefixBytes := []byte(prefix)
	it, err := db.NewIter(nil)
	if err != nil {
		return nil, 0, "", err
	}
	defer it.Close()

	// 从统计表获取总数
	cntKey := metaid + "_" + pathHash + "_count"
	val, closer, err1 := pd.Database.CountDB.Get([]byte(cntKey))
	if err1 == nil {
		total, _ = strconv.ParseInt(string(val), 10, 64)
		closer.Close()
	}

	// 判断是否为首页查询
	isFirstPage := cursor == "" || cursor == "0"

	// 定位迭代器
	if !isFirstPage {
		// 从游标位置继续
		it.SeekLT([]byte(cursor))
		if !it.Valid() {
			return pinList, total, "", nil
		}
	} else {
		// 首页：从最新的开始
		it.SeekLT(append(prefixBytes, 0xff))
		if !it.Valid() {
			// 尝试从头开始找到最后一个
			it.SeekGE(prefixBytes)
			if !it.Valid() {
				return pinList, total, "", nil
			}
			for it.Next() {
				key := it.Key()
				if len(key) < len(prefixBytes) || string(key[:len(prefixBytes)]) != prefix {
					it.Prev()
					break
				}
			}
			if !it.Valid() {
				it.Last()
			}
		}
	}

	// 收集 pinId（逆序遍历）
	pinIds := make([]string, 0, size)
	keys := make([]string, 0, size)
	pinIdSet := make(map[string]bool) // 用于去重
	var count int64

	for it.Valid() && count < size {
		key := it.Key()
		if len(key) < len(prefixBytes) || string(key[:len(prefixBytes)]) != prefix {
			break
		}

		keyStr := string(key)

		// 提取 pinId（第6个字段）
		sepCount := 0
		startIdx := -1
		for i := 0; i < len(key); i++ {
			if key[i] == '&' {
				sepCount++
				if sepCount == 5 {
					startIdx = i + 1
					break
				}
			}
		}
		if startIdx > 0 && startIdx < len(key) {
			pinId := string(key[startIdx:])
			// 检查是否已经存在，避免重复
			if !pinIdSet[pinId] {
				pinIdSet[pinId] = true
				pinIds = append(pinIds, pinId)
				keys = append(keys, keyStr)
				count++
			}
		}
		it.Prev()
	}

	// 批量查询 pin 数据
	if len(pinIds) > 0 {
		pinDataMap := pd.Database.BatchGetPinByKeys(pinIds, false)
		for _, pinId := range pinIds {
			if data, ok := pinDataMap[pinId]; ok {
				var pinNode pin.PinInscription
				if err := sonic.Unmarshal(data, &pinNode); err == nil {
					pinNode.ContentSummary = string(pinNode.ContentBody)
					pinNode.ContentBody = []byte{}
					pinList = append(pinList, &pinNode)
				}
			}
		}
		// 设置下一页游标
		if len(keys) > 0 {
			nextCursor = keys[len(keys)-1]
		}
	}

	return pinList, total, nextCursor, err
}
func (pd *PebbleData) GetAllPinByPathPageList(path string, cursor string, size int64) (pinList []*pin.PinInscription, total int64, nextCursor string, err error) {
	//key是 path&blockTime&chainName&height&pinId
	//5d7b6c2f61327986929fac2888fc1de467248d99bf80edc23cf7f45d87394068&1757984310&btc&0000000432&58be6bc384edc7e2709462bcee4ffa2e265e086f753456db22368cfe0162fdb8i0
	prefix := common.GetMetaIdByAddress(path) + "&"
	prefixBytes := []byte(prefix)
	db := pd.Database.PathPinDB
	it, err := db.NewIter(nil)
	if err != nil {
		return nil, 0, "", err
	}
	defer it.Close()

	// 第一步：从统计表获取总数
	// 注意：统计表的 key 使用的是 hash(path)，与 PathPinDB 的 prefix 一致
	pathHash := common.GetMetaIdByAddress(path)
	cntKey := pathHash + "_count"
	val, closer, err1 := pd.Database.CountDB.Get([]byte(cntKey))
	if err1 == nil {
		total, _ = strconv.ParseInt(string(val), 10, 64)
		closer.Close()
	}

	// 判断是否为首页查询（cursor 为空或为 "0"）
	isFirstPage := cursor == "" || cursor == "0"

	// 如果有有效游标，使用游标定位
	if !isFirstPage {
		// 从游标的下一个位置开始（逆序继续）
		it.SeekLT([]byte(cursor))
		if !it.Valid() {
			return pinList, total, "", nil
		}
	} else {
		// 首页：直接从最新的开始
		it.SeekLT(append(prefixBytes, 0xff))
		if !it.Valid() {
			// 没有数据，尝试从头开始
			it.SeekGE(prefixBytes)
			if !it.Valid() {
				return pinList, total, "", nil
			}
			// 找到最后一个
			for it.Next() {
				key := it.Key()
				if len(key) < len(prefixBytes) || string(key[:len(prefixBytes)]) != prefix {
					it.Prev()
					break
				}
			}
			if !it.Valid() {
				it.Last()
			}
		}
	}

	// 第二步：收集所有需要的 pinId（逆序收集，保持时间倒序）
	pinIds := make([]string, 0, size)
	keys := make([]string, 0, size)
	pinIdSet := make(map[string]bool) // 用于去重
	var count int64

	// 逆序收集（使用 Prev）
	for it.Valid() && count < size {
		key := it.Key()
		if len(key) < len(prefixBytes) || string(key[:len(prefixBytes)]) != prefix {
			break
		}

		keyStr := string(key)

		// 直接从字节切片中提取 pinId
		sepCount := 0
		startIdx := -1
		for i := 0; i < len(key); i++ {
			if key[i] == '&' {
				sepCount++
				if sepCount == 4 {
					startIdx = i + 1
					break
				}
			}
		}
		if startIdx > 0 && startIdx < len(key) {
			pinId := string(key[startIdx:])
			// 检查是否已经存在，避免重复
			if !pinIdSet[pinId] {
				pinIdSet[pinId] = true
				pinIds = append(pinIds, pinId)
				keys = append(keys, keyStr)
				count++
			}
		}
		it.Prev()
	}

	// 第三步：批量查询所有 pin 数据
	if len(pinIds) > 0 {
		pinDataMap := pd.Database.BatchGetPinByKeys(pinIds, false)
		// 按照 pinIds 的顺序组装结果（已经是时间倒序）
		for _, pinId := range pinIds {
			if data, ok := pinDataMap[pinId]; ok {
				var pinNode pin.PinInscription
				if err := sonic.Unmarshal(data, &pinNode); err == nil {
					// 在这里设置 ContentSummary 并清空 ContentBody，减少 API 层的处理
					pinNode.ContentSummary = string(pinNode.ContentBody)
					pinNode.ContentBody = []byte{}
					pinList = append(pinList, &pinNode)
				}
			}
		}
		// 设置下一页游标为最后一个 key（用于继续逆序遍历）
		if len(keys) > 0 {
			nextCursor = keys[len(keys)-1]
		}
	}

	return pinList, total, nextCursor, err
}
