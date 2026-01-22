package man

import (
	"encoding/json"
	"fmt"
	"log"
	"manindexer/mrc20"
	"manindexer/pin"
	"strconv"
	"strings"
	"sync"

	bsvwire "github.com/bitcoinsv/bsvd/wire"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/txscript"
	btcwire "github.com/btcsuite/btcd/wire"
	"github.com/shopspring/decimal"
)

// Doge 链交易缓存（因为 Doge 节点没有 txindex）
// 每次处理新区块时会被覆盖，只缓存当前区块的交易
var dogeTxCache = make(map[string]*btcutil.Tx)
var dogeTxCacheMutex sync.RWMutex

// SetDogeTxCache 设置 Doge 区块交易缓存（由 dogecoin indexer 在 CatchPins 时调用）
func SetDogeTxCache(txMap map[string]*btcutil.Tx) {
	dogeTxCacheMutex.Lock()
	defer dogeTxCacheMutex.Unlock()
	dogeTxCache = txMap
}

// GetTransactionWithCache 获取交易，Doge 链优先从缓存获取
func GetTransactionWithCache(chainName string, txid string) (*btcutil.Tx, error) {
	// 只有 Doge 链使用缓存
	if chainName == "doge" {
		dogeTxCacheMutex.RLock()
		if tx, ok := dogeTxCache[txid]; ok {
			dogeTxCacheMutex.RUnlock()
			return tx, nil
		}
		dogeTxCacheMutex.RUnlock()
	}

	// 从 RPC 获取
	tx, err := ChainAdapter[chainName].GetTransaction(txid)
	if err != nil {
		return nil, err
	}
	return tx.(*btcutil.Tx), nil
}

func Mrc20Handle(chainName string, height int64, mrc20List []*pin.PinInscription, mrc20TransferPinTx map[string]struct{}, txInList []string, isMempool bool) {
	log.Printf("[MRC20] Mrc20Handle: chain=%s, height=%d, mrc20List=%d, txInList=%d", chainName, height, len(mrc20List), len(txInList))

	validator := Mrc20Validator{}
	var mrc20UtxoList []mrc20.Mrc20Utxo

	var mrc20TrasferList []*mrc20.Mrc20Utxo
	//var deployHandleList []*pin.PinInscription
	var mintHandleList []*pin.PinInscription
	var transferHandleList []*pin.PinInscription
	var arrivalHandleList []*pin.PinInscription
	for _, pinNode := range mrc20List {
		log.Printf("[MRC20] Processing PIN: path=%s, id=%s", pinNode.Path, pinNode.Id)
		switch pinNode.Path {
		case "/ft/mrc20/deploy":
			//deployHandleList = append(deployHandleList, pinNode)
			//Prioritize handling deploy
			deployResult := deployHandle(pinNode)
			if len(deployResult) > 0 {
				mrc20UtxoList = append(mrc20UtxoList, deployResult...)
			}
		case "/ft/mrc20/mint":
			mintHandleList = append(mintHandleList, pinNode)
		case "/ft/mrc20/transfer":
			transferHandleList = append(transferHandleList, pinNode)
		case "/ft/mrc20/arrival":
			arrivalHandleList = append(arrivalHandleList, pinNode)
		}
	}

	// 处理 arrival (跃迁目标)
	// arrival 处理优先于 transfer，因为 teleport transfer 需要引用 arrival
	for _, pinNode := range arrivalHandleList {
		err := arrivalHandle(pinNode)
		if err != nil {
			log.Println("arrivalHandle error:", err)
		}
	}

	for _, pinNode := range mintHandleList {
		mrc20Pin, err := CreateMrc20MintPin(pinNode, &validator, false)
		if err == nil {
			mrc20Pin.Chain = pinNode.ChainName
			mrc20UtxoList = append(mrc20UtxoList, mrc20Pin)
		}
	}
	changedTick := make(map[string]int64)
	if len(mrc20UtxoList) > 0 {
		PebbleStore.SaveMrc20Pin(mrc20UtxoList)
		for _, item := range mrc20UtxoList {
			if item.MrcOption != mrc20.OptionDeploy {
				changedTick[item.Mrc20Id] += 1
			}
		}
	}

	//CatchNativeMrc20Transfer
	handleNativTransfer(chainName, height, mrc20TransferPinTx, txInList, isMempool)
	// mrc20transferCheck, err := PebbleStore.GetMrc20UtxoByOutPutList(txInList, isMempool)
	// if err == nil && len(mrc20transferCheck) > 0 {
	// 	mrc20TrasferList := IndexerAdapter[chainName].CatchNativeMrc20Transfer(height, mrc20transferCheck, mrc20TransferPinTx)
	// 	if len(mrc20TrasferList) > 0 {
	// 		PebbleStore.UpdateMrc20Utxo(mrc20TrasferList, isMempool)
	// 	}
	// }

	mrc20TrasferList = transferHandle(transferHandleList)
	if len(mrc20TrasferList) > 0 {
		//PebbleStore.UpdateMrc20Utxo(mrc20TrasferList, false)
		for _, item := range mrc20TrasferList {
			if item.MrcOption != mrc20.OptionDeploy {
				changedTick[item.Mrc20Id] += 1
			}
		}
	}
	//CatchNativeMrc20Transfer Agin
	handleNativTransfer(chainName, height, mrc20TransferPinTx, txInList, isMempool)
	//update holders,txCount
	for id, txNum := range changedTick {
		go PebbleStore.UpdateMrc20TickHolder(id, txNum)
	}
}
func handleNativTransfer(chainName string, height int64, mrc20TransferPinTx map[string]struct{}, txInList []string, isMempool bool) {
	log.Printf("[DEBUG] handleNativTransfer: height=%d, txInList count=%d", height, len(txInList))
	mrc20transferCheck, err := PebbleStore.GetMrc20UtxoByOutPutList(txInList, isMempool)
	log.Printf("[DEBUG] handleNativTransfer: GetMrc20UtxoByOutPutList returned %d UTXOs, err=%v", len(mrc20transferCheck), err)
	if err == nil && len(mrc20transferCheck) > 0 {
		for _, utxo := range mrc20transferCheck {
			log.Printf("[DEBUG] handleNativTransfer: found UTXO %s, status=%d, amt=%s", utxo.TxPoint, utxo.Status, utxo.AmtChange)
		}
		mrc20TrasferList := IndexerAdapter[chainName].CatchNativeMrc20Transfer(height, mrc20transferCheck, mrc20TransferPinTx)
		log.Printf("[DEBUG] handleNativTransfer: CatchNativeMrc20Transfer returned %d UTXOs", len(mrc20TrasferList))
		if len(mrc20TrasferList) > 0 {
			PebbleStore.UpdateMrc20Utxo(mrc20TrasferList, isMempool)
		}
	}
}
func transferHandle(transferHandleList []*pin.PinInscription) (mrc20UtxoList []*mrc20.Mrc20Utxo) {
	validator := Mrc20Validator{}

	// 分离普通 transfer 和 teleport transfer
	// teleport 可能依赖同一区块内普通 transfer 创建的 UTXO，所以必须分两步处理
	var normalTransferList []*pin.PinInscription
	var teleportTransferList []*pin.PinInscription

	for _, pinNode := range transferHandleList {
		// 检查是否是 teleport 类型
		if isTeleportTransfer(pinNode) {
			teleportTransferList = append(teleportTransferList, pinNode)
		} else {
			normalTransferList = append(normalTransferList, pinNode)
		}
	}

	log.Printf("[DEBUG] transferHandle: total=%d, normal=%d, teleport=%d",
		len(transferHandleList), len(normalTransferList), len(teleportTransferList))

	// 第一步：处理所有普通 transfer，创建输出 UTXO
	// 使用循环重试机制处理依赖关系（同一区块内普通 transfer 之间的依赖）
	normalSuccessMap := make(map[string]struct{})
	maxTimes := len(normalTransferList)
	for i := 0; i < maxTimes; i++ {
		if len(normalSuccessMap) >= maxTimes {
			break
		}
		for _, pinNode := range normalTransferList {
			if _, ok := normalSuccessMap[pinNode.Id]; ok {
				continue
			}

			log.Printf("[DEBUG] Processing normal transfer PIN: %s", pinNode.Id)

			transferPinList, _ := CreateMrc20TransferUtxo(pinNode, &validator, false)
			if len(transferPinList) > 0 {
				mrc20UtxoList = append(mrc20UtxoList, transferPinList...)
				normalSuccessMap[pinNode.Id] = struct{}{}
				PebbleStore.UpdateMrc20Utxo(transferPinList, false)
			}
		}
	}

	// 第二步：处理所有 teleport transfer
	// 此时同一区块内的普通 transfer 已经处理完毕，UTXO 已创建
	teleportSuccessMap := make(map[string]struct{})
	maxTimes = len(teleportTransferList)
	for i := 0; i < maxTimes; i++ {
		if len(teleportSuccessMap) >= maxTimes {
			break
		}
		for _, pinNode := range teleportTransferList {
			if _, ok := teleportSuccessMap[pinNode.Id]; ok {
				continue
			}

			log.Printf("[DEBUG] Processing teleport transfer PIN: %s", pinNode.Id)

			isTeleport, teleportUtxoList, err := processTeleportTransfer(pinNode, false)
			log.Printf("[DEBUG] processTeleportTransfer result: isTeleport=%v, utxoCount=%d, err=%v",
				isTeleport, len(teleportUtxoList), err)

			if !isTeleport {
				// 不应该发生，因为我们已经预先筛选过
				log.Printf("[WARN] PIN %s was classified as teleport but processTeleportTransfer returned false", pinNode.Id)
				teleportSuccessMap[pinNode.Id] = struct{}{}
				continue
			}

			if err != nil {
				log.Println("processTeleportTransfer error:", err)
				// teleport 处理失败，留给下一轮重试
				continue
			}

			if len(teleportUtxoList) > 0 {
				mrc20UtxoList = append(mrc20UtxoList, teleportUtxoList...)
				PebbleStore.UpdateMrc20Utxo(teleportUtxoList, false)
			}
			teleportSuccessMap[pinNode.Id] = struct{}{}
		}
	}

	return
}

// isTeleportTransfer 检查 PIN 是否是 teleport 类型的 transfer
// 只检查 JSON 格式，不执行实际处理
func isTeleportTransfer(pinNode *pin.PinInscription) bool {
	// 尝试解析为 teleport 格式
	var teleportData []mrc20.Mrc20TeleportTransferData

	// 先尝试解析为数组
	err := json.Unmarshal(pinNode.ContentBody, &teleportData)
	if err != nil {
		// 数组解析失败，尝试解析为单个对象
		var singleData mrc20.Mrc20TeleportTransferData
		err = json.Unmarshal(pinNode.ContentBody, &singleData)
		if err != nil {
			return false // 不是有效的 teleport JSON
		}
		teleportData = []mrc20.Mrc20TeleportTransferData{singleData}
	}

	// 检查是否有 teleport 类型的项
	for _, item := range teleportData {
		if item.Type == "teleport" {
			return true
		}
	}
	return false
}

// IsTeleportTransferDebug 导出函数供测试使用
func IsTeleportTransferDebug(pinNode *pin.PinInscription) bool {
	return isTeleportTransfer(pinNode)
}

func deployHandle(pinNode *pin.PinInscription) (mrc20UtxoList []mrc20.Mrc20Utxo) {
	log.Printf("[MRC20] deployHandle: pinId=%s", pinNode.Id)
	var deployList []mrc20.Mrc20DeployInfo
	validator := Mrc20Validator{}
	//for _, pinNode := range deployHandleList {
	mrc20Pin, preMineUtxo, info, err := CreateMrc20DeployPin(pinNode, &validator)
	log.Printf("[MRC20] CreateMrc20DeployPin result: err=%v, mrc20Id=%s, tick=%s", err, info.Mrc20Id, info.Tick)
	if err == nil {
		if mrc20Pin.Mrc20Id != "" {
			mrc20Pin.Chain = pinNode.ChainName
			mrc20UtxoList = append(mrc20UtxoList, mrc20Pin)
		}
		if preMineUtxo.Mrc20Id != "" {
			mrc20UtxoList = append(mrc20UtxoList, preMineUtxo)
		}
		if info.Tick != "" && info.Mrc20Id != "" {
			deployList = append(deployList, info)
		}
	}
	//}
	if len(deployList) > 0 {
		PebbleStore.SaveMrc20Tick(deployList)
	}
	return
}
func CreateMrc20DeployPin(pinNode *pin.PinInscription, validator *Mrc20Validator) (mrc20Utxo mrc20.Mrc20Utxo, preMineUtxo mrc20.Mrc20Utxo, info mrc20.Mrc20DeployInfo, err error) {
	var df mrc20.Mrc20Deploy
	log.Printf("[MRC20] CreateMrc20DeployPin: contentBody=%s", string(pinNode.ContentBody))
	err = json.Unmarshal(pinNode.ContentBody, &df)
	if err != nil {
		log.Printf("[MRC20] CreateMrc20DeployPin: json unmarshal error: %v", err)
		return
	}
	log.Printf("[MRC20] CreateMrc20DeployPin: parsed deploy data: tick=%s, mintCount=%s, amtPerMint=%s, premineCount=%s", df.Tick, df.MintCount, df.AmtPerMint, df.PremineCount)
	premineCount := int64(0)
	if df.PremineCount != "" {
		premineCount, err = strconv.ParseInt(df.PremineCount, 10, 64)
		if err != nil {
			log.Printf("[MRC20] CreateMrc20DeployPin: premineCount parse error: %v", err)
			return
		}
	}
	mintCount, err := strconv.ParseInt(df.MintCount, 10, 64)
	if err != nil {
		log.Printf("[MRC20] CreateMrc20DeployPin: mintCount parse error: %v", err)
		return
	}
	if mintCount < 0 {
		mintCount = int64(0)
	}
	amtPerMint, err := strconv.ParseInt(df.AmtPerMint, 10, 64)
	if err != nil {
		log.Printf("[MRC20] CreateMrc20DeployPin: amtPerMint parse error: %v", err)
		return
	}
	if amtPerMint < 0 {
		amtPerMint = int64(0)
	}
	//premineCount
	if mintCount < premineCount {
		log.Printf("[MRC20] CreateMrc20DeployPin: mintCount(%d) < premineCount(%d), returning", mintCount, premineCount)
		return
	}
	log.Printf("[MRC20] CreateMrc20DeployPin: calling validator.Deploy")
	premineAddress, pointValue, err1 := validator.Deploy(pinNode.ContentBody, pinNode)
	log.Printf("[MRC20] CreateMrc20DeployPin: validator.Deploy result: premineAddress=%s, pointValue=%d, err=%v", premineAddress, pointValue, err1)
	if err1 != nil {
		//mrc20Utxo.Verify = false
		//mrc20Utxo.Msg = err1.Error()
		err = err1 // 传递错误
		return
	}
	info.Tick = strings.ToUpper(df.Tick)
	info.TokenName = df.TokenName
	info.Decimals = df.Decimals
	info.AmtPerMint = df.AmtPerMint
	info.PremineCount = uint64(premineCount)
	info.MintCount = uint64(mintCount)
	info.BeginHeight = df.BeginHeight
	info.EndHeight = df.EndHeight
	info.Metadata = df.Metadata
	info.DeployType = df.DeployType
	info.PinCheck = df.PinCheck
	info.PayCheck = df.PayCheck
	info.DeployTime = pinNode.Timestamp

	info.Mrc20Id = pinNode.Id
	info.PinNumber = pinNode.Number
	info.Chain = pinNode.ChainName
	info.Address = pinNode.Address
	info.MetaId = pinNode.MetaId
	mrc20Utxo.Tick = info.Tick
	mrc20Utxo.Mrc20Id = pinNode.Id
	mrc20Utxo.PinId = pinNode.Id
	mrc20Utxo.BlockHeight = pinNode.GenesisHeight
	mrc20Utxo.MrcOption = mrc20.OptionDeploy
	mrc20Utxo.FromAddress = pinNode.CreateAddress
	mrc20Utxo.ToAddress = pinNode.Address
	mrc20Utxo.TxPoint = pinNode.Output
	mrc20Utxo.PinContent = string(pinNode.ContentBody)
	mrc20Utxo.Timestamp = pinNode.Timestamp
	mrc20Utxo.PointValue = uint64(pinNode.OutputValue)
	mrc20Utxo.Verify = true

	if premineAddress != "" && premineCount > 0 {
		preMineUtxo.Verify = true
		//preMineUtxo.PinId = pinNode.Id
		preMineUtxo.BlockHeight = pinNode.GenesisHeight
		preMineUtxo.MrcOption = mrc20.OptionPreMint
		preMineUtxo.FromAddress = pinNode.Address
		preMineUtxo.ToAddress = premineAddress
		preMineUtxo.TxPoint = fmt.Sprintf("%s:%d", pinNode.GenesisTransaction, 1)
		//mrc20Utxo.PinContent = string(pinNode.ContentBody)
		preMineUtxo.Timestamp = pinNode.Timestamp
		preMineUtxo.PointValue = uint64(pointValue)
		preMineUtxo.Mrc20Id = info.Mrc20Id
		preMineUtxo.Tick = info.Tick
		preMineUtxo.Chain = pinNode.ChainName
		//preMineUtxo.AmtChange = premineCount * amtPerMint
		num := strconv.FormatInt(premineCount*amtPerMint, 10)
		preMineUtxo.AmtChange, _ = decimal.NewFromString(num)
		info.TotalMinted = uint64(premineCount)
	}
	return
}

func CreateMrc20MintPin(pinNode *pin.PinInscription, validator *Mrc20Validator, mempool bool) (mrc20Utxo mrc20.Mrc20Utxo, err error) {
	var content mrc20.Mrc20MintData
	err = json.Unmarshal(pinNode.ContentBody, &content)
	if err != nil {
		return
	}
	mrc20Utxo.Verify = true
	mrc20Utxo.PinId = pinNode.Id
	mrc20Utxo.BlockHeight = pinNode.GenesisHeight
	mrc20Utxo.MrcOption = mrc20.OptionMint
	mrc20Utxo.FromAddress = pinNode.Address
	mrc20Utxo.ToAddress = pinNode.Address
	mrc20Utxo.TxPoint = pinNode.Output
	mrc20Utxo.PinContent = string(pinNode.ContentBody)
	mrc20Utxo.Timestamp = pinNode.Timestamp
	mrc20Utxo.PointValue = uint64(pinNode.OutputValue)
	info, shovelList, toAddress, vout, err1 := validator.Mint(content, pinNode)
	if toAddress != "" {
		mrc20Utxo.ToAddress = toAddress
		mrc20Utxo.TxPoint = fmt.Sprintf("%s:%d", pinNode.GenesisTransaction, vout)
	}
	if info != (mrc20.Mrc20DeployInfo{}) {
		mrc20Utxo.Mrc20Id = info.Mrc20Id
		mrc20Utxo.Tick = info.Tick
	}
	if mempool {
		mrc20Utxo.Mrc20Id = info.Mrc20Id
		mrc20Utxo.AmtChange, _ = decimal.NewFromString(info.AmtPerMint)
		return
	}
	if err1 != nil {
		mrc20Utxo.Mrc20Id = info.Mrc20Id
		mrc20Utxo.Verify = false
		mrc20Utxo.Msg = err1.Error()
	} else {
		if len(shovelList) > 0 {
			PebbleStore.AddMrc20Shovel(shovelList, pinNode.Id, mrc20Utxo.Mrc20Id)
		}
		PebbleStore.UpdateMrc20TickInfo(info.Mrc20Id, mrc20Utxo.TxPoint, uint64(info.TotalMinted)+1)
		//mrc20Utxo.AmtChange, _ = strconv.ParseInt(info.AmtPerMint, 10, 64)
		mrc20Utxo.AmtChange, _ = decimal.NewFromString(info.AmtPerMint)
	}

	return
}

func CreateMrc20TransferUtxo(pinNode *pin.PinInscription, validator *Mrc20Validator, isMempool bool) (mrc20UtxoList []*mrc20.Mrc20Utxo, err error) {
	//Check if it has been processed
	find, err1 := PebbleStore.CheckOperationtx(pinNode.GenesisTransaction, isMempool)
	if err1 != nil || find != nil {
		return
	}

	var content []mrc20.Mrc20TranferData
	err = json.Unmarshal(pinNode.ContentBody, &content)
	if err != nil {
		mrc20UtxoList = sendAllAmountToFirstOutput(pinNode, "Transfer JSON format error", isMempool)
		return
	}
	//check
	toAddress, utxoList, outputValueList, msg, firstIdx, err1 := validator.Transfer(content, pinNode, isMempool)
	//if err1 != nil && err1.Error() != "valueErr" {
	if err1 != nil {
		mrc20UtxoList = sendAllAmountToFirstOutput(pinNode, msg, isMempool)
		return
	}
	address := make(map[string]string)
	name := make(map[string]string)
	inputAmtMap := make(map[string]decimal.Decimal)
	var spendUtxoList []*mrc20.Mrc20Utxo
	for _, utxo := range utxoList {
		address[utxo.Mrc20Id] = utxo.ToAddress
		name[utxo.Mrc20Id] = utxo.Tick
		// 处理输入 UTXO 状态
		mrc20Utxo := *utxo
		if isMempool {
			// mempool 阶段：设置为 TransferPending（待转出）
			mrc20Utxo.Status = mrc20.UtxoStatusTransferPending
		} else {
			// 出块确认：设置为 Spent（已消耗）
			mrc20Utxo.Status = mrc20.UtxoStatusSpent
		}
		// 注意：不修改 MrcOption，保留原始操作类型（mint/deploy/teleport/transfer 等）
		// MrcOption 表示 UTXO 是如何创建的，而不是如何被花费的
		mrc20Utxo.OperationTx = pinNode.GenesisTransaction
		spendUtxoList = append(spendUtxoList, &mrc20Utxo)
		inputAmtMap[utxo.Mrc20Id] = inputAmtMap[utxo.Mrc20Id].Add(utxo.AmtChange)
	}
	outputAmtMap := make(map[string]decimal.Decimal)
	x := 0
	var reciveUtxoList []*mrc20.Mrc20Utxo
	for _, item := range content {
		mrc20Utxo := mrc20.Mrc20Utxo{}
		mrc20Utxo.Mrc20Id = item.Id
		mrc20Utxo.Tick = name[item.Id]
		mrc20Utxo.Verify = true
		mrc20Utxo.PinId = pinNode.Id
		mrc20Utxo.BlockHeight = pinNode.GenesisHeight
		mrc20Utxo.MrcOption = mrc20.OptionDataTransfer
		mrc20Utxo.FromAddress = address[item.Id]
		mrc20Utxo.ToAddress = toAddress[item.Vout]
		mrc20Utxo.Chain = pinNode.ChainName
		mrc20Utxo.TxPoint = fmt.Sprintf("%s:%d", pinNode.GenesisTransaction, item.Vout)
		mrc20Utxo.PinContent = string(pinNode.ContentBody)
		mrc20Utxo.Index = x
		mrc20Utxo.OperationTx = pinNode.GenesisTransaction
		mrc20Utxo.PointValue = uint64(outputValueList[item.Vout])
		//mrc20Utxo.AmtChange, _ = strconv.ParseInt(item.Amount, 10, 64)
		mrc20Utxo.AmtChange, _ = decimal.NewFromString(item.Amount)
		//outputAmtMap[item.Id] += mrc20Utxo.AmtChange
		outputAmtMap[item.Id] = outputAmtMap[item.Id].Add(mrc20Utxo.AmtChange)
		mrc20Utxo.Timestamp = pinNode.Timestamp
		reciveUtxoList = append(reciveUtxoList, &mrc20Utxo)
		x += 1
	}
	//Check if the input exceeds the output.
	for id, inputAmt := range inputAmtMap {
		//inputAmt > outputAmtMap[id]
		if inputAmt.Compare(outputAmtMap[id]) == 1 {
			//if !isMempool {
			// find := false
			// for _, utxo := range mrc20UtxoList {
			// 	vout := strings.Split(utxo.TxPoint, ":")[1]
			// 	if utxo.Mrc20Id == id && utxo.ToAddress == toAddress[0] && vout == "0" {
			// 		//utxo.AmtChange += (inputAmt - outputAmtMap[id])

			// 		diff := inputAmt.Sub(outputAmtMap[id])
			// 		fmt.Println("2===>", diff, utxo.AmtChange)
			// 		utxo.AmtChange = utxo.AmtChange.Add(diff)

			// 		utxo.Msg = "The total input amount is greater than the output amount"
			// 		find = true
			// 	}
			// }
			// if find {
			// 	continue
			// }
			//}
			mrc20Utxo := mrc20.Mrc20Utxo{}
			mrc20Utxo.Mrc20Id = id
			mrc20Utxo.Tick = name[id]
			mrc20Utxo.Verify = true
			mrc20Utxo.PinId = pinNode.Id
			mrc20Utxo.BlockHeight = pinNode.GenesisHeight
			mrc20Utxo.MrcOption = mrc20.OptionDataTransfer
			mrc20Utxo.FromAddress = address[id]
			mrc20Utxo.ToAddress = toAddress[0]
			mrc20Utxo.Chain = pinNode.ChainName
			mrc20Utxo.Timestamp = pinNode.Timestamp
			mrc20Utxo.TxPoint = fmt.Sprintf("%s:%d", pinNode.GenesisTransaction, firstIdx)
			mrc20Utxo.PointValue = uint64(outputValueList[firstIdx])
			mrc20Utxo.PinContent = string(pinNode.ContentBody)
			mrc20Utxo.OperationTx = pinNode.GenesisTransaction
			mrc20Utxo.Index = x
			//mrc20Utxo.AmtChange = inputAmt - outputAmtMap[id]
			mrc20Utxo.AmtChange = inputAmt.Sub(outputAmtMap[id])
			mrc20Utxo.Msg = "The total input amount is greater than the output amount"
			mrc20UtxoList = append(mrc20UtxoList, &mrc20Utxo)
			x += 1
		}
	}
	mrc20UtxoList = append(mrc20UtxoList, spendUtxoList...)
	mrc20UtxoList = append(mrc20UtxoList, reciveUtxoList...)
	return
}
func sendAllAmountToFirstOutput(pinNode *pin.PinInscription, msg string, isMempool bool) (mrc20UtxoList []*mrc20.Mrc20Utxo) {
	txb, err := GetTransactionWithCache(pinNode.ChainName, pinNode.GenesisTransaction)
	if err != nil {
		log.Println("GetTransactionWithCache:", err)
		return
	}
	toAddress := ""
	idx := 0
	value := int64(0)
	for i, out := range txb.MsgTx().TxOut {
		class, addresses, _, _ := txscript.ExtractPkScriptAddrs(out.PkScript, getBtcNetParams(pinNode.ChainName))
		if class.String() != "nulldata" && class.String() != "nonstandard" && len(addresses) > 0 {
			toAddress = addresses[0].String()
			idx = i
			value = out.Value
			break
		}
	}
	if toAddress == "" {
		return
	}
	var inputList []string
	for _, in := range txb.MsgTx().TxIn {
		s := fmt.Sprintf("%s:%d", in.PreviousOutPoint.Hash.String(), in.PreviousOutPoint.Index)
		inputList = append(inputList, s)
	}
	list, err := PebbleStore.GetMrc20UtxoByOutPutList(inputList, isMempool)
	if err != nil {
		//log.Println("GetMrc20UtxoByOutPutList:", err)
		return
	}
	utxoList := make(map[string]*mrc20.Mrc20Utxo)
	for _, item := range list {
		//Spent the input UTXO
		//amt := item.AmtChange * -1
		amt := item.AmtChange.Neg()
		mrc20Utxo := mrc20.Mrc20Utxo{TxPoint: item.TxPoint, Index: item.Index, Mrc20Id: item.Mrc20Id, Verify: true, Status: -1, AmtChange: amt}
		mrc20UtxoList = append(mrc20UtxoList, &mrc20Utxo)
		if v, ok := utxoList[item.Mrc20Id]; ok {
			//v.AmtChange += item.AmtChange
			v.AmtChange = v.AmtChange.Add(item.AmtChange)
		} else {
			utxoList[item.Mrc20Id] = &mrc20.Mrc20Utxo{
				Mrc20Id:     item.Mrc20Id,
				Tick:        item.Tick,
				Verify:      true,
				PinId:       pinNode.Id,
				BlockHeight: pinNode.GenesisHeight,
				MrcOption:   mrc20.OptionDataTransfer,
				FromAddress: pinNode.Address,
				ToAddress:   toAddress,
				Chain:       pinNode.ChainName,
				Timestamp:   pinNode.Timestamp,
				TxPoint:     fmt.Sprintf("%s:%d", pinNode.GenesisTransaction, idx),
				PointValue:  uint64(value),
				PinContent:  string(pinNode.ContentBody),
				Index:       0,
				AmtChange:   item.AmtChange,
				Msg:         msg,
				OperationTx: pinNode.GenesisTransaction,
			}
		}

	}
	for _, mrc20Utxo := range utxoList {
		mrc20UtxoList = append(mrc20UtxoList, mrc20Utxo)
	}
	return
}
func Mrc20NativeTransferHandle(sendList []*mrc20.Mrc20Utxo, reciveAddressList map[string]*string, txPointList map[string]*string) (mrc20UtxoList []mrc20.Mrc20Utxo, err error) {

	return
}

// ================ Teleport 跃迁处理 ================

// arrivalHandle 处理 /ft/mrc20/arrival PIN
// arrival 是跃迁的目标端，记录了预期从源链转移的资产信息
// arrival 出块时检查源链 UTXO 状态：
// - Status == 0 (可用)：正常保存 arrival，等待 teleport
// - Status == 1 (teleport pending)：查找 pending teleport 并执行跃迁
// - Status == -1 (已消耗)：检查是否有对应的 teleport 记录
//   - 有记录 → 跃迁已完成
//   - 无记录 → UTXO 被其他操作花费，跃迁失败
func arrivalHandle(pinNode *pin.PinInscription) error {
	var data mrc20.Mrc20ArrivalData
	err := json.Unmarshal(pinNode.ContentBody, &data)
	if err != nil {
		log.Println("arrivalHandle: JSON parse error:", err)
		return saveInvalidArrival(pinNode, "JSON parse error: "+err.Error())
	}

	// 验证必填字段
	if data.AssetOutpoint == "" {
		return saveInvalidArrival(pinNode, "assetOutpoint is required")
	}
	if data.Amount == "" {
		return saveInvalidArrival(pinNode, "amount is required")
	}
	if data.TickId == "" {
		return saveInvalidArrival(pinNode, "tickId is required")
	}

	// 解析金额
	amount, err := decimal.NewFromString(string(data.Amount))
	if err != nil {
		return saveInvalidArrival(pinNode, "invalid amount format: "+err.Error())
	}
	if amount.LessThanOrEqual(decimal.Zero) {
		return saveInvalidArrival(pinNode, "amount must be greater than 0")
	}

	// 尝试获取 tickId 信息 (跨链情况下可能不存在于目标链)
	var tickName string
	tickInfo, err := PebbleStore.GetMrc20TickInfo(data.TickId, "")
	if err == nil {
		tickName = tickInfo.Tick
	} else {
		log.Printf("[Arrival] tickId %s not found locally, may be cross-chain arrival", data.TickId)
	}

	// 获取接收地址 (output[locationIndex] 的地址)
	// 使用带区块高度的版本，支持没有 txindex 的节点
	toAddress, err := getAddressFromOutputWithHeight(pinNode.ChainName, pinNode.GenesisTransaction, data.LocationIndex, pinNode.GenesisHeight)
	if err != nil {
		return saveInvalidArrival(pinNode, "invalid locationIndex: "+err.Error())
	}

	// 检查是否已存在相同 assetOutpoint 的 arrival
	existingArrival, _ := PebbleStore.GetMrc20ArrivalByAssetOutpoint(data.AssetOutpoint)
	if existingArrival != nil {
		return saveInvalidArrival(pinNode, "arrival already exists for this assetOutpoint")
	}

	// 获取源链 UTXO 并检查状态
	sourceChain := ""
	sourceUtxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(data.AssetOutpoint, false) // 不检查状态

	if sourceUtxo != nil {
		sourceChain = sourceUtxo.Chain

		// 验证 tickId 匹配
		if sourceUtxo.Mrc20Id != data.TickId {
			return saveInvalidArrival(pinNode, fmt.Sprintf("tickId mismatch: expected %s, got %s", sourceUtxo.Mrc20Id, data.TickId))
		}
		// 验证金额必须是 UTXO 的全部金额
		if !sourceUtxo.AmtChange.Equal(amount) {
			return saveInvalidArrival(pinNode, fmt.Sprintf("amount must be the full UTXO amount: expected %s, got %s", sourceUtxo.AmtChange.String(), amount.String()))
		}
		// 获取 tick 名称
		if tickName == "" {
			tickName = sourceUtxo.Tick
		}

		// 根据 UTXO 状态处理
		switch sourceUtxo.Status {
		case mrc20.UtxoStatusSpent: // -1: 已消耗
			// 检查是否有对应的 teleport 记录
			if PebbleStore.CheckTeleportExistsByAssetOutpoint(data.AssetOutpoint) {
				// 跃迁已完成，arrival 也标记为完成
				log.Printf("[Arrival] UTXO %s already teleported, marking arrival as completed", data.AssetOutpoint)
				return saveCompletedArrival(pinNode, data, tickName, toAddress, sourceChain, "UTXO already teleported")
			}
			// UTXO 被其他操作花费了，跃迁失败
			log.Printf("[Arrival] UTXO %s spent by other operation, teleport failed", data.AssetOutpoint)
			return saveInvalidArrival(pinNode, fmt.Sprintf("UTXO %s already spent by other operation, teleport failed", data.AssetOutpoint))

		case mrc20.UtxoStatusTeleportPending: // 1: 等待跃迁
			// UTXO 已处于 pending 状态，说明 teleport transfer 先出块
			log.Printf("[Arrival] UTXO %s is in teleport pending state, processing pending teleport", data.AssetOutpoint)
		}
	}

	// 创建 arrival 记录 (状态为 pending)
	arrival := &mrc20.Mrc20Arrival{
		PinId:         pinNode.Id,
		TxId:          pinNode.GenesisTransaction,
		AssetOutpoint: data.AssetOutpoint,
		Amount:        amount,
		TickId:        data.TickId,
		Tick:          tickName,
		LocationIndex: data.LocationIndex,
		ToAddress:     toAddress,
		Chain:         pinNode.ChainName,
		SourceChain:   sourceChain,
		Status:        mrc20.ArrivalStatusPending,
		BlockHeight:   pinNode.GenesisHeight,
		Timestamp:     pinNode.Timestamp,
	}

	err = PebbleStore.SaveMrc20Arrival(arrival)
	if err != nil {
		return err
	}

	// 检查是否有等待此 arrival 的 pending teleport
	processPendingTeleportForArrival(arrival)

	return nil
}

// saveTeleportPendingIn 保存 teleport 接收方的 pending 余额记录
func saveTeleportPendingIn(arrival *mrc20.Mrc20Arrival, pending *mrc20.PendingTeleport) {
	amount, _ := decimal.NewFromString(pending.Amount)
	pendingIn := &mrc20.TeleportPendingIn{
		Coord:       arrival.PinId,
		ToAddress:   arrival.ToAddress,
		TickId:      arrival.TickId,
		Tick:        arrival.Tick,
		Amount:      amount,
		Chain:       arrival.Chain,
		SourceChain: pending.SourceChain,
		FromAddress: pending.FromAddress,
		TeleportTx:  pending.TxId,
		ArrivalTx:   arrival.TxId,
		BlockHeight: pending.BlockHeight, // 使用 teleport 的区块高度
		Timestamp:   pending.Timestamp,
	}
	err := PebbleStore.SaveTeleportPendingIn(pendingIn)
	if err != nil {
		log.Printf("SaveTeleportPendingIn error: %v", err)
	} else {
		log.Printf("[TeleportPendingIn] Saved pending in for address %s, tick %s, amount %s",
			arrival.ToAddress, arrival.Tick, amount.String())
	}
}

// processPendingTeleportForArrival 处理等待特定 arrival 的 pending teleport
// 当 arrival 被处理后调用，检查是否有 teleport 在等待这个 arrival
func processPendingTeleportForArrival(arrival *mrc20.Mrc20Arrival) {
	pending, err := PebbleStore.GetPendingTeleportByCoord(arrival.PinId)
	if err != nil {
		// 没有等待的 teleport，正常情况
		return
	}

	log.Printf("Found pending teleport %s waiting for arrival %s, processing now...", pending.PinId, arrival.PinId)

	// 验证 pending teleport 的 assetOutpoint 与 arrival 声明的一致
	if pending.AssetOutpoint != arrival.AssetOutpoint {
		log.Printf("AssetOutpoint mismatch: pending has %s, arrival expects %s", pending.AssetOutpoint, arrival.AssetOutpoint)
		// 更新 pending 状态为 invalid
		pending.Status = -1
		PebbleStore.SavePendingTeleport(pending)
		// 更新 arrival 状态为 invalid
		PebbleStore.UpdateMrc20ArrivalStatus(arrival.PinId, mrc20.ArrivalStatusInvalid, "", "", "", 0)
		return
	}

	// 保存 TeleportPendingIn 记录（用于接收方的 PendingInBalance）
	saveTeleportPendingIn(arrival, pending)

	// 获取 UTXO 并验证状态
	sourceUtxo, err := PebbleStore.GetMrc20UtxoByTxPoint(pending.AssetOutpoint, false)
	if err != nil {
		log.Printf("Source UTXO not found: %s", pending.AssetOutpoint)
		pending.Status = -1
		PebbleStore.SavePendingTeleport(pending)
		return
	}

	// 检查 UTXO 状态
	if sourceUtxo.Status == mrc20.UtxoStatusSpent {
		// UTXO 已被消耗（在 pending 期间被其他交易花费了）
		log.Printf("UTXO %s was spent during pending state, teleport failed", pending.AssetOutpoint)
		pending.Status = -1
		PebbleStore.SavePendingTeleport(pending)
		PebbleStore.UpdateMrc20ArrivalStatus(arrival.PinId, mrc20.ArrivalStatusInvalid, "", "", "", 0)
		return
	}

	// 构造 teleport 数据
	teleportData := mrc20.Mrc20TeleportTransferData{
		Id:     pending.TickId,
		Amount: pending.Amount,
		Coord:  pending.Coord,
		Chain:  pending.TargetChain,
		Type:   "teleport",
	}

	// 构造 pinNode 用于处理
	fakePinNode := &pin.PinInscription{
		Id:                 pending.PinId,
		GenesisTransaction: pending.TxId,
		Address:            pending.FromAddress,
		ChainName:          pending.SourceChain,
		GenesisHeight:      pending.BlockHeight,
		Timestamp:          pending.Timestamp,
		ContentBody:        pending.RawContent,
	}

	// 执行跃迁
	utxoList, err := executeTeleportTransfer(fakePinNode, teleportData, sourceUtxo, arrival, false)
	if err != nil {
		log.Printf("Execute pending teleport error: %v", err)
		pending.RetryCount++
		pending.Status = -1
		PebbleStore.SavePendingTeleport(pending)
		return
	}

	// 处理成功，保存 UTXO
	if len(utxoList) > 0 {
		PebbleStore.UpdateMrc20Utxo(utxoList, false)
	}

	// 更新 pending 状态为完成
	pending.Status = 1
	PebbleStore.SavePendingTeleport(pending)

	log.Printf("Pending teleport %s processed successfully", pending.PinId)
}

// saveCompletedArrival 保存已完成的 arrival 记录
func saveCompletedArrival(pinNode *pin.PinInscription, data mrc20.Mrc20ArrivalData, tickName, toAddress, sourceChain, msg string) error {
	amount, _ := decimal.NewFromString(string(data.Amount))
	arrival := &mrc20.Mrc20Arrival{
		PinId:         pinNode.Id,
		TxId:          pinNode.GenesisTransaction,
		AssetOutpoint: data.AssetOutpoint,
		Amount:        amount,
		TickId:        data.TickId,
		Tick:          tickName,
		LocationIndex: data.LocationIndex,
		ToAddress:     toAddress,
		Chain:         pinNode.ChainName,
		SourceChain:   sourceChain,
		Status:        mrc20.ArrivalStatusCompleted,
		Msg:           msg,
		BlockHeight:   pinNode.GenesisHeight,
		Timestamp:     pinNode.Timestamp,
	}
	return PebbleStore.SaveMrc20Arrival(arrival)
}

// saveInvalidArrival 保存无效的 arrival 记录
func saveInvalidArrival(pinNode *pin.PinInscription, msg string) error {
	arrival := &mrc20.Mrc20Arrival{
		PinId:       pinNode.Id,
		TxId:        pinNode.GenesisTransaction,
		Chain:       pinNode.ChainName,
		Status:      mrc20.ArrivalStatusInvalid,
		Msg:         msg,
		BlockHeight: pinNode.GenesisHeight,
		Timestamp:   pinNode.Timestamp,
	}
	return PebbleStore.SaveMrc20Arrival(arrival)
}

// getAddressFromOutput 从交易的指定 output 获取地址
func getAddressFromOutput(chainName, txid string, outputIndex int) (string, error) {
	tx, err := ChainAdapter[chainName].GetTransaction(txid)
	if err != nil {
		return "", fmt.Errorf("get transaction error: %w", err)
	}

	txb := tx.(*btcutil.Tx)
	if outputIndex < 0 || outputIndex >= len(txb.MsgTx().TxOut) {
		return "", fmt.Errorf("output index out of range: %d", outputIndex)
	}

	out := txb.MsgTx().TxOut[outputIndex]
	class, addresses, _, err := txscript.ExtractPkScriptAddrs(out.PkScript, getBtcNetParams(chainName))
	if err != nil {
		return "", fmt.Errorf("extract address error: %w", err)
	}
	if class.String() == "nulldata" || class.String() == "nonstandard" || len(addresses) == 0 {
		return "", fmt.Errorf("invalid output type: %s", class.String())
	}

	return addresses[0].String(), nil
}

// getAddressFromOutputWithHeight 从交易的指定 output 获取地址，支持通过区块高度获取交易
// 当节点没有 txindex 时，可以通过区块高度从区块中获取交易
func getAddressFromOutputWithHeight(chainName, txid string, outputIndex int, blockHeight int64) (string, error) {
	// 先尝试直接获取交易
	tx, err := ChainAdapter[chainName].GetTransaction(txid)
	if err == nil {
		txb := tx.(*btcutil.Tx)
		if outputIndex < 0 || outputIndex >= len(txb.MsgTx().TxOut) {
			return "", fmt.Errorf("output index out of range: %d", outputIndex)
		}
		out := txb.MsgTx().TxOut[outputIndex]
		return extractAddressFromPkScript(out.PkScript, chainName)
	}

	// GetTransaction 失败，尝试从区块获取
	if blockHeight <= 0 {
		return "", fmt.Errorf("get transaction failed and no block height provided: %w", err)
	}

	log.Printf("[MRC20] GetTransaction failed for %s, trying to get from block %d", txid, blockHeight)

	block, err := ChainAdapter[chainName].GetBlock(blockHeight)
	if err != nil {
		return "", fmt.Errorf("get block %d failed: %w", blockHeight, err)
	}

	// 从区块中查找交易 - 处理不同链的区块类型
	switch b := block.(type) {
	case *bsvwire.MsgBlock:
		// bsvd/wire.MsgBlock (用于 MVC)
		for _, blockTx := range b.Transactions {
			if blockTx.TxHash().String() == txid {
				if outputIndex < 0 || outputIndex >= len(blockTx.TxOut) {
					return "", fmt.Errorf("output index out of range: %d", outputIndex)
				}
				return extractAddressFromPkScript(blockTx.TxOut[outputIndex].PkScript, chainName)
			}
		}
	case *btcwire.MsgBlock:
		// btcsuite/btcd/wire.MsgBlock (用于 BTC, Doge)
		for _, blockTx := range b.Transactions {
			if blockTx.TxHash().String() == txid {
				if outputIndex < 0 || outputIndex >= len(blockTx.TxOut) {
					return "", fmt.Errorf("output index out of range: %d", outputIndex)
				}
				return extractAddressFromPkScript(blockTx.TxOut[outputIndex].PkScript, chainName)
			}
		}
	case *btcutil.Block:
		// btcutil.Block wrapper
		for _, blockTx := range b.MsgBlock().Transactions {
			if blockTx.TxHash().String() == txid {
				if outputIndex < 0 || outputIndex >= len(blockTx.TxOut) {
					return "", fmt.Errorf("output index out of range: %d", outputIndex)
				}
				return extractAddressFromPkScript(blockTx.TxOut[outputIndex].PkScript, chainName)
			}
		}
	default:
		return "", fmt.Errorf("unsupported block type: %T", block)
	}

	return "", fmt.Errorf("transaction %s not found in block %d", txid, blockHeight)
}

// extractAddressFromPkScript 从 PkScript 提取地址
func extractAddressFromPkScript(pkScript []byte, chainName string) (string, error) {
	class, addresses, _, err := txscript.ExtractPkScriptAddrs(pkScript, getBtcNetParams(chainName))
	if err != nil {
		return "", fmt.Errorf("extract address error: %w", err)
	}
	if class.String() == "nulldata" || class.String() == "nonstandard" || len(addresses) == 0 {
		return "", fmt.Errorf("invalid output type: %s", class.String())
	}
	return addresses[0].String(), nil
}

// processTeleportTransfer 处理 teleport 类型的 transfer
// 返回 true 表示是 teleport 并且已处理，返回 false 表示不是 teleport 需要走普通 transfer 流程
func processTeleportTransfer(pinNode *pin.PinInscription, isMempool bool) (bool, []*mrc20.Mrc20Utxo, error) {
	// 尝试解析为 teleport 格式 - 支持对象或数组格式
	var teleportData []mrc20.Mrc20TeleportTransferData

	// 先尝试解析为数组
	err := json.Unmarshal(pinNode.ContentBody, &teleportData)
	if err != nil {
		// 数组解析失败，尝试解析为单个对象
		var singleData mrc20.Mrc20TeleportTransferData
		err = json.Unmarshal(pinNode.ContentBody, &singleData)
		if err != nil {
			return false, nil, nil // 不是有效的 JSON，走普通 transfer
		}
		// 单个对象转为数组
		teleportData = []mrc20.Mrc20TeleportTransferData{singleData}
	}

	// 检查是否有 teleport 类型的项
	hasTeleport := false
	for _, item := range teleportData {
		if item.Type == "teleport" {
			hasTeleport = true
			break
		}
	}
	if !hasTeleport {
		return false, nil, nil // 没有 teleport 项，走普通 transfer
	}

	// 处理 teleport transfer
	var mrc20UtxoList []*mrc20.Mrc20Utxo
	var failedTeleport bool
	var failedMsg string

	for _, item := range teleportData {
		if item.Type != "teleport" {
			// 非 teleport 项暂时跳过 (TODO: 可以支持混合 transfer)
			continue
		}

		// 验证 teleport 数据
		utxoList, err := validateAndProcessTeleport(pinNode, item, isMempool)
		if err != nil {
			log.Println("processTeleportTransfer error:", err)
			failedTeleport = true
			failedMsg = err.Error()
			// teleport 验证失败，继续检查其他项，但需要记录失败
			continue
		}

		mrc20UtxoList = append(mrc20UtxoList, utxoList...)
	}

	// 如果所有 teleport 都失败了，需要处理交易输入中的 MRC20 UTXO
	// 将它们转到第一个有效输出地址，防止 UTXO 状态不一致
	if failedTeleport && len(mrc20UtxoList) == 0 {
		log.Printf("[Teleport] All teleports failed for tx %s, handling input UTXOs: %s", pinNode.GenesisTransaction, failedMsg)
		fallbackUtxoList := handleFailedTeleportInputs(pinNode, failedMsg, isMempool)
		mrc20UtxoList = append(mrc20UtxoList, fallbackUtxoList...)
	}

	return true, mrc20UtxoList, nil
}

// validateAndProcessTeleport 验证并处理单个 teleport 项
// 核心逻辑：
// 1. 从交易输入获取匹配的 MRC20 UTXO
// 2. 验证 UTXO 状态必须是可用(0)
// 3. 如果 arrival 存在且 pending → 执行跃迁
// 4. 如果 arrival 不存在 → UTXO 状态设为 pending(1)，加入等待队列
func validateAndProcessTeleport(pinNode *pin.PinInscription, data mrc20.Mrc20TeleportTransferData, isMempool bool) ([]*mrc20.Mrc20Utxo, error) {
	// 1. 验证必填字段
	if data.Coord == "" {
		return nil, fmt.Errorf("coord is required for teleport")
	}
	if data.Id == "" {
		return nil, fmt.Errorf("id (tickId) is required for teleport")
	}
	if data.Amount == "" {
		return nil, fmt.Errorf("amount is required for teleport")
	}
	if data.Chain == "" {
		return nil, fmt.Errorf("chain (target chain) is required for teleport")
	}

	// 2. 检查是否已有对应的 teleport (防止重复处理)
	if PebbleStore.CheckTeleportExists(data.Coord) {
		return nil, fmt.Errorf("teleport already exists for coord: %s", data.Coord)
	}

	// 3. 解析 teleport 金额
	teleportAmount, err := decimal.NewFromString(data.Amount)
	if err != nil {
		return nil, fmt.Errorf("invalid amount format: %s", data.Amount)
	}

	// 4. 从交易输入获取匹配的 MRC20 UTXO
	sourceUtxo, err := findTeleportSourceUtxo(pinNode, data.Id, teleportAmount)
	if err != nil {
		return nil, fmt.Errorf("find source UTXO error: %w", err)
	}

	// 5. 验证 UTXO 状态必须是可用(0)
	// 不允许同一个 UTXO 有多个 teleport transfer
	if sourceUtxo.Status != mrc20.UtxoStatusAvailable {
		if sourceUtxo.Status == mrc20.UtxoStatusTeleportPending {
			return nil, fmt.Errorf("UTXO %s is already in teleport pending state", sourceUtxo.TxPoint)
		}
		return nil, fmt.Errorf("UTXO %s is not available, status: %d", sourceUtxo.TxPoint, sourceUtxo.Status)
	}

	// 6. 检查是否有 arrival 记录
	arrival, err := PebbleStore.GetMrc20ArrivalByPinId(data.Coord)
	if err != nil {
		// arrival 还未被索引，将 UTXO 设为 pending 状态，加入等待队列
		log.Printf("Arrival not found for coord %s, setting UTXO to pending and adding to queue", data.Coord)

		// 将 UTXO 状态设为 pending(1)
		pendingUtxo := *sourceUtxo
		pendingUtxo.Status = mrc20.UtxoStatusTeleportPending
		pendingUtxo.Msg = fmt.Sprintf("teleport pending, waiting for arrival %s", data.Coord)
		pendingUtxo.OperationTx = pinNode.GenesisTransaction

		// 保存 pending teleport 记录
		pending := &mrc20.PendingTeleport{
			PinId:         pinNode.Id,
			TxId:          pinNode.GenesisTransaction,
			Coord:         data.Coord,
			TickId:        data.Id,
			Amount:        data.Amount,
			AssetOutpoint: sourceUtxo.TxPoint, // 记录待跃迁的 UTXO
			TargetChain:   data.Chain,
			FromAddress:   pinNode.Address,
			SourceChain:   pinNode.ChainName,
			BlockHeight:   pinNode.GenesisHeight,
			Timestamp:     pinNode.Timestamp,
			RetryCount:    0,
			Status:        0, // pending
			RawContent:    pinNode.ContentBody,
		}
		err = PebbleStore.SavePendingTeleport(pending)
		if err != nil {
			log.Println("SavePendingTeleport error:", err)
		}

		// 返回需要更新的 UTXO（状态变为 pending）
		return []*mrc20.Mrc20Utxo{&pendingUtxo}, nil
	}

	// 7. arrival 存在，验证状态
	if arrival.Status != mrc20.ArrivalStatusPending {
		return nil, fmt.Errorf("arrival is not pending, status: %d", arrival.Status)
	}

	// 8. 验证 arrival 数据与 teleport 数据匹配
	if arrival.TickId != data.Id {
		return nil, fmt.Errorf("tickId mismatch: arrival has %s, teleport has %s", arrival.TickId, data.Id)
	}
	if !arrival.Amount.Equal(teleportAmount) {
		return nil, fmt.Errorf("amount mismatch: arrival has %s, teleport has %s", arrival.Amount.String(), teleportAmount.String())
	}
	if arrival.Chain != data.Chain {
		return nil, fmt.Errorf("target chain mismatch: arrival is on %s, teleport targets %s", arrival.Chain, data.Chain)
	}

	// 9. 验证交易输入的 UTXO 与 arrival 声明的 assetOutpoint 匹配
	if sourceUtxo.TxPoint != arrival.AssetOutpoint {
		return nil, fmt.Errorf("UTXO mismatch: found %s in inputs, but arrival expects %s", sourceUtxo.TxPoint, arrival.AssetOutpoint)
	}

	// 10. 保存 TeleportPendingIn 记录（用于接收方的 PendingInBalance）
	// 构造一个临时的 PendingTeleport 用于调用 saveTeleportPendingIn
	tempPending := &mrc20.PendingTeleport{
		PinId:       pinNode.Id,
		TxId:        pinNode.GenesisTransaction,
		Amount:      data.Amount,
		FromAddress: pinNode.Address,
		SourceChain: pinNode.ChainName,
		BlockHeight: pinNode.GenesisHeight,
		Timestamp:   pinNode.Timestamp,
	}
	saveTeleportPendingIn(arrival, tempPending)

	// arrival 已存在且验证通过，执行跃迁
	return executeTeleportTransfer(pinNode, data, sourceUtxo, arrival, isMempool)
}

// findTeleportSourceUtxo 从交易输入中查找符合 teleport 条件的 MRC20 UTXO
func findTeleportSourceUtxo(pinNode *pin.PinInscription, tickId string, amount decimal.Decimal) (*mrc20.Mrc20Utxo, error) {
	// 获取交易
	txb, err := GetTransactionWithCache(pinNode.ChainName, pinNode.GenesisTransaction)
	if err != nil {
		return nil, fmt.Errorf("get transaction error: %w", err)
	}

	// 获取所有交易输入的 txpoint
	var inputList []string
	for _, in := range txb.MsgTx().TxIn {
		s := fmt.Sprintf("%s:%d", in.PreviousOutPoint.Hash.String(), in.PreviousOutPoint.Index)
		inputList = append(inputList, s)
	}

	// 查找输入中的 MRC20 UTXO（包括 pending 状态的，用于检查是否重复）
	for _, txPoint := range inputList {
		utxo, err := PebbleStore.GetMrc20UtxoByTxPoint(txPoint, false) // 不检查状态
		if err != nil {
			continue // 不是 MRC20 UTXO，跳过
		}

		// 匹配 tickId 和金额
		if utxo.Mrc20Id == tickId && utxo.AmtChange.Equal(amount) {
			// 验证 UTXO 所有者
			if utxo.ToAddress != pinNode.Address {
				return nil, fmt.Errorf("not authorized to spend UTXO %s: owner is %s, sender is %s",
					txPoint, utxo.ToAddress, pinNode.Address)
			}
			return utxo, nil
		}
	}

	return nil, fmt.Errorf("no matching MRC20 UTXO found in transaction inputs for tickId %s, amount %s", tickId, amount.String())
}

// handleFailedTeleportInputs 处理 teleport 失败时的输入 UTXO
// 将交易输入中的所有 MRC20 UTXO 转到第一个有效输出地址
// 这确保了即使 teleport 验证失败，UTXO 状态仍然与链上一致
func handleFailedTeleportInputs(pinNode *pin.PinInscription, failedMsg string, isMempool bool) []*mrc20.Mrc20Utxo {
	var mrc20UtxoList []*mrc20.Mrc20Utxo

	// 获取交易
	txb, err := GetTransactionWithCache(pinNode.ChainName, pinNode.GenesisTransaction)
	if err != nil {
		log.Println("handleFailedTeleportInputs: GetTransactionWithCache error:", err)
		return nil
	}

	// 获取第一个有效输出地址
	toAddress := ""
	outputIdx := 0
	outputValue := int64(0)
	for i, out := range txb.MsgTx().TxOut {
		class, addresses, _, _ := txscript.ExtractPkScriptAddrs(out.PkScript, getBtcNetParams(pinNode.ChainName))
		if class.String() != "nulldata" && class.String() != "nonstandard" && len(addresses) > 0 {
			toAddress = addresses[0].String()
			outputIdx = i
			outputValue = out.Value
			break
		}
	}
	if toAddress == "" {
		log.Println("handleFailedTeleportInputs: no valid output address found")
		return nil
	}

	// 获取所有交易输入的 txpoint
	var inputList []string
	for _, in := range txb.MsgTx().TxIn {
		s := fmt.Sprintf("%s:%d", in.PreviousOutPoint.Hash.String(), in.PreviousOutPoint.Index)
		inputList = append(inputList, s)
	}

	// 查找并处理输入中的 MRC20 UTXO
	list, err := PebbleStore.GetMrc20UtxoByOutPutList(inputList, isMempool)
	if err != nil || len(list) == 0 {
		log.Println("handleFailedTeleportInputs: no MRC20 UTXOs in inputs")
		return nil
	}

	// 按 tickId 聚合金额
	utxoByTick := make(map[string]*mrc20.Mrc20Utxo)
	for _, item := range list {
		// 标记输入 UTXO 为已消耗
		spentUtxo := *item
		spentUtxo.Status = mrc20.UtxoStatusSpent
		spentUtxo.OperationTx = pinNode.GenesisTransaction
		spentUtxo.Msg = fmt.Sprintf("teleport failed: %s", failedMsg)
		mrc20UtxoList = append(mrc20UtxoList, &spentUtxo)

		// 聚合到新 UTXO
		if v, ok := utxoByTick[item.Mrc20Id]; ok {
			v.AmtChange = v.AmtChange.Add(item.AmtChange)
		} else {
			utxoByTick[item.Mrc20Id] = &mrc20.Mrc20Utxo{
				Mrc20Id:     item.Mrc20Id,
				Tick:        item.Tick,
				Verify:      true,
				PinId:       pinNode.Id,
				BlockHeight: pinNode.GenesisHeight,
				MrcOption:   mrc20.OptionTeleportTransfer,
				FromAddress: item.ToAddress,
				ToAddress:   toAddress,
				Chain:       pinNode.ChainName,
				Timestamp:   pinNode.Timestamp,
				TxPoint:     fmt.Sprintf("%s:%d", pinNode.GenesisTransaction, outputIdx),
				PointValue:  uint64(outputValue),
				PinContent:  string(pinNode.ContentBody),
				Index:       0,
				AmtChange:   item.AmtChange,
				Status:      mrc20.UtxoStatusAvailable,
				Msg:         fmt.Sprintf("teleport failed, fallback to address: %s", failedMsg),
				OperationTx: pinNode.GenesisTransaction,
			}
		}
	}

	// 添加新的 UTXO（转到第一个有效输出）
	for _, newUtxo := range utxoByTick {
		mrc20UtxoList = append(mrc20UtxoList, newUtxo)
	}

	log.Printf("[Teleport] Failed teleport handled: %d input UTXOs transferred to %s", len(list), toAddress)
	return mrc20UtxoList
}

// executeTeleportTransfer 实际执行 teleport 转账
// 此函数在 arrival 已存在且验证通过的情况下调用
func executeTeleportTransfer(pinNode *pin.PinInscription, data mrc20.Mrc20TeleportTransferData, sourceUtxo *mrc20.Mrc20Utxo, arrival *mrc20.Mrc20Arrival, isMempool bool) ([]*mrc20.Mrc20Utxo, error) {
	var mrc20UtxoList []*mrc20.Mrc20Utxo

	teleportAmount, _ := decimal.NewFromString(data.Amount)

	// ========== 执行跃迁 ==========

	// 标记源 UTXO 为已消耗 (teleported)
	spentUtxo := *sourceUtxo
	spentUtxo.Status = mrc20.UtxoStatusSpent
	// 注意：不修改 MrcOption，保留原始操作类型
	spentUtxo.OperationTx = pinNode.GenesisTransaction
	spentUtxo.Msg = fmt.Sprintf("teleported to %s via coord %s", data.Chain, data.Coord)
	mrc20UtxoList = append(mrc20UtxoList, &spentUtxo)

	// 在目标链创建新 UTXO
	newUtxo := mrc20.Mrc20Utxo{
		Tick:        sourceUtxo.Tick,
		Mrc20Id:     sourceUtxo.Mrc20Id,
		TxPoint:     fmt.Sprintf("%s:%d", arrival.TxId, arrival.LocationIndex),
		PointValue:  0, // 目标链的 output value 需要从交易获取
		PinId:       arrival.PinId,
		PinContent:  string(pinNode.ContentBody),
		Verify:      true,
		BlockHeight: arrival.BlockHeight,
		MrcOption:   mrc20.OptionTeleportTransfer,
		FromAddress: pinNode.Address, // 源链发送者
		ToAddress:   arrival.ToAddress,
		AmtChange:   teleportAmount,
		Status:      mrc20.UtxoStatusAvailable,
		Chain:       arrival.Chain,
		Index:       0,
		Timestamp:   arrival.Timestamp,
		OperationTx: pinNode.GenesisTransaction,
		Msg:         fmt.Sprintf("teleported from %s via coord %s", pinNode.ChainName, data.Coord),
	}
	mrc20UtxoList = append(mrc20UtxoList, &newUtxo)

	// 更新 arrival 状态为已完成
	err := PebbleStore.UpdateMrc20ArrivalStatus(
		arrival.PinId,
		mrc20.ArrivalStatusCompleted,
		pinNode.Id,
		pinNode.ChainName,
		pinNode.GenesisTransaction,
		pinNode.Timestamp,
	)
	if err != nil {
		log.Println("UpdateMrc20ArrivalStatus error:", err)
	}

	// 删除 TeleportPendingIn 记录（跃迁完成，不再是 pending 状态）
	err = PebbleStore.DeleteTeleportPendingIn(arrival.PinId, arrival.ToAddress)
	if err != nil {
		log.Println("DeleteTeleportPendingIn error:", err)
	}

	// 保存 teleport 记录
	teleportRecord := &mrc20.Mrc20Teleport{
		PinId:          pinNode.Id,
		TxId:           pinNode.GenesisTransaction,
		TickId:         data.Id,
		Tick:           sourceUtxo.Tick,
		Amount:         teleportAmount,
		Coord:          data.Coord,
		FromAddress:    pinNode.Address,
		SourceChain:    pinNode.ChainName,
		TargetChain:    data.Chain,
		SpentUtxoPoint: sourceUtxo.TxPoint,
		Status:         1, // 完成
		BlockHeight:    pinNode.GenesisHeight,
		Timestamp:      pinNode.Timestamp,
	}
	err = PebbleStore.SaveMrc20Teleport(teleportRecord)
	if err != nil {
		log.Println("SaveMrc20Teleport error:", err)
	}

	log.Printf("Teleport completed: %s from %s to %s, amount: %s, coord: %s",
		sourceUtxo.Tick, pinNode.ChainName, data.Chain, teleportAmount.String(), data.Coord)

	return mrc20UtxoList, nil
}
