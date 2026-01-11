package dogecoin

import (
	"encoding/hex"
	"errors"
	"fmt"
	"manindexer/common"
	"manindexer/mrc20"
	"manindexer/pin"
	"strconv"
	"strings"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
)

var PopCutNum int = 0
var netParams *chaincfg.Params

type Indexer struct {
	ChainParams string
	Block       interface{}
	PopCutNum   int
	ChainName   string
}

func (indexer *Indexer) InitIndexer() {
	// Dogecoin network parameters
	// Note: Dogecoin uses similar address format to Bitcoin but has different magic bytes
	// We'll use regtest params as default and can be extended for mainnet/testnet
	switch indexer.ChainParams {
	case "mainnet":
		netParams = &DogeMainNetParams
	case "testnet":
		netParams = &DogeTestNetParams
	case "regtest":
		netParams = &DogeRegTestParams
	default:
		netParams = &DogeRegTestParams
	}
	PopCutNum = common.Config.Doge.PopCutNum
}

func (indexer *Indexer) GetCurHeight() (height int64) {
	return
}

func (indexer *Indexer) GetAddress(pkScript []byte) (address string) {
	_, addresses, _, _ := txscript.ExtractPkScriptAddrs(pkScript, netParams)
	if len(addresses) > 0 {
		address = addresses[0].String()
	}
	return
}

func (indexer *Indexer) CatchPins(blockHeight int64) (pinInscriptions *[]*pin.PinInscription, txInList *[]string, creatorMap *map[string]string) {
	m := make(map[string]string)
	creatorMap = &m
	var txInListLocal []string
	var pinInscriptionsLocal []*pin.PinInscription
	txInList = &txInListLocal
	pinInscriptions = &pinInscriptionsLocal

	chain := DogecoinChain{}
	blockMsg, err := chain.GetBlock(blockHeight)
	if err != nil {
		return
	}
	indexer.Block = blockMsg
	block := blockMsg.(*wire.MsgBlock)
	timestamp := block.Header.Timestamp.Unix()
	blockHash := block.BlockHash().String()
	merkleRoot := block.Header.MerkleRoot.String()
	for i, tx := range block.Transactions {
		for _, in := range tx.TxIn {
			id := common.ConcatBytesOptimized([]string{in.PreviousOutPoint.Hash.String(), ":", strconv.FormatUint(uint64(in.PreviousOutPoint.Index), 10)}, "")
			*txInList = append(*txInList, id)
		}
		// Dogecoin inscriptions are in ScriptSig (P2SH), not in Witness
		txPins := indexer.CatchPinsByTx(tx, blockHeight, timestamp, blockHash, merkleRoot, i)
		for _, p := range txPins {
			validErr := pin.ManValidator(p)
			if validErr == nil {
				*pinInscriptions = append(*pinInscriptions, p)
			}
		}
	}
	return
}

func (indexer *Indexer) CatchMempoolPins(txList []interface{}) (pinInscriptions []*pin.PinInscription, txInList []string) {
	timestamp := time.Now().Unix()
	blockHash := "none"
	merkleRoot := "none"
	for i, item := range txList {
		tx := item.(*wire.MsgTx)
		for _, in := range tx.TxIn {
			id := fmt.Sprintf("%s:%d", in.PreviousOutPoint.Hash.String(), in.PreviousOutPoint.Index)
			txInList = append(txInList, id)
		}
		txPins := indexer.CatchPinsByTx(tx, -1, timestamp, blockHash, merkleRoot, i)
		if len(txPins) > 0 {
			pinInscriptions = append(pinInscriptions, txPins...)
		}
	}
	return
}

func (indexer *Indexer) CatchTransfer(idMap map[string]string) (trasferMap map[string]*pin.PinTransferInfo) {
	trasferMap = make(map[string]*pin.PinTransferInfo)
	block := indexer.Block.(*wire.MsgBlock)
	for _, tx := range block.Transactions {
		for _, in := range tx.TxIn {
			id := fmt.Sprintf("%s:%d", in.PreviousOutPoint.Hash.String(), in.PreviousOutPoint.Index)
			if fromAddress, ok := idMap[id]; ok {
				info, err := indexer.GetOWnerAddress(id, tx)
				if err == nil && info != nil {
					info.FromAddress = fromAddress
					trasferMap[id] = info
				}
			}
		}
	}
	return
}

func (indexer *Indexer) CatchNativeMrc20Transfer(blockHeight int64, utxoList []*mrc20.Mrc20Utxo, mrc20TransferPinTx map[string]struct{}) (savelist []*mrc20.Mrc20Utxo) {
	pointMap := make(map[string][]*mrc20.Mrc20Utxo)
	for _, u := range utxoList {
		if u.MrcOption == "deploy" {
			continue
		}
		pointMap[u.TxPoint] = append(pointMap[u.TxPoint], u)
	}
	block := indexer.Block.(*wire.MsgBlock)
	t := block.Header.Timestamp.Unix()
	for _, tx := range block.Transactions {
		_, ok := mrc20TransferPinTx[tx.TxHash().String()]
		if ok {
			continue
		}
		list := indexer.createMrc20NativeTransfer(tx, blockHeight, t, pointMap)
		if len(list) > 0 {
			savelist = append(savelist, list...)
		}
	}
	return
}

func (indexer *Indexer) CatchMempoolNativeMrc20Transfer(txList []interface{}, utxoList []*mrc20.Mrc20Utxo, mrc20TransferPinTx map[string]struct{}) (savelist []*mrc20.Mrc20Utxo) {
	pointMap := make(map[string][]*mrc20.Mrc20Utxo)
	for _, u := range utxoList {
		if u.MrcOption == "deploy" {
			continue
		}
		pointMap[u.TxPoint] = append(pointMap[u.TxPoint], u)
	}
	t := time.Now().Unix()
	for _, item := range txList {
		tx := item.(*wire.MsgTx)
		_, ok := mrc20TransferPinTx[tx.TxHash().String()]
		if ok {
			continue
		}
		list := indexer.createMrc20NativeTransfer(tx, -1, t, pointMap)
		if len(list) > 0 {
			savelist = append(savelist, list...)
		}
	}
	return
}

func (indexer *Indexer) createMrc20NativeTransfer(tx *wire.MsgTx, blockHeight int64, blockTime int64, pointMap map[string][]*mrc20.Mrc20Utxo) (mrc20Utxolist []*mrc20.Mrc20Utxo) {
	keyMap := make(map[string]*mrc20.Mrc20Utxo)
	for _, in := range tx.TxIn {
		id := fmt.Sprintf("%s:%d", in.PreviousOutPoint.Hash.String(), in.PreviousOutPoint.Index)
		if v, ok := pointMap[id]; ok {
			for _, utxo := range v {
				send := *utxo
				send.Status = -1
				send.OperationTx = tx.TxHash().String()
				mrc20Utxolist = append(mrc20Utxolist, &send)
				key := send.Mrc20Id
				_, find := keyMap[key]
				if find {
					keyMap[key].AmtChange = keyMap[key].AmtChange.Add(send.AmtChange)
				} else {
					recive := *utxo
					recive.MrcOption = "native-transfer"
					recive.FromAddress = recive.ToAddress
					recive.ToAddress = indexer.GetAddress(tx.TxOut[0].PkScript)
					recive.BlockHeight = blockHeight
					recive.TxPoint = fmt.Sprintf("%s:%d", tx.TxHash().String(), 0)
					recive.Timestamp = blockTime
					recive.Chain = "doge"
					recive.Msg = "native-transfer"
					recive.OperationTx = tx.TxHash().String()
					keyMap[key] = &recive
				}
			}
		}
	}
	for _, u := range keyMap {
		mrc20Utxolist = append(mrc20Utxolist, u)
	}
	return
}

func (indexer *Indexer) GetOWnerAddress(inputId string, tx *wire.MsgTx) (info *pin.PinTransferInfo, err error) {
	info = &pin.PinTransferInfo{}
	firstInputId := fmt.Sprintf("%s:%d", tx.TxIn[0].PreviousOutPoint.Hash, tx.TxIn[0].PreviousOutPoint.Index)
	if len(tx.TxIn) == 1 || firstInputId == inputId {
		class, addresses, _, _ := txscript.ExtractPkScriptAddrs(tx.TxOut[0].PkScript, netParams)
		if len(addresses) > 0 {
			info.Address = addresses[0].String()
		} else if class.String() == "nulldata" {
			info.Address = hex.EncodeToString(tx.TxOut[0].PkScript)
		}
		info.Location = fmt.Sprintf("%s:%d:%d", tx.TxHash().String(), 0, 0)
		info.Offset = 0
		info.Output = fmt.Sprintf("%s:%d", tx.TxHash().String(), 0)
		info.OutputValue = tx.TxOut[0].Value
		return
	}
	totalOutputValue := int64(0)
	for _, out := range tx.TxOut {
		totalOutputValue += out.Value
	}
	inputValue := int64(0)
	for _, in := range tx.TxIn {
		id := fmt.Sprintf("%s:%d", in.PreviousOutPoint.Hash, in.PreviousOutPoint.Index)
		if id == inputId {
			break
		}
		value, err1 := GetValueByTx(in.PreviousOutPoint.Hash.String(), int(in.PreviousOutPoint.Index))
		if err1 != nil {
			err = errors.New("get value error")
			return
		}
		inputValue += value
		if inputValue > totalOutputValue {
			return
		}
	}
	outputValue := int64(0)
	for i, out := range tx.TxOut {
		outputValue += out.Value
		if outputValue > inputValue {
			class, addresses, _, _ := txscript.ExtractPkScriptAddrs(out.PkScript, netParams)
			if len(addresses) > 0 {
				info.Address = addresses[0].String()
			} else if class.String() == "nulldata" {
				info.Address = hex.EncodeToString(out.PkScript)
			}
			info.Output = fmt.Sprintf("%s:%d", tx.TxHash().String(), i)
			info.Location = fmt.Sprintf("%s:%d", info.Output, out.Value-(outputValue-inputValue))
			info.Offset = uint64(i)
			info.OutputValue = out.Value
			break
		}
	}
	return
}

// CatchPinsByTx parses Dogecoin inscriptions from ScriptSig (P2SH redeem script)
func (indexer *Indexer) CatchPinsByTx(msgTxInf interface{}, blockHeight int64, timestamp int64, blockHash string, merkleRoot string, txIndex int) (pinInscriptions []*pin.PinInscription) {
	msgTx := msgTxInf.(*wire.MsgTx)
	chain := DogecoinChain{}
	haveOpReturn := false

	// Check OP_RETURN outputs first
	for i, out := range msgTx.TxOut {
		class, _, _, _ := txscript.ExtractPkScriptAddrs(out.PkScript, netParams)
		if class.String() == "nonstandard" {
			pinInscription := indexer.ParseOpReturnPin(out.PkScript)
			if pinInscription == nil {
				continue
			}
			_, host, path := pin.ValidHostPath(pinInscription.Path)
			if common.CheckBlockedHost(host) {
				continue
			}
			if !common.CheckHost(host) {
				continue
			}
			address, outIdx, locationIdx := indexer.GetOpReturnPinOwner(msgTx)
			if address == "" {
				continue
			}
			txHash := msgTx.TxHash().String()
			id := fmt.Sprintf("%si%d", txHash, outIdx)
			metaId := common.GetMetaIdByAddress(address)
			contentTypeDetect := common.DetectContentType(&pinInscription.ContentBody)
			pop := ""
			if merkleRoot != "" && blockHash != "" {
				pop, _ = common.GenPop(id, merkleRoot, blockHash)
			}
			popLv, _ := pin.PopLevelCount(indexer.ChainName, pop)
			creator := address
			if common.Config.Sync.IsFullNode {
				creator = chain.GetCreatorAddress(msgTx.TxIn[0].PreviousOutPoint.Hash.String(), msgTx.TxIn[0].PreviousOutPoint.Index, netParams)
			}
			pinInscriptions = append(pinInscriptions, &pin.PinInscription{
				ChainName:          indexer.ChainName,
				Id:                 id,
				MetaId:             metaId,
				Number:             0,
				Address:            address,
				InitialOwner:       address,
				CreateAddress:      creator,
				CreateMetaId:       common.GetMetaIdByAddress(creator),
				Timestamp:          timestamp,
				GenesisHeight:      blockHeight,
				GenesisTransaction: txHash,
				Output:             fmt.Sprintf("%s:%d", txHash, outIdx),
				OutputValue:        msgTx.TxOut[outIdx].Value,
				TxInIndex:          uint32(i - 1),
				Offset:             uint64(outIdx),
				TxIndex:            txIndex,
				Operation:          pinInscription.Operation,
				Location:           fmt.Sprintf("%s:%d:%d", txHash, outIdx, locationIdx),
				Path:               strings.TrimSpace(path),
				OriginalPath:       strings.TrimSpace(pinInscription.Path),
				ParentPath:         strings.TrimSpace(pinInscription.ParentPath),
				Encryption:         pinInscription.Encryption,
				Version:            pinInscription.Version,
				ContentType:        pinInscription.ContentType,
				ContentTypeDetect:  contentTypeDetect,
				ContentBody:        pinInscription.ContentBody,
				ContentLength:      pinInscription.ContentLength,
				ContentSummary:     getContentSummary(pinInscription, id, contentTypeDetect),
				Pop:                pop,
				PopLv:              popLv,
				PoPScore:           pin.GetPoPScore(pop, int64(popLv), common.Config.Doge.PopCutNum),
				PoPScoreV1:         pin.GetPoPScoreV1(pop, popLv),
				DataValue:          pin.RarityScoreBinary(indexer.ChainName, pop),
				Mrc20MintId:        []string{},
				Host:               host,
			})
			haveOpReturn = true
			break
		}
	}
	if haveOpReturn {
		return
	}

	// Dogecoin: Parse inscriptions from ScriptSig (P2SH redeem script)
	// Unlike Bitcoin's SegWit which uses witness data, Dogecoin uses legacy P2SH
	for i, input := range msgTx.TxIn {
		index := i
		// Check if ScriptSig exists
		if len(input.SignatureScript) == 0 {
			continue
		}

		// Try parsing direct format first (metaid data at the beginning of ScriptSig)
		pinInscription := indexer.ParsePinFromDirectScriptSig(input.SignatureScript)
		if pinInscription == nil {
			// Parse ScriptSig to extract the redeem script
			// ScriptSig format for P2SH: <signature> <redeemScript>
			tokenizer := txscript.MakeScriptTokenizer(0, input.SignatureScript)
			var redeemScript []byte
			var lastData []byte

			// Iterate through ScriptSig to find the redeem script (last push data)
			for tokenizer.Next() {
				if len(tokenizer.Data()) > 0 {
					lastData = tokenizer.Data()
				}
			}

			// The last pushed data in ScriptSig should be the redeem script
			if len(lastData) > 0 {
				redeemScript = lastData
			} else {
				continue
			}

			// Parse the redeem script for inscription data
			pinInscription = indexer.ParsePinFromRedeemScript(redeemScript)
		}

		if pinInscription == nil {
			continue
		}

		address, outIdx, locationIdx := indexer.GetPinOwner(msgTx, index)
		id := fmt.Sprintf("%si%d", msgTx.TxHash().String(), outIdx)
		metaId := common.GetMetaIdByAddress(address)
		contentTypeDetect := common.DetectContentType(&pinInscription.ContentBody)
		pop := ""
		if merkleRoot != "" && blockHash != "" {
			if merkleRoot == "none" && blockHash == "none" {
				pop = "none"
			} else {
				pop, _ = common.GenPop(id, merkleRoot, blockHash)
			}
		}
		popLv, _ := pin.PopLevelCount(indexer.ChainName, pop)
		creator := address
		if common.Config.Sync.IsFullNode {
			creator = chain.GetCreatorAddress(input.PreviousOutPoint.Hash.String(), input.PreviousOutPoint.Index, netParams)
		}
		_, host, path := pin.ValidHostPath(pinInscription.Path)
		pinInscriptions = append(pinInscriptions, &pin.PinInscription{
			ChainName:          indexer.ChainName,
			Id:                 id,
			MetaId:             metaId,
			Number:             0,
			Address:            address,
			InitialOwner:       address,
			CreateAddress:      creator,
			CreateMetaId:       common.GetMetaIdByAddress(creator),
			Timestamp:          timestamp,
			GenesisHeight:      blockHeight,
			GenesisTransaction: msgTx.TxHash().String(),
			Output:             fmt.Sprintf("%s:%d", msgTx.TxHash().String(), outIdx),
			OutputValue:        msgTx.TxOut[outIdx].Value,
			TxInIndex:          uint32(index),
			Offset:             uint64(outIdx),
			TxIndex:            txIndex,
			Operation:          pinInscription.Operation,
			Location:           fmt.Sprintf("%s:%d:%d", msgTx.TxHash().String(), outIdx, locationIdx),
			Path:               strings.TrimSpace(path),
			OriginalPath:       strings.TrimSpace(pinInscription.Path),
			ParentPath:         strings.TrimSpace(pinInscription.ParentPath),
			Encryption:         pinInscription.Encryption,
			Version:            pinInscription.Version,
			ContentType:        pinInscription.ContentType,
			ContentTypeDetect:  contentTypeDetect,
			ContentBody:        pinInscription.ContentBody,
			ContentLength:      pinInscription.ContentLength,
			ContentSummary:     getContentSummary(pinInscription, id, contentTypeDetect),
			Pop:                pop,
			PopLv:              popLv,
			PoPScore:           pin.GetPoPScore(pop, int64(popLv), common.Config.Doge.PopCutNum),
			PoPScoreV1:         pin.GetPoPScoreV1(pop, popLv),
			DataValue:          pin.RarityScoreBinary(indexer.ChainName, pop),
			Mrc20MintId:        []string{},
			Host:               host,
		})
	}
	return
}

func getParentPath(path string) (parentPath string) {
	arr := strings.Split(path, "/")
	if len(arr) < 3 {
		return
	}
	parentPath = strings.Join(arr[0:len(arr)-1], "/")
	return
}

func getContentSummary(pinode *pin.PersonalInformationNode, id string, contentTypeDetect string) (content string) {
	if contentTypeDetect[0:4] != "text" {
		return fmt.Sprintf("/content/%s", id)
	} else {
		c := string(pinode.ContentBody)
		if len(c) > 150 {
			return c[0:150]
		} else {
			return string(pinode.ContentBody)
		}
	}
}

func (indexer *Indexer) ParseOpReturnPin(pkScript []byte) (pinode *pin.PersonalInformationNode) {
	tokenizer := txscript.MakeScriptTokenizer(0, pkScript)
	for tokenizer.Next() {
		if tokenizer.Opcode() == txscript.OP_RETURN {
			if !tokenizer.Next() || hex.EncodeToString(tokenizer.Data()) != common.Config.ProtocolID {
				return
			}
			pinode = indexer.parseOpReturnOnePin(&tokenizer)
		}
	}
	return
}

func (indexer *Indexer) parseOpReturnOnePin(tokenizer *txscript.ScriptTokenizer) *pin.PersonalInformationNode {
	var infoList [][]byte
	for tokenizer.Next() {
		infoList = append(infoList, tokenizer.Data())
	}
	if err := tokenizer.Err(); err != nil {
		return nil
	}
	if len(infoList) < 1 {
		return nil
	}

	pinode := pin.PersonalInformationNode{}
	pinode.Operation = strings.ToLower(string(infoList[0]))
	if pinode.Operation == "init" {
		pinode.Path = "/"
		return &pinode
	}
	if len(infoList) < 6 && pinode.Operation != "revoke" {
		return nil
	}
	if pinode.Operation == "revoke" && len(infoList) < 5 {
		return nil
	}
	pinode.Path = strings.ToLower(string(infoList[1]))
	pinode.ParentPath = getParentPath(pinode.Path)
	encryption := "0"
	if infoList[2] != nil {
		encryption = string(infoList[2])
	}
	pinode.Encryption = encryption
	version := "0"
	if infoList[3] != nil {
		version = string(infoList[3])
	}
	pinode.Version = version
	contentType := "application/json"
	if infoList[4] != nil {
		contentType = strings.ToLower(string(infoList[4]))
	}
	pinode.ContentType = contentType
	var body []byte
	for i := 5; i < len(infoList); i++ {
		body = append(body, infoList[i]...)
	}
	pinode.ContentBody = body
	pinode.ContentLength = uint64(len(body))
	return &pinode
}

func (indexer *Indexer) GetOpReturnPinOwner(tx *wire.MsgTx) (address string, outIdx int, locationIdx int64) {
	for i, out := range tx.TxOut {
		class, addresses, _, _ := txscript.ExtractPkScriptAddrs(out.PkScript, netParams)
		if class.String() != "nonstandard" && len(addresses) > 0 {
			outIdx = i
			address = addresses[0].String()
			locationIdx = 0
		}
	}
	return
}

func (indexer *Indexer) GetPinOwner(tx *wire.MsgTx, inIdx int) (address string, outIdx int, locationIdx int64) {
	if len(tx.TxIn) == 1 || len(tx.TxOut) == 1 || inIdx == 0 {
		_, addresses, _, _ := txscript.ExtractPkScriptAddrs(tx.TxOut[0].PkScript, netParams)
		if len(addresses) > 0 {
			address = addresses[0].String()
		}
		return
	}
	inputValue := int64(0)
	for i, in := range tx.TxIn {
		if i == inIdx {
			break
		}
		value, err := GetValueByTx(in.PreviousOutPoint.Hash.String(), int(in.PreviousOutPoint.Index))
		if err != nil {
			return
		}
		inputValue += value
	}
	outputValue := int64(0)
	for x, out := range tx.TxOut {
		outputValue += out.Value
		if outputValue > inputValue {
			locationIdx = outputValue - inputValue
			_, addresses, _, _ := txscript.ExtractPkScriptAddrs(out.PkScript, netParams)
			if len(addresses) > 0 {
				address = addresses[0].String()
				outIdx = x
			}
			break
		}
	}
	return
}

// ParsePinFromRedeemScript parses Dogecoin inscription data from P2SH redeem script
// Dogecoin inscription format in redeem script:
// <pubkey> OP_CHECKSIGVERIFY OP_FALSE OP_IF "metaid" <operation> <path> <encryption> <version> <contentType> <content> [more content...] OP_ENDIF
func (indexer *Indexer) ParsePinFromRedeemScript(redeemScript []byte) (pinode *pin.PersonalInformationNode) {
	tokenizer := txscript.MakeScriptTokenizer(0, redeemScript)

	// Skip the pubkey and OP_CHECKSIGVERIFY at the beginning
	if !tokenizer.Next() {
		return nil
	}
	if !tokenizer.Next() || tokenizer.Opcode() != txscript.OP_CHECKSIGVERIFY {
		return nil
	}

	// Look for inscription envelope: OP_FALSE OP_IF
	if !tokenizer.Next() || tokenizer.Opcode() != txscript.OP_FALSE {
		return nil
	}
	if !tokenizer.Next() || tokenizer.Opcode() != txscript.OP_IF {
		return nil
	}

	// Check for "metaid" marker (protocol identifier)
	if !tokenizer.Next() {
		return nil
	}
	marker := string(tokenizer.Data())
	if marker != "metaid" {
		return nil
	}

	// Parse inscription data following the standard metaid protocol format
	// Format: "metaid" <operation> <path> <encryption> <version> <contentType> <content> [more content...]
	// Collect all data fields until OP_ENDIF
	var infoList [][]byte
	for tokenizer.Next() {
		if tokenizer.Opcode() == txscript.OP_ENDIF {
			break
		}
		infoList = append(infoList, tokenizer.Data())
		if len(tokenizer.Data()) > 520 {
			return nil
		}
	}

	// No OP_ENDIF
	if tokenizer.Opcode() != txscript.OP_ENDIF {
		return nil
	}

	// Error occurred
	if err := tokenizer.Err(); err != nil {
		return nil
	}

	if len(infoList) < 1 {
		return nil
	}

	pinode = &pin.PersonalInformationNode{}
	pinode.Operation = strings.ToLower(string(infoList[0]))

	// Special case: init operation
	if pinode.Operation == "init" {
		pinode.Path = "/"
		return pinode
	}

	// Standard validation: need at least 6 fields (operation, path, encryption, version, contentType, content)
	if len(infoList) < 6 && pinode.Operation != "revoke" {
		return nil
	}
	if pinode.Operation == "revoke" && len(infoList) < 5 {
		return nil
	}

	pinode.Path = strings.ToLower(string(infoList[1]))
	pinode.ParentPath = getParentPath(pinode.Path)

	encryption := "0"
	if infoList[2] != nil {
		encryption = string(infoList[2])
	}
	pinode.Encryption = encryption

	version := "0"
	if infoList[3] != nil {
		version = string(infoList[3])
	}
	pinode.Version = version

	contentType := "application/json"
	if infoList[4] != nil {
		contentType = strings.ToLower(string(infoList[4]))
	}
	pinode.ContentType = contentType

	// Collect all remaining fields as content body
	var body []byte
	for i := 5; i < len(infoList); i++ {
		body = append(body, infoList[i]...)
	}
	pinode.ContentBody = body
	pinode.ContentLength = uint64(len(body))

	return pinode
}

// ParsePinFromDirectScriptSig parses Dogecoin inscription data directly from ScriptSig
// This format has metaid protocol data at the beginning of ScriptSig without OP_IF/OP_ENDIF wrapper
// Format: <pushdata metaid> <pushdata operation> <pushdata contentType> <pushdata encryption> <pushdata version> <pushdata contentTypeBody> <pushdata content> <signature> <pubkey> ...
func (indexer *Indexer) ParsePinFromDirectScriptSig(scriptSig []byte) (pinode *pin.PersonalInformationNode) {
	if len(scriptSig) < 7 {
		return nil
	}

	tokenizer := txscript.MakeScriptTokenizer(0, scriptSig)
	var infoList [][]byte

	// Collect all push data from ScriptSig
	for tokenizer.Next() {
		if len(tokenizer.Data()) > 0 {
			infoList = append(infoList, tokenizer.Data())
		}
	}

	if err := tokenizer.Err(); err != nil {
		return nil
	}

	if len(infoList) < 6 {
		return nil
	}

	// Check if first field is "metaid" protocol marker
	if string(infoList[0]) != "metaid" {
		return nil
	}

	pinode = &pin.PersonalInformationNode{}
	pinode.Operation = strings.ToLower(string(infoList[1]))

	// Special case: init operation
	if pinode.Operation == "init" {
		pinode.Path = "/"
		return pinode
	}

	// Validate operation
	if pinode.Operation != "create" && pinode.Operation != "modify" && pinode.Operation != "revoke" {
		return nil
	}

	// For revoke, we need at least 5 fields; for others, at least 6
	if pinode.Operation == "revoke" && len(infoList) < 5 {
		return nil
	}
	if pinode.Operation != "revoke" && len(infoList) < 7 {
		return nil
	}

	// Parse content type (field 2)
	contentType := "application/json"
	if len(infoList) > 2 && len(infoList[2]) > 0 {
		contentType = strings.ToLower(string(infoList[2]))
	}
	pinode.ContentType = contentType

	// Parse encryption (field 3)
	encryption := "0"
	if len(infoList) > 3 && len(infoList[3]) > 0 {
		encryption = string(infoList[3])
	}
	pinode.Encryption = encryption

	// Parse version (field 4)
	version := "0"
	if len(infoList) > 4 && len(infoList[4]) > 0 {
		version = string(infoList[4])
	}
	pinode.Version = version

	// Parse content type body/path (field 5)
	if len(infoList) > 5 && len(infoList[5]) > 0 {
		// This could be path or contentTypeBody, try to determine
		fieldStr := string(infoList[5])
		// If it looks like a path (starts with /), use it as path
		if strings.HasPrefix(fieldStr, "/") {
			pinode.Path = strings.ToLower(fieldStr)
			pinode.ParentPath = getParentPath(pinode.Path)
		} else {
			// Otherwise treat as content type body and use default path
			pinode.Path = "/info"
			pinode.ParentPath = "/"
		}
	}

	// Parse content body (field 6 onwards, before signature)
	// The signature is typically 71-73 bytes (DER encoded), and pubkey is 33 or 65 bytes
	// We need to stop before signature data
	var body []byte
	for i := 6; i < len(infoList); i++ {
		data := infoList[i]
		// Stop if this looks like a signature (starts with 0x30 and is 70-73 bytes)
		// or a pubkey (33 or 65 bytes starting with 0x02, 0x03, 0x04)
		if len(data) >= 70 && len(data) <= 73 && data[0] == 0x30 {
			break
		}
		if (len(data) == 33 || len(data) == 65) && (data[0] == 0x02 || data[0] == 0x03 || data[0] == 0x04) {
			break
		}
		body = append(body, data...)
	}

	pinode.ContentBody = body
	pinode.ContentLength = uint64(len(body))

	return pinode
}

func (indexer *Indexer) GetBlockTxHash(blockHeight int64) (txhashList []string, pinIdList []string) {
	chain := DogecoinChain{}
	blockMsg, err := chain.GetBlock(blockHeight)
	if err != nil {
		return
	}
	block := blockMsg.(*wire.MsgBlock)
	for _, tx := range block.Transactions {
		for i := range tx.Copy().TxOut {
			var pinId strings.Builder
			pinId.WriteString(tx.TxHash().String())
			pinId.WriteString("i")
			pinId.WriteString(strconv.Itoa(i))
			pinIdList = append(pinIdList, pinId.String())
		}
		txhashList = append(txhashList, tx.TxHash().String())
	}
	return
}
