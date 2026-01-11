package dogecoin

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"log"
	"manindexer/common"
	"manindexer/pin"
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
)

var (
	client *rpcclient.Client
)

type DogecoinChain struct {
	IsTest bool
}

func (chain *DogecoinChain) InitChain() {
	doge := common.Config.Doge
	rpcConfig := &rpcclient.ConnConfig{
		Host:                 doge.RpcHost,
		User:                 doge.RpcUser,
		Pass:                 doge.RpcPass,
		HTTPPostMode:         doge.RpcHTTPPostMode, // Dogecoin core only supports HTTP POST mode
		DisableTLS:           doge.RpcDisableTLS,   // Dogecoin core does not provide TLS by default
		DisableAutoReconnect: false,
		DisableConnectOnNew:  false,
	}
	var err error
	client, err = rpcclient.New(rpcConfig, nil)
	if err != nil {
		panic(err)
	}
	log.Printf("Dogecoin RPC client initialized: %s", doge.RpcHost)
}

func (chain *DogecoinChain) GetBlock(blockHeight int64) (block interface{}, err error) {
	blockhash, err := client.GetBlockHash(blockHeight)
	if err != nil {
		return
	}

	// btcsuite's GetBlock has issues with Dogecoin (witness flag parsing)
	// Fall back to manual parsing via RPC
	msgBlock, err := client.GetBlock(blockhash)
	if err == nil {
		block = msgBlock
		return
	}

	msgBlock, err = chain.getBlockByRPC(blockhash)
	if err != nil {
		return
	}

	block = msgBlock
	return
}

func (chain *DogecoinChain) GetBlockTime(blockHeight int64) (timestamp int64, err error) {
	block, err := chain.GetBlock(blockHeight)
	if err != nil {
		return
	}
	b := block.(*wire.MsgBlock)
	timestamp = b.Header.Timestamp.Unix()
	return
}

func (chain *DogecoinChain) GetBlockByHash(hash string) (block *btcjson.GetBlockVerboseResult, err error) {
	blockhash, err := chainhash.NewHashFromStr(hash)
	if err != nil {
		return
	}
	block, err = client.GetBlockVerbose(blockhash)
	return
}

func (chain *DogecoinChain) GetTransaction(txId string) (tx interface{}, err error) {
	txHash, _ := chainhash.NewHashFromStr(txId)
	return client.GetRawTransaction(txHash)
}

func GetValueByTx(txId string, txIdx int) (value int64, err error) {
	txHash, _ := chainhash.NewHashFromStr(txId)
	tx, err := client.GetRawTransaction(txHash)
	if err != nil {
		return
	}
	value = tx.MsgTx().TxOut[txIdx].Value
	return
}

func (chain *DogecoinChain) GetInitialHeight() (height int64) {
	return common.Config.Doge.InitialHeight
}

func (chain *DogecoinChain) GetBestHeight() (height int64) {
	info, err := client.GetBlockChainInfo()
	if err != nil {
		//log.Printf("GetBlockChainInfo error: %v, trying GetBlockCount", err)
		height, err = client.GetBlockCount()
		if err == nil {
			//log.Printf("Dogecoin best height: %d", height)
			return height
		}
		//log.Printf("GetBlockCount error: %v", err)
		return 0
	}
	height = int64(info.Blocks)
	//log.Printf("Dogecoin best height: %d", height)
	return
}

func (chain *DogecoinChain) GetBlockMsg(height int64) (blockMsg *pin.BlockMsg) {
	blockhash, err := client.GetBlockHash(height)
	if err != nil {
		return
	}
	block, err := client.GetBlockVerbose(blockhash)
	if err != nil {
		return
	}
	blockMsg = &pin.BlockMsg{}
	blockMsg.BlockHash = block.Hash
	blockMsg.Target = block.MerkleRoot
	blockMsg.Weight = int64(block.Weight)
	blockMsg.Timestamp = time.Unix(block.Time, 0).Format("2006-01-02 15:04:05")
	blockMsg.Size = int64(block.Size)
	blockMsg.Transaction = block.Tx
	blockMsg.TransactionNum = len(block.Tx)
	return
}

func (chain *DogecoinChain) GetCreatorAddress(txHashStr string, idx uint32, netParams *chaincfg.Params) (address string) {
	txHash, err := chainhash.NewHashFromStr(txHashStr)
	if err != nil {
		return "errorAddr"
	}
	// Get commit tx
	tx, err := client.GetRawTransaction(txHash)
	if err != nil {
		return "errorAddr"
	}
	// Get commit tx first input
	inputHash := tx.MsgTx().TxIn[0].PreviousOutPoint.Hash
	inputIdx := tx.MsgTx().TxIn[0].PreviousOutPoint.Index
	inputTx, err := client.GetRawTransaction(&inputHash)
	if err != nil {
		return "errorAddr"
	}
	_, addresses, _, _ := txscript.ExtractPkScriptAddrs(inputTx.MsgTx().TxOut[inputIdx].PkScript, netParams)
	if len(addresses) > 0 {
		address = addresses[0].String()
	} else {
		address = "errorAddr"
	}
	return
}

func (chain *DogecoinChain) GetMempoolTransactionList() (list []interface{}, err error) {
	txIdList, err := client.GetRawMempool()
	if err != nil {
		return
	}
	for _, txHash := range txIdList {
		tx, err := client.GetRawTransaction(txHash)
		if err != nil {
			continue
		}
		list = append(list, tx.MsgTx())
	}
	return
}

func (chain *DogecoinChain) GetTxSizeAndFees(txHash string) (fee int64, size int64, blockHash string, err error) {
	hash, err := chainhash.NewHashFromStr(txHash)
	if err != nil {
		return
	}
	tx, err := client.GetRawTransactionVerbose(hash)
	if err != nil {
		return
	}
	var inputAmount int64
	for _, vin := range tx.Vin {
		inputTxHash, err := chainhash.NewHashFromStr(vin.Txid)
		if err != nil {
			continue
		}
		inputTx, err := client.GetRawTransactionVerbose(inputTxHash)
		if err != nil {
			continue
		}
		inputAmount += int64(inputTx.Vout[vin.Vout].Value * 1e8)
	}
	var outputAmount int64
	for _, vout := range tx.Vout {
		outputAmount += int64(vout.Value * 1e8)
	}
	fee = inputAmount - outputAmount
	size = int64(tx.Size)
	blockHash = tx.BlockHash
	return
}

// Helper function to parse chainhash, panic on error since this should not fail for valid block data
func mustParseChainhash(hashStr string) chainhash.Hash {
	hash, err := chainhash.NewHashFromStr(hashStr)
	if err != nil {
		panic(fmt.Sprintf("Invalid hash: %s, error: %v", hashStr, err))
	}
	return *hash
}

// Parse hex string to uint32 for the Bits field
func parseHexUint32(hexStr string) (uint32, error) {
	var val uint32
	_, err := fmt.Sscanf(hexStr, "%x", &val)
	if err != nil {
		return 0, fmt.Errorf("failed to parse hex uint32: %v", err)
	}
	return val, nil
}

// getBlockByRPC manually fetches block and individual transactions to avoid btcsuite parsing issues
func (chain *DogecoinChain) getBlockByRPC(blockhash *chainhash.Hash) (*wire.MsgBlock, error) {
	// Get block verbose first to get tx list
	blockVerbose, err := client.GetBlockVerbose(blockhash)
	if err != nil {
		return nil, fmt.Errorf("GetBlockVerbose failed: %v", err)
	}

	// Parse bits
	bits, err := parseHexUint32(blockVerbose.Bits)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bits: %v", err)
	}

	// Build MsgBlock
	msgBlock := &wire.MsgBlock{
		Header: wire.BlockHeader{
			Version:    blockVerbose.Version,
			PrevBlock:  mustParseChainhash(blockVerbose.PreviousHash),
			MerkleRoot: mustParseChainhash(blockVerbose.MerkleRoot),
			Timestamp:  time.Unix(blockVerbose.Time, 0),
			Bits:       bits,
			Nonce:      blockVerbose.Nonce,
		},
		Transactions: make([]*wire.MsgTx, 0, len(blockVerbose.Tx)),
	}

	// Get each transaction individually by txid
	for _, txid := range blockVerbose.Tx {
		txhash, err := chainhash.NewHashFromStr(txid)
		if err != nil {
			continue
		}

		txVerbose, err := client.GetRawTransactionVerbose(txhash)
		if err != nil {
			continue
		}

		tx, err := parseTxFromVerbose(txVerbose)
		if err != nil {
			continue
		}
		msgBlock.Transactions = append(msgBlock.Transactions, tx)
	}

	return msgBlock, nil
}

// Parse transaction from verbose JSON format to wire.MsgTx
func parseTxFromVerbose(txVerbose *btcjson.TxRawResult) (*wire.MsgTx, error) {
	// Use the Hex field which contains the raw transaction bytes
	txBytes, err := hex.DecodeString(txVerbose.Hex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode tx hex: %v", err)
	}

	// Deserialize the transaction without witness flag (Dogecoin doesn't support SegWit)
	tx := &wire.MsgTx{}
	err = tx.Deserialize(bytes.NewReader(txBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize tx: %v", err)
	}

	return tx, nil
}
