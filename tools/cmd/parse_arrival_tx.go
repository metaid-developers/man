package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"manindexer/adapter/dogecoin"
	"manindexer/common"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/shopspring/decimal"
)

// Mrc20ArrivalDataFlex 使用 json.RawMessage 来处理 amount 字段可能是 string 或 number 的情况
type Mrc20ArrivalDataFlex struct {
	AssetOutpoint string          `json:"assetOutpoint"`
	Amount        json.RawMessage `json:"amount"` // 使用 RawMessage 来灵活处理
	TickId        string          `json:"tickId"`
	LocationIndex int             `json:"locationIndex"`
	Metadata      string          `json:"metadata"`
}

func main() {
	// 初始化配置 - 使用主配置
	common.InitConfig("../../config_dev_main.toml")

	// 初始化 Dogecoin 链
	chain := dogecoin.DogecoinChain{}
	chain.InitChain()

	// 要解析的交易 ID
	txID := "fbe035e525eeb352ef00f9f10c3f7767f0529a1dc5695994bcf0169f6363e707"

	fmt.Println("========================================")
	fmt.Printf("解析 Dogecoin Arrival 交易: %s\n", txID)
	fmt.Println("========================================\n")

	// 获取交易
	txResult, err := chain.GetTransaction(txID)
	if err != nil {
		log.Fatal("Failed to get transaction:", err)
	}

	txObj := txResult.(*btcutil.Tx)
	tx := txObj.MsgTx()

	fmt.Printf("交易版本: %d\n", tx.Version)
	fmt.Printf("输入数量: %d\n", len(tx.TxIn))
	fmt.Printf("输出数量: %d\n\n", len(tx.TxOut))

	// 解析输出
	fmt.Println("分析交易输出：")
	fmt.Println("----------------------------------------")
	for i, txOut := range tx.TxOut {
		fmt.Printf("输出 #%d: value=%d satoshis, scriptLen=%d\n", i, txOut.Value, len(txOut.PkScript))
	}

	// 使用 PIN 解析器解析
	fmt.Println("\n使用 PIN 解析器解析交易：")
	fmt.Println("----------------------------------------")

	// 创建索引器来解析
	indexer := dogecoin.Indexer{}
	indexer.ChainName = "doge"
	indexer.ChainParams = "mainnet"
	indexer.InitIndexer()

	pinInscription := indexer.CatchPinsByTx(tx, 0, 0, "", "", 0)
	fmt.Printf("解析到的 PIN 数量: %d\n", len(pinInscription))

	for i, p := range pinInscription {
		fmt.Printf("\nPIN #%d:\n", i)
		fmt.Printf("  ID: %s\n", p.Id)
		fmt.Printf("  Path: %s\n", p.Path)
		fmt.Printf("  Operation: %s\n", p.Operation)
		fmt.Printf("  ContentType: %s\n", p.ContentType)
		fmt.Printf("  ContentLength: %d\n", len(p.ContentBody))

		if p.Path == "/ft/mrc20/arrival" {
			fmt.Printf("\n  这是一个 MRC20 Arrival PIN！\n")
			fmt.Printf("  原始内容 (string): %s\n", string(p.ContentBody))
			fmt.Printf("  原始内容 (hex): %s\n", hex.EncodeToString(p.ContentBody))

			// 尝试解析 JSON
			fmt.Println("\n  尝试解析 JSON：")

			// 使用灵活的结构体
			var dataFlex Mrc20ArrivalDataFlex
			err := json.Unmarshal(p.ContentBody, &dataFlex)
			if err != nil {
				fmt.Printf("  JSON 解析失败: %v\n", err)
			} else {
				fmt.Printf("  AssetOutpoint: %s\n", dataFlex.AssetOutpoint)
				fmt.Printf("  TickId: %s\n", dataFlex.TickId)
				fmt.Printf("  LocationIndex: %d\n", dataFlex.LocationIndex)
				fmt.Printf("  Amount (raw): %s\n", string(dataFlex.Amount))

				// 解析 amount - 可能是字符串或数字
				amountStr := string(dataFlex.Amount)
				// 如果是带引号的字符串，去掉引号
				if len(amountStr) > 0 && amountStr[0] == '"' {
					var s string
					json.Unmarshal(dataFlex.Amount, &s)
					amountStr = s
				}
				fmt.Printf("  Amount (parsed string): %s\n", amountStr)

				// 尝试转换为 decimal
				amt, err := decimal.NewFromString(amountStr)
				if err != nil {
					fmt.Printf("  Amount decimal 转换失败: %v\n", err)
				} else {
					fmt.Printf("  Amount (decimal): %s\n", amt.String())
				}
			}

			// 也打印原始 JSON 格式化后的样子
			fmt.Println("\n  格式化的 JSON:")
			var jsonObj map[string]interface{}
			json.Unmarshal(p.ContentBody, &jsonObj)
			prettyJSON, _ := json.MarshalIndent(jsonObj, "  ", "  ")
			fmt.Printf("  %s\n", string(prettyJSON))
		}
	}

	// 打印输入脚本信息
	fmt.Println("\n分析交易输入：")
	fmt.Println("----------------------------------------")
	for i, txIn := range tx.TxIn {
		fmt.Printf("\n输入 #%d:\n", i)
		fmt.Printf("  Previous TxID: %s\n", txIn.PreviousOutPoint.Hash.String())
		fmt.Printf("  Previous Vout: %d\n", txIn.PreviousOutPoint.Index)
		fmt.Printf("  ScriptSig 长度: %d bytes\n", len(txIn.SignatureScript))

		// 检查是否是 metaid 协议
		scriptHex := hex.EncodeToString(txIn.SignatureScript)
		if len(scriptHex) > 100 {
			fmt.Printf("  ScriptSig (前100字符): %s...\n", scriptHex[:100])

			// 检查是否有 metaid 标识 (6d6574616964)
			metaidHex := "6d6574616964"
			if idx := findSubstring(scriptHex, metaidHex); idx != -1 {
				fmt.Printf("  发现 metaid 协议标识在位置: %d\n", idx)
			}
		}
	}
}

func findSubstring(s, sub string) int {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
