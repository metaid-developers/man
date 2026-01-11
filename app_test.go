package main

import (
	"fmt"
	"manindexer/common"
	"manindexer/man"
	"strings"
	"testing"
)

func TestIndexer(t *testing.T) {
	common.InitConfig("./config_regtest.toml")
	common.TestNet = "0"
	common.Chain = "mvc"
	man.InitAdapter(common.Chain, common.Db, common.TestNet, common.Server)
	man.PebbleStore.DoIndexerRun("mvc", 145049, false)
}

func TestDogeIndexer(t *testing.T) {
	common.InitConfig("./config_doge.toml")
	common.TestNet = "0"
	common.Chain = "doge"
	man.InitAdapter(common.Chain, common.Db, common.TestNet, common.Server)

	// 测试区块 6005462（包含交易 426c69a2d179fc5cbfa2caff7f0b0ee4f90c596fada8eb077ce17fffec01a433）
	blockHeight := int64(6005462)

	fmt.Printf("\n========================================\n")
	fmt.Printf("测试 Dogecoin 区块 %d\n", blockHeight)
	fmt.Printf("预期交易: 426c69a2d179fc5cbfa2caff7f0b0ee4f90c596fada8eb077ce17fffec01a433\n")
	fmt.Printf("========================================\n")

	man.PebbleStore.DoIndexerRun("doge", blockHeight, false)
	fmt.Println("索引完成！")

	// 查看索引的数据
	result, err := man.PebbleStore.Database.GetlBlocksDB("doge", int(blockHeight))
	if err != nil {
		fmt.Printf("❌ 获取区块数据失败: %v\n", err)
		t.Fatalf("获取区块数据失败: %v", err)
	} else if result != nil && *result != "" {
		fmt.Printf("✅ 区块 %d 包含的 PIN IDs: %s\n", blockHeight, *result)

		// 获取并显示 PIN 详情
		pinIds := strings.Split(*result, ",")
		fmt.Printf("\n找到 %d 个 PIN:\n", len(pinIds))
		for i, pinId := range pinIds {
			pinData, err := man.PebbleStore.Database.GetPinByKey(pinId)
			if err == nil && pinData != nil {
				fmt.Printf("\n[PIN #%d] ID: %s\n", i+1, pinId)
				fmt.Printf("数据长度: %d bytes\n", len(pinData))
				// 只显示前200个字符
				if len(pinData) > 200 {
					fmt.Printf("数据预览: %s...\n", string(pinData[:200]))
				} else {
					fmt.Printf("数据: %s\n", string(pinData))
				}
			} else {
				fmt.Printf("\n[PIN #%d] ID: %s (无法读取数据)\n", i+1, pinId)
			}
		}
	} else {
		fmt.Printf("❌ 区块 %d 没有找到 PIN 数据\n", blockHeight)
		t.Errorf("预期找到 PIN 数据但实际为空")
	}
}
func TestPebbleDb(t *testing.T) {
	common.InitConfig("./config_regtest.toml")
	common.TestNet = "2"
	man.InitAdapter(common.Chain, common.Db, common.TestNet, common.Server)

	it, err := man.PebbleStore.Database.PinSort.NewIter(nil)
	if err != nil {
		// 处理错误
		return
	}
	defer it.Close()

	for it.First(); it.Valid(); it.Next() {
		key := it.Key()
		// dbkey := strings.Split(string(key), "&")
		// if dbkey[0] == common.GetMetaIdByAddress("/protocols/simplenote") {
		// 	fmt.Println("Path Key:", string(key))
		// }
		fmt.Println(" Key:", string(key))
	}
}

func TestMempoolDelete(t *testing.T) {
	common.InitConfig("./config_regtest.toml")
	common.TestNet = "2"
	man.InitAdapter(common.Chain, common.Db, common.TestNet, common.Server)

	man.DeleteMempoolData(438, "btc")
}
