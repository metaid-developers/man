package main

import (
	"fmt"
	"os"
	"strings"

	"manindexer/common"
	"manindexer/man"
)

// 手动索引单个区块的 MRC20 交易
func main() {
	chainName := "doge"
	blockHeight := int64(6050998)
	configFile := "./config_dev_main.toml"

	// 从命令行参数读取
	for _, arg := range os.Args[1:] {
		if strings.HasPrefix(arg, "-chain=") {
			chainName = strings.TrimPrefix(arg, "-chain=")
		}
		if strings.HasPrefix(arg, "-height=") {
			fmt.Sscanf(strings.TrimPrefix(arg, "-height="), "%d", &blockHeight)
		}
		if strings.HasPrefix(arg, "-config=") {
			configFile = strings.TrimPrefix(arg, "-config=")
		}
	}

	fmt.Printf("=== 手动索引区块 ===\n")
	fmt.Printf("Chain: %s\n", chainName)
	fmt.Printf("Height: %d\n", blockHeight)
	fmt.Printf("Config: %s\n", configFile)

	// 重置 os.Args 避免 InitConfig 解析出错
	os.Args = []string{os.Args[0]}

	// 初始化
	common.InitConfig(configFile)
	common.TestNet = "0"
	common.Chain = chainName
	man.InitAdapter(common.Chain, common.Db, common.TestNet, common.Server)

	// 直接调用索引
	fmt.Printf("\n开始索引区块 %d...\n", blockHeight)
	man.PebbleStore.DoIndexerRun(chainName, blockHeight, false)
	fmt.Println("索引完成！")

	// 查看索引的数据
	result, err := man.PebbleStore.Database.GetlBlocksDB(chainName, int(blockHeight))
	if err != nil {
		fmt.Printf("❌ 获取区块数据失败: %v\n", err)
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
	}
}
