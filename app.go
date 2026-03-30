package main

import (
	"embed"
	"fmt"
	"log"
	"manindexer/api"
	"manindexer/common"
	"manindexer/man"
	"manindexer/pebblestore"
	"time"
)

var (
	//go:embed web/static/* web/template/* web/template/home/* web/template/public/*
	f embed.FS
)

func main() {
	banner := `
    __  ___  ___     _   __
   /  |/  / /   |   / | / / v2.0.1
  / /|_/ / / /| |  /  |/ / 
 / /  / / / ___ | / /|  /  
/_/  /_/ /_/  |_|/_/ |_/                   
 `
	fmt.Println(banner)
	configPath := "./config.toml"
	if common.ConfigFile != "" {
		configPath = common.ConfigFile
	}
	common.InitConfig(configPath)
	man.InitAdapter(common.Chain, common.Db, common.TestNet, common.Server)

	// 显示运行模式
	modeInfo := ""
	if common.Config.Sync.Mrc20Only {
		modeInfo = ",mode=MRC20-ONLY"
	}
	log.Printf("ManIndex,chain=%s,fullnode=%v,test=%s,db=%s,server=%s,config=%s%s,metaChain=%s", common.Chain, common.Config.Sync.IsFullNode, common.TestNet, common.Db, common.Server, common.ConfigFile, modeInfo, common.Config.Statistics.MetaChainHost)

	if common.Server == "1" {
		go api.Start(f)
	}
	go man.ZmqRun()

	// 首次启动时先执行 MRC20 补索引，确保 MRC20 进度追上主索引
	man.Mrc20CatchUpRun()

	// Execute statistics（MRC20 Only 模式下跳过），只启动一次避免 goroutine 堆积
	if !common.Config.Sync.Mrc20Only {
		go pebblestore.StatMetaId(man.PebbleStore.Database)
		go pebblestore.StatPinSort(man.PebbleStore.Database)
	}

	for {
		man.IndexerRun(common.TestNet)

		// 每轮主索引后再次检查是否需要补索引
		// （正常情况下不需要，除非配置文件修改了 mrc20Height）
		man.Mrc20CatchUpRun()

		man.CheckNewBlock()
		time.Sleep(time.Second * 10)
	}
}
