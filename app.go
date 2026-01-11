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
	//go:embed web/static/* web/template/*
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
	log.Printf("ManIndex,chain=%s,fullnode=%v,test=%s,db=%s,server=%s,config=%s,metaChain=%s", common.Chain, common.Config.Sync.IsFullNode, common.TestNet, common.Db, common.Server, common.ConfigFile, common.Config.Statistics.MetaChainHost)
	if common.Server == "1" {
		go api.Start(f)
	}
	go man.ZmqRun()
	for {
		// Execute statistics
		go pebblestore.StatMetaId(man.PebbleStore.Database)
		go pebblestore.StatPinSort(man.PebbleStore.Database)
		man.IndexerRun(common.TestNet)
		man.CheckNewBlock()
		time.Sleep(time.Second * 10)
	}
}
