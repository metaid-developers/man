package main

import (
	"fmt"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"
)

type Mrc20Utxo struct {
	Tick        string `json:"tick"`
	Mrc20Id     string `json:"mrc20Id"`
	TxPoint     string `json:"txPoint"`
	ToAddress   string `json:"toAddress"`
	Status      int    `json:"status"`
	MrcOption   string `json:"mrcOption"`
	AmtChange   string `json:"amtChange"`
	BlockHeight int64  `json:"blockHeight"`
}

func main() {
	db, err := pebble.Open("../../man_base_data_pebble/mrc20/db", &pebble.Options{ReadOnly: true})
	if err != nil {
		fmt.Println("Open error:", err)
		return
	}
	defer db.Close()

	targetAddr := "195gtuVbW9DsKPnSZLrt9kdJrQmvrAt7e3"

	// 搜索 mrc20_in_ 前缀
	fmt.Println("=== Searching mrc20_in_ prefix ===")
	prefix := "mrc20_in_"
	iter, _ := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "~"),
	})

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if strings.Contains(key, targetAddr) {
			var utxo Mrc20Utxo
			sonic.Unmarshal(iter.Value(), &utxo)
			fmt.Printf("Found: key=%s\n", key)
			fmt.Printf("  Tick: %s, Status: %d, Amount: %s\n", utxo.Tick, utxo.Status, utxo.AmtChange)
			count++
		}
	}
	iter.Close()
	fmt.Printf("Total found in mrc20_in_: %d\n\n", count)

	// 搜索 mrc20_utxo_ 前缀
	fmt.Println("=== Searching mrc20_utxo_ prefix ===")
	prefix2 := "mrc20_utxo_"
	iter2, _ := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix2),
		UpperBound: []byte(prefix2 + "~"),
	})

	count2 := 0
	for iter2.First(); iter2.Valid(); iter2.Next() {
		var utxo Mrc20Utxo
		sonic.Unmarshal(iter2.Value(), &utxo)
		if utxo.ToAddress == targetAddr {
			fmt.Printf("Found: key=%s\n", string(iter2.Key()))
			fmt.Printf("  Tick: %s, Status: %d, Amount: %s, Option: %s\n", utxo.Tick, utxo.Status, utxo.AmtChange, utxo.MrcOption)
			count2++
		}
	}
	iter2.Close()
	fmt.Printf("Total found in mrc20_utxo_: %d\n", count2)

	// 搜索旧的 mrc20_addr_ 前缀
	fmt.Println("\n=== Searching old mrc20_addr_ prefix ===")
	prefix3 := "mrc20_addr_"
	iter3, _ := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix3),
		UpperBound: []byte(prefix3 + "~"),
	})

	count3 := 0
	for iter3.First(); iter3.Valid(); iter3.Next() {
		key := string(iter3.Key())
		if strings.Contains(key, targetAddr) {
			var utxo Mrc20Utxo
			sonic.Unmarshal(iter3.Value(), &utxo)
			fmt.Printf("Found: key=%s\n", key)
			fmt.Printf("  Tick: %s, Status: %d, Amount: %s\n", utxo.Tick, utxo.Status, utxo.AmtChange)
			count3++
		}
	}
	iter3.Close()
	fmt.Printf("Total found in mrc20_addr_: %d\n", count3)
}
