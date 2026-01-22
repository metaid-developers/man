package main

import (
	"fmt"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"
)

func main() {
	db, err := pebble.Open("./man_base_data_pebble/mrc20/db", &pebble.Options{})
	if err != nil {
		fmt.Println("Open db error:", err)
		return
	}
	defer db.Close()

	arrivalPinId := "6e6c8ee0d7fdec6d03df5e8119542fa721a1dcfa121b54804b574a7b6dab0156i0"
	transferPinId := "9495241af74000812e230bd2c1e2cad756840cb93e3deb21764d7d4904809147i0"

	// 检查 arrival
	fmt.Println("=== Checking Arrival ===")
	key := fmt.Sprintf("arrival_%s", arrivalPinId)
	value, closer, err := db.Get([]byte(key))
	if err != nil {
		fmt.Println("Arrival not found:", err)
	} else {
		var data map[string]interface{}
		sonic.Unmarshal(value, &data)
		fmt.Printf("Arrival data: %+v\n", data)
		closer.Close()
	}

	// 检查 pending teleport
	fmt.Println("\n=== Checking Pending Teleport ===")
	pendingKey := fmt.Sprintf("pending_teleport_%s", transferPinId)
	value, closer, err = db.Get([]byte(pendingKey))
	if err != nil {
		fmt.Println("Pending teleport not found:", err)
	} else {
		var data map[string]interface{}
		sonic.Unmarshal(value, &data)
		fmt.Printf("Pending teleport: %+v\n", data)
		closer.Close()
	}

	// 检查 pending teleport by coord
	fmt.Println("\n=== Checking Pending Teleport by Coord ===")
	coordKey := fmt.Sprintf("pending_teleport_coord_%s", arrivalPinId)
	value, closer, err = db.Get([]byte(coordKey))
	if err != nil {
		fmt.Println("Pending teleport coord not found:", err)
	} else {
		fmt.Printf("Pending teleport coord -> %s\n", string(value))
		closer.Close()
	}

	// 检查 teleport 记录
	fmt.Println("\n=== Checking Teleport Record ===")
	teleportKey := fmt.Sprintf("teleport_%s", transferPinId)
	value, closer, err = db.Get([]byte(teleportKey))
	if err != nil {
		fmt.Println("Teleport record not found:", err)
	} else {
		var data map[string]interface{}
		sonic.Unmarshal(value, &data)
		fmt.Printf("Teleport record: %+v\n", data)
		closer.Close()
	}

	// 检查 teleport by coord
	fmt.Println("\n=== Checking Teleport by Coord ===")
	teleportCoordKey := fmt.Sprintf("teleport_coord_%s", arrivalPinId)
	value, closer, err = db.Get([]byte(teleportCoordKey))
	if err != nil {
		fmt.Println("Teleport coord not found:", err)
	} else {
		fmt.Printf("Teleport coord -> %s\n", string(value))
		closer.Close()
	}

	// 检查源 UTXO
	fmt.Println("\n=== Checking Source UTXO ===")
	assetOutpoint := "0b00124b8292d36212d4131ae4334daa442768864eb9e4ee6bfa61a5f01845eb:0"
	utxoKey := fmt.Sprintf("mrc20_utxo_%s", assetOutpoint)
	value, closer, err = db.Get([]byte(utxoKey))
	if err != nil {
		fmt.Println("Source UTXO not found:", err)
	} else {
		var data map[string]interface{}
		sonic.Unmarshal(value, &data)
		fmt.Printf("Source UTXO: %+v\n", data)
		closer.Close()
	}

	// 检查 PIN 数据库中的 transfer PIN
	fmt.Println("\n=== Checking PIN Database ===")
	pinDbs := []string{
		"./man_base_data_pebble/pins_0/db",
		"./man_base_data_pebble/pins_1/db",
		"./man_base_data_pebble/pins_2/db",
		"./man_base_data_pebble/pins_3/db",
		"./man/man_base_data_pebble/pins_0/db",
		"./man/man_base_data_pebble/pins_1/db",
	}

	transferTxId := strings.TrimSuffix(transferPinId, "i0")

	for _, dbPath := range pinDbs {
		pinDb, err := pebble.Open(dbPath, &pebble.Options{ReadOnly: true})
		if err != nil {
			continue
		}

		// 尝试查找 PIN
		pinKey := fmt.Sprintf("pin_%s", transferPinId)
		value, closer, err := pinDb.Get([]byte(pinKey))
		if err == nil {
			fmt.Printf("Found PIN in %s: %s\n", dbPath, string(value)[:200])
			closer.Close()
			pinDb.Close()
			break
		}

		// 尝试按 txid 查找
		iter, _ := pinDb.NewIter(&pebble.IterOptions{
			LowerBound: []byte("pin_"),
			UpperBound: []byte("pin_~"),
		})
		for iter.First(); iter.Valid(); iter.Next() {
			if strings.Contains(string(iter.Key()), transferTxId) {
				fmt.Printf("Found PIN by txid in %s: key=%s\n", dbPath, string(iter.Key()))
				break
			}
		}
		iter.Close()
		pinDb.Close()
	}
}
