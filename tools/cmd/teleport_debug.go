package main

import (
	"fmt"
	"log"
	"manindexer/common"
	"manindexer/man"
	"manindexer/mrc20"
	"os"

	"github.com/bytedance/sonic"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage:")
		fmt.Println("  teleport_debug list-pending       # 列出所有 pending teleport")
		fmt.Println("  teleport_debug list-arrivals      # 列出所有 arrival")
		fmt.Println("  teleport_debug check-match        # 检查匹配关系")
		fmt.Println("  teleport_debug fix-pending        # 尝试修复卡住的 pending teleport")
		os.Exit(1)
	}

	command := os.Args[1]

	// 初始化配置
	common.InitConfig()
	err := man.InitPebble()
	if err != nil {
		log.Fatal("InitPebble error:", err)
	}
	defer man.PebbleStore.Database.MrcDb.Close()

	switch command {
	case "list-pending":
		listPendingTeleports()
	case "list-arrivals":
		listArrivals()
	case "check-match":
		checkMatching()
	case "fix-pending":
		fixPendingTeleports()
	default:
		fmt.Println("Unknown command:", command)
		os.Exit(1)
	}
}

func listPendingTeleports() {
	fmt.Println("=== Pending Teleports ===")

	// 遍历所有 pending_teleport 记录
	prefix := []byte("pending_teleport_")
	iter := man.PebbleStore.Database.MrcDb.NewIter(nil)
	defer iter.Close()

	count := 0
	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if len(key) < len("pending_teleport_") || key[:17] != "pending_teleport_" {
			break
		}

		// 跳过索引键
		if len(key) > 23 && key[:23] == "pending_teleport_coord_" {
			continue
		}

		var pending mrc20.PendingTeleport
		err := sonic.Unmarshal(iter.Value(), &pending)
		if err != nil {
			log.Printf("Unmarshal error for key %s: %v", key, err)
			continue
		}

		count++
		fmt.Printf("\n[%d] PinId: %s\n", count, pending.PinId)
		fmt.Printf("    TxId: %s\n", pending.TxId)
		fmt.Printf("    Coord: %s\n", pending.Coord)
		fmt.Printf("    TickId: %s\n", pending.TickId)
		fmt.Printf("    Amount: %s\n", pending.Amount)
		fmt.Printf("    AssetOutpoint: %s\n", pending.AssetOutpoint)
		fmt.Printf("    SourceChain: %s -> TargetChain: %s\n", pending.SourceChain, pending.TargetChain)
		fmt.Printf("    FromAddress: %s\n", pending.FromAddress)
		fmt.Printf("    Status: %d (0=pending, 1=completed, -1=invalid)\n", pending.Status)
		fmt.Printf("    RetryCount: %d\n", pending.RetryCount)
		fmt.Printf("    BlockHeight: %d\n", pending.BlockHeight)

		// 检查 UTXO 状态
		utxo, err := man.PebbleStore.GetMrc20UtxoByTxPoint(pending.AssetOutpoint, false)
		if err != nil {
			fmt.Printf("    ⚠️  UTXO Not Found: %s\n", pending.AssetOutpoint)
		} else {
			statusStr := "Available"
			if utxo.Status == mrc20.UtxoStatusTeleportPending {
				statusStr = "TeleportPending"
			} else if utxo.Status == mrc20.UtxoStatusSpent {
				statusStr = "Spent"
			} else if utxo.Status == mrc20.UtxoStatusTransferPending {
				statusStr = "TransferPending"
			}
			fmt.Printf("    UTXO Status: %d (%s)\n", utxo.Status, statusStr)
			fmt.Printf("    UTXO Msg: %s\n", utxo.Msg)
		}

		// 检查是否有对应的 arrival
		arrival, err := man.PebbleStore.GetMrc20ArrivalByPinId(pending.Coord)
		if err != nil {
			fmt.Printf("    ❌ Arrival NOT FOUND for coord: %s\n", pending.Coord)
		} else {
			statusStr := "Pending"
			if arrival.Status == 1 {
				statusStr = "Completed"
			} else if arrival.Status == -1 {
				statusStr = "Invalid"
			}
			fmt.Printf("    ✅ Arrival FOUND: %s (Status: %d - %s)\n", arrival.PinId, arrival.Status, statusStr)
			fmt.Printf("       Arrival AssetOutpoint: %s\n", arrival.AssetOutpoint)
			fmt.Printf("       Arrival ToAddress: %s\n", arrival.ToAddress)

			// 检查 assetOutpoint 是否匹配
			if arrival.AssetOutpoint != pending.AssetOutpoint {
				fmt.Printf("       ⚠️  AssetOutpoint MISMATCH! Arrival expects: %s, Pending has: %s\n",
					arrival.AssetOutpoint, pending.AssetOutpoint)
			}
		}
	}

	fmt.Printf("\n总计: %d 个 pending teleport 记录\n", count)
}

func listArrivals() {
	fmt.Println("=== All Arrivals ===")

	prefix := []byte("arrival_")
	iter := man.PebbleStore.Database.MrcDb.NewIter(nil)
	defer iter.Close()

	count := 0
	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if len(key) < 8 || key[:8] != "arrival_" {
			break
		}

		// 跳过索引键
		if len(key) > 13 && key[:13] == "arrival_asset" {
			continue
		}
		if len(key) > 15 && key[:15] == "arrival_pending" {
			continue
		}

		var arrival mrc20.Mrc20Arrival
		err := sonic.Unmarshal(iter.Value(), &arrival)
		if err != nil {
			log.Printf("Unmarshal error for key %s: %v", key, err)
			continue
		}

		count++
		statusStr := "Pending"
		if arrival.Status == 1 {
			statusStr = "Completed"
		} else if arrival.Status == -1 {
			statusStr = "Invalid"
		}

		fmt.Printf("\n[%d] PinId: %s\n", count, arrival.PinId)
		fmt.Printf("    TxId: %s\n", arrival.TxId)
		fmt.Printf("    AssetOutpoint: %s\n", arrival.AssetOutpoint)
		fmt.Printf("    TickId: %s\n", arrival.TickId)
		fmt.Printf("    Tick: %s\n", arrival.Tick)
		fmt.Printf("    Amount: %s\n", arrival.Amount.String())
		fmt.Printf("    Chain: %s (from %s)\n", arrival.Chain, arrival.SourceChain)
		fmt.Printf("    ToAddress: %s\n", arrival.ToAddress)
		fmt.Printf("    Status: %d (%s)\n", arrival.Status, statusStr)
		fmt.Printf("    BlockHeight: %d\n", arrival.BlockHeight)

		// 检查是否有对应的 pending teleport
		pending, err := man.PebbleStore.GetPendingTeleportByCoord(arrival.PinId)
		if err != nil {
			fmt.Printf("    ⚠️  No Pending Teleport waiting for this arrival\n")
		} else {
			fmt.Printf("    ✅ Pending Teleport FOUND: %s (Status: %d)\n", pending.PinId, pending.Status)
		}
	}

	fmt.Printf("\n总计: %d 个 arrival 记录\n", count)
}

func checkMatching() {
	fmt.Println("=== Checking Teleport-Arrival Matching ===\n")

	// 收集所有 pending teleport
	prefix := []byte("pending_teleport_")
	iter := man.PebbleStore.Database.MrcDb.NewIter(nil)
	defer iter.Close()

	pendings := make(map[string]*mrc20.PendingTeleport)
	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if len(key) < 17 || key[:17] != "pending_teleport_" {
			break
		}
		if len(key) > 23 && key[:23] == "pending_teleport_coord_" {
			continue
		}

		var pending mrc20.PendingTeleport
		err := sonic.Unmarshal(iter.Value(), &pending)
		if err != nil {
			continue
		}
		pendings[pending.Coord] = &pending
	}
	iter.Close()

	// 收集所有 arrival
	arrPrefix := []byte("arrival_")
	iter2 := man.PebbleStore.Database.MrcDb.NewIter(nil)
	defer iter2.Close()

	arrivals := make(map[string]*mrc20.Mrc20Arrival)
	for iter2.SeekGE(arrPrefix); iter2.Valid(); iter2.Next() {
		key := string(iter2.Key())
		if len(key) < 8 || key[:8] != "arrival_" {
			break
		}
		if len(key) > 13 && (key[:13] == "arrival_asset" || key[:15] == "arrival_pending") {
			continue
		}

		var arrival mrc20.Mrc20Arrival
		err := sonic.Unmarshal(iter2.Value(), &arrival)
		if err != nil {
			continue
		}
		arrivals[arrival.PinId] = &arrival
	}

	fmt.Printf("Found %d pending teleports and %d arrivals\n\n", len(pendings), len(arrivals))

	// 检查匹配关系
	matched := 0
	unmatched := 0

	for coord, pending := range pendings {
		if arrival, ok := arrivals[coord]; ok {
			matched++
			fmt.Printf("✅ MATCHED: Coord=%s\n", coord)
			fmt.Printf("   Pending: %s (Status: %d)\n", pending.PinId, pending.Status)
			fmt.Printf("   Arrival: %s (Status: %d)\n", arrival.PinId, arrival.Status)

			// 检查是否应该完成但还没完成
			if pending.Status == 0 && arrival.Status == 0 {
				fmt.Printf("   ⚠️  BOTH PENDING - Should be processed!\n")
				fmt.Printf("      AssetOutpoint Match: %t\n", pending.AssetOutpoint == arrival.AssetOutpoint)
			}
		} else {
			unmatched++
			fmt.Printf("❌ UNMATCHED Pending: Coord=%s, PinId=%s\n", coord, pending.PinId)
			fmt.Printf("   Arrival with PinId=%s NOT FOUND\n", coord)

			// 检查是否有相似的 arrival (assetOutpoint 匹配)
			for _, arr := range arrivals {
				if arr.AssetOutpoint == pending.AssetOutpoint {
					fmt.Printf("   ⚠️  FOUND Arrival with SAME AssetOutpoint but DIFFERENT PinId:\n")
					fmt.Printf("      Arrival PinId: %s (expected: %s)\n", arr.PinId, coord)
					break
				}
			}
		}
		fmt.Println()
	}

	fmt.Printf("Summary: %d matched, %d unmatched\n", matched, unmatched)
}

func fixPendingTeleports() {
	fmt.Println("=== Fixing Pending Teleports ===\n")
	fmt.Println("This will attempt to process all pending teleports that have matching arrivals.\n")

	// TODO: 实现修复逻辑
	// 1. 查找所有 status=0 的 pending teleport
	// 2. 检查是否有对应的 arrival
	// 3. 如果有，调用 processPendingTeleportForArrival

	fmt.Println("Fix functionality not yet implemented.")
	fmt.Println("Please use 'check-match' to diagnose the issue first.")
}
