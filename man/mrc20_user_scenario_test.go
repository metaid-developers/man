package man

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"manindexer/common"
	"manindexer/mrc20"
	"manindexer/pebblestore"

	"github.com/shopspring/decimal"
)

// TestUserReportedTeleportScenario 测试用户报告的完整场景：
// 初始: 发送方有 700+38=738 MAN (btc 链)
// 操作1: transfer 700 → 600+100 (自转账拆分)
// 操作2: teleport 100 到 doge 链
// 预期结果（所有交易出块后）:
//   - 发送方 (btc): balance=638, pendingOut=0, pendingIn=0
//   - 接收方 (doge): balance=100 (新增), pendingIn=0
func TestUserReportedTeleportScenario(t *testing.T) {
	initTeleportTestConfig()

	tmpDir, err := os.MkdirTemp("", "mrc20_user_scenario_*")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	db, err := pebblestore.NewDataBase(tmpDir, 1)
	if err != nil {
		t.Fatalf("初始化数据库失败: %v", err)
	}
	defer db.Close()

	PebbleStore = &PebbleData{Database: db}
	common.Config.Module = []string{"mrc20"}

	// 测试参数
	sourceChain := "btc"
	targetChain := "doge"
	senderAddress := "bc1q_sender_address"
	receiverAddress := "D_receiver_address"
	tickId := "0c5f575e39b1e640a0457df39939562fac899da658effaa14b740b072f863d13i0"
	tickName := "MAN"

	// 创建 tick
	tick := mrc20.Mrc20DeployInfo{
		Mrc20Id: tickId,
		Tick:    tickName,
		Chain:   sourceChain,
	}
	PebbleStore.SaveMrc20Tick([]mrc20.Mrc20DeployInfo{tick})

	fmt.Println("========================================")
	fmt.Println("用户报告场景测试：Transfer + Teleport")
	fmt.Println("========================================")

	// ==================== Step 1: 创建初始 UTXO ====================
	fmt.Println("\n📝 Step 1: 创建初始 UTXO (700 + 38)")

	utxo700 := mrc20.Mrc20Utxo{
		Tick:        tickName,
		Mrc20Id:     tickId,
		TxPoint:     "init_tx_700:0",
		FromAddress: "",
		ToAddress:   senderAddress,
		AmtChange:   decimal.NewFromInt(700),
		Status:      mrc20.UtxoStatusAvailable,
		Chain:       sourceChain,
		BlockHeight: 100,
	}

	utxo38 := mrc20.Mrc20Utxo{
		Tick:        tickName,
		Mrc20Id:     tickId,
		TxPoint:     "init_tx_38:0",
		FromAddress: "",
		ToAddress:   senderAddress,
		AmtChange:   decimal.NewFromInt(38),
		Status:      mrc20.UtxoStatusAvailable,
		Chain:       sourceChain,
		BlockHeight: 100,
	}

	PebbleStore.SaveMrc20Pin([]mrc20.Mrc20Utxo{utxo700, utxo38})

	// 初始化 AccountBalance
	PebbleStore.UpdateMrc20AccountBalance(
		sourceChain, senderAddress, tickId, tickName,
		decimal.NewFromInt(738), // Balance = 738
		decimal.Zero,            // PendingOut = 0
		decimal.Zero,            // PendingIn = 0
		2,                       // UtxoCount = 2
		"init", 100, 0,
	)

	balance, _ := PebbleStore.GetMrc20AccountBalance(sourceChain, senderAddress, tickId)
	fmt.Printf("  初始余额: Balance=%s, PendingOut=%s, UtxoCount=%d\n",
		balance.Balance.String(), balance.PendingOut.String(), balance.UtxoCount)

	// ==================== Step 2: Mempool - Transfer 700 → 600+100 ====================
	fmt.Println("\n📝 Step 2: [Mempool] Transfer 700 → 600+100 (自转账拆分)")

	// 2.1 原 700 UTXO 状态变成 TransferPending
	utxo700.Status = mrc20.UtxoStatusTransferPending
	utxo700.OperationTx = "split_tx"
	PebbleStore.UpdateMrc20Utxo([]*mrc20.Mrc20Utxo{&utxo700}, true)

	// 2.2 创建新的 600 和 100 UTXO（mempool）
	utxo600 := mrc20.Mrc20Utxo{
		Tick:        tickName,
		Mrc20Id:     tickId,
		TxPoint:     "split_tx:0",
		FromAddress: senderAddress,
		ToAddress:   senderAddress,
		AmtChange:   decimal.NewFromInt(600),
		Status:      mrc20.UtxoStatusAvailable,
		Chain:       sourceChain,
		BlockHeight: -1, // mempool
		OperationTx: "split_tx",
	}

	utxo100 := mrc20.Mrc20Utxo{
		Tick:        tickName,
		Mrc20Id:     tickId,
		TxPoint:     "split_tx:1",
		FromAddress: senderAddress,
		ToAddress:   senderAddress,
		AmtChange:   decimal.NewFromInt(100),
		Status:      mrc20.UtxoStatusAvailable,
		Chain:       sourceChain,
		BlockHeight: -1, // mempool
		OperationTx: "split_tx",
	}

	PebbleStore.SaveMrc20Pin([]mrc20.Mrc20Utxo{utxo600, utxo100})

	// 2.3 更新 AccountBalance（自转账 mempool）
	// Balance -= 700 (原 UTXO 被花费), PendingOut += 700, UtxoCount--
	PebbleStore.UpdateMrc20AccountBalance(
		sourceChain, senderAddress, tickId, tickName,
		decimal.NewFromInt(-700), // Balance -= 700
		decimal.NewFromInt(700),  // PendingOut += 700
		decimal.Zero,
		-1, // UtxoCount--
		"split_tx", -1, 0,
	)

	// 2.4 保存 TransferPendingIn（自转账接收方）
	transferPendingIn := &mrc20.TransferPendingIn{
		TxPoint:     "split_tx:0",
		TxId:        "split_tx",
		ToAddress:   senderAddress,
		TickId:      tickId,
		Tick:        tickName,
		Amount:      decimal.NewFromInt(700), // 总转入金额
		Chain:       sourceChain,
		FromAddress: senderAddress,
		TxType:      "native_transfer",
		BlockHeight: -1,
	}
	PebbleStore.SaveTransferPendingIn(transferPendingIn)

	balance, _ = PebbleStore.GetMrc20AccountBalance(sourceChain, senderAddress, tickId)
	fmt.Printf("  Transfer mempool 后: Balance=%s, PendingOut=%s\n",
		balance.Balance.String(), balance.PendingOut.String())

	// ==================== Step 3: Mempool - Teleport 100 ====================
	fmt.Println("\n📝 Step 3: [Mempool] Teleport 100 到 doge 链")

	// 3.1 将 100 UTXO 状态变成 TeleportPending
	utxo100.Status = mrc20.UtxoStatusTeleportPending
	utxo100.OperationTx = "teleport_tx"
	PebbleStore.UpdateMrc20Utxo([]*mrc20.Mrc20Utxo{&utxo100}, true)

	// 3.2 保存 PendingTeleport
	pending := &mrc20.PendingTeleport{
		PinId:         "pin_teleport_txi0",
		TxId:          "teleport_tx",
		Coord:         "pin_arrival_txi0",
		TickId:        tickId,
		Amount:        "100",
		AssetOutpoint: utxo100.TxPoint,
		TargetChain:   targetChain,
		FromAddress:   senderAddress,
		SourceChain:   sourceChain,
		BlockHeight:   -1,
		Status:        0,
	}
	PebbleStore.SavePendingTeleport(pending)

	// 3.3 更新 AccountBalance: Balance -= 100, PendingOut += 100
	PebbleStore.UpdateMrc20AccountBalance(
		sourceChain, senderAddress, tickId, tickName,
		decimal.NewFromInt(-100), // Balance -= 100
		decimal.NewFromInt(100),  // PendingOut += 100
		decimal.Zero,
		-1, // UtxoCount--
		"teleport_tx", -1, 0,
	)

	// 3.4 保存 Arrival（mempool 阶段）
	arrival := &mrc20.Mrc20Arrival{
		PinId:         "pin_arrival_txi0",
		TxId:          "arrival_tx",
		AssetOutpoint: utxo100.TxPoint,
		Amount:        decimal.NewFromInt(100),
		TickId:        tickId,
		Tick:          tickName,
		LocationIndex: 0,
		ToAddress:     receiverAddress,
		Chain:         targetChain,
		SourceChain:   sourceChain,
		Status:        mrc20.ArrivalStatusPending,
		BlockHeight:   -1, // mempool
	}
	PebbleStore.SaveMrc20Arrival(arrival)

	// 3.5 保存 TeleportPendingIn（接收方的 PendingIn）
	teleportPendingIn := &mrc20.TeleportPendingIn{
		Coord:       "pin_arrival_txi0",
		ToAddress:   receiverAddress,
		TickId:      tickId,
		Tick:        tickName,
		Amount:      decimal.NewFromInt(100),
		Chain:       targetChain,
		SourceChain: sourceChain,
		FromAddress: senderAddress,
		BlockHeight: -1,
	}
	PebbleStore.SaveTeleportPendingIn(teleportPendingIn)

	// 3.6 更新接收方 AccountBalance: PendingIn += 100
	PebbleStore.UpdateMrc20AccountBalance(
		targetChain, receiverAddress, tickId, tickName,
		decimal.Zero,            // Balance 不变
		decimal.Zero,            // PendingOut 不变
		decimal.NewFromInt(100), // PendingIn += 100
		0,                       // UtxoCount 不变
		"arrival_tx", -1, 0,
	)

	balance, _ = PebbleStore.GetMrc20AccountBalance(sourceChain, senderAddress, tickId)
	fmt.Printf("  Teleport mempool 后 (发送方): Balance=%s, PendingOut=%s\n",
		balance.Balance.String(), balance.PendingOut.String())

	receiverBalance, _ := PebbleStore.GetMrc20AccountBalance(targetChain, receiverAddress, tickId)
	if receiverBalance != nil {
		fmt.Printf("  Teleport mempool 后 (接收方): Balance=%s, PendingIn=%s\n",
			receiverBalance.Balance.String(), receiverBalance.PendingIn.String())
	}

	// ==================== Step 4: 区块确认 - Transfer 700 → 600+100 ====================
	fmt.Println("\n📝 Step 4: [区块确认] Transfer 700 → 600+100")

	// 4.1 删除原 700 UTXO（已花费）
	PebbleStore.DeleteMrc20Utxo(utxo700.TxPoint, senderAddress, tickId)

	// 4.2 更新 600 和 100 UTXO 的 BlockHeight
	utxo600.BlockHeight = 200
	utxo100.BlockHeight = 200 // 注意：此时 utxo100 仍是 TeleportPending 状态
	PebbleStore.UpdateMrc20Utxo([]*mrc20.Mrc20Utxo{&utxo600, &utxo100}, false)

	// 4.3 更新 AccountBalance（Transfer 确认）
	// Balance += 700（新 UTXO 确认）, PendingOut -= 700, UtxoCount += 2
	// 注意：这里 utxo100 虽然出块，但仍是 TeleportPending，所以只计 utxo600 的 UtxoCount
	PebbleStore.UpdateMrc20AccountBalance(
		sourceChain, senderAddress, tickId, tickName,
		decimal.NewFromInt(700),  // Balance += 700（新 UTXO 确认）
		decimal.NewFromInt(-700), // PendingOut -= 700
		decimal.Zero,
		2, // UtxoCount += 2（但实际上 utxo100 是 TeleportPending，应该只 +1）
		"split_tx", 200, 0,
	)

	// 4.4 删除 TransferPendingIn
	PebbleStore.DeleteTransferPendingIn("split_tx:0", senderAddress)

	balance, _ = PebbleStore.GetMrc20AccountBalance(sourceChain, senderAddress, tickId)
	fmt.Printf("  Transfer 确认后: Balance=%s, PendingOut=%s, UtxoCount=%d\n",
		balance.Balance.String(), balance.PendingOut.String(), balance.UtxoCount)

	// ==================== Step 5: 区块确认 - Teleport + Arrival ====================
	fmt.Println("\n📝 Step 5: [区块确认] Teleport + Arrival")

	// 5.1 获取源 UTXO 状态
	sourceUtxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(utxo100.TxPoint, false)
	fmt.Printf("  源 UTXO 状态: Status=%d (1=TeleportPending)\n", sourceUtxo.Status)

	// 5.2 更新 arrival 状态
	arrival.BlockHeight = 201
	arrival.Status = mrc20.ArrivalStatusCompleted
	PebbleStore.SaveMrc20Arrival(arrival)

	// 5.3 删除源 UTXO（teleport 完成）
	PebbleStore.DeleteMrc20Utxo(utxo100.TxPoint, senderAddress, tickId)

	// 5.4 创建目标链 UTXO
	targetUtxo := mrc20.Mrc20Utxo{
		Tick:        tickName,
		Mrc20Id:     tickId,
		TxPoint:     "arrival_tx:0",
		FromAddress: senderAddress,
		ToAddress:   receiverAddress,
		AmtChange:   decimal.NewFromInt(100),
		Status:      mrc20.UtxoStatusAvailable,
		Chain:       targetChain,
		BlockHeight: 201,
		MrcOption:   mrc20.OptionTeleportTransfer,
	}
	PebbleStore.SaveMrc20Pin([]mrc20.Mrc20Utxo{targetUtxo})

	// 5.5 更新发送方 AccountBalance（Teleport 完成）
	// 根据 executeTeleportTransfer 的逻辑：
	// 如果 sourceUtxo.Status == TeleportPending: PendingOut -= amount, Balance 不变
	if sourceUtxo.Status == mrc20.UtxoStatusTeleportPending {
		fmt.Println("  → 源 UTXO 是 TeleportPending 状态，执行 PendingOut -= 100")
		PebbleStore.UpdateMrc20AccountBalance(
			sourceChain, senderAddress, tickId, tickName,
			decimal.Zero,             // Balance 不变
			decimal.NewFromInt(-100), // PendingOut -= 100
			decimal.Zero,
			0, // UtxoCount 不变
			"teleport_tx", 201, 0,
		)
	}

	// 5.6 更新接收方 AccountBalance（Teleport 完成）
	// Balance += 100, PendingIn -= 100, UtxoCount++
	PebbleStore.UpdateMrc20AccountBalance(
		targetChain, receiverAddress, tickId, tickName,
		decimal.NewFromInt(100),  // Balance += 100
		decimal.Zero,             // PendingOut 不变
		decimal.NewFromInt(-100), // PendingIn -= 100
		1,                        // UtxoCount++
		"arrival_tx", 201, 0,
	)

	// 5.7 删除 TeleportPendingIn
	PebbleStore.DeleteTeleportPendingIn("pin_arrival_txi0", receiverAddress)

	// 5.8 更新 PendingTeleport 状态为完成
	pending.Status = 1
	PebbleStore.SavePendingTeleport(pending)

	// ==================== Step 6: 验证最终状态 ====================
	fmt.Println("\n📝 Step 6: 验证最终状态")

	// 发送方
	balance, _ = PebbleStore.GetMrc20AccountBalance(sourceChain, senderAddress, tickId)
	fmt.Printf("  发送方 (btc): Balance=%s, PendingOut=%s, PendingIn=%s, UtxoCount=%d\n",
		balance.Balance.String(), balance.PendingOut.String(), balance.PendingIn.String(), balance.UtxoCount)

	// 接收方
	receiverBalance, _ = PebbleStore.GetMrc20AccountBalance(targetChain, receiverAddress, tickId)
	fmt.Printf("  接收方 (doge): Balance=%s, PendingOut=%s, PendingIn=%s, UtxoCount=%d\n",
		receiverBalance.Balance.String(), receiverBalance.PendingOut.String(), receiverBalance.PendingIn.String(), receiverBalance.UtxoCount)

	// 列出发送方剩余 UTXO
	fmt.Println("  发送方剩余 UTXO:")
	listUtxosForAddress2(senderAddress, tickId)

	// 列出接收方 UTXO
	fmt.Println("  接收方 UTXO:")
	listUtxosForAddress2(receiverAddress, tickId)

	// ==================== 验证预期 ====================
	fmt.Println("\n🔍 验证结果:")

	// 发送方预期: Balance=638, PendingOut=0
	expectedSenderBalance := decimal.NewFromInt(638) // 38 + 600
	expectedSenderPendingOut := decimal.Zero

	if balance.Balance.Equal(expectedSenderBalance) {
		fmt.Printf("  ✅ 发送方 Balance 正确: %s\n", balance.Balance.String())
	} else {
		fmt.Printf("  ❌ 发送方 Balance 错误: 期望 %s, 实际 %s\n", expectedSenderBalance.String(), balance.Balance.String())
		t.Errorf("Sender Balance mismatch: expected %s, got %s", expectedSenderBalance.String(), balance.Balance.String())
	}

	if balance.PendingOut.Equal(expectedSenderPendingOut) {
		fmt.Printf("  ✅ 发送方 PendingOut 正确: %s\n", balance.PendingOut.String())
	} else {
		fmt.Printf("  ❌ 发送方 PendingOut 错误: 期望 %s, 实际 %s\n", expectedSenderPendingOut.String(), balance.PendingOut.String())
		t.Errorf("Sender PendingOut mismatch: expected %s, got %s", expectedSenderPendingOut.String(), balance.PendingOut.String())
	}

	// 接收方预期: Balance=100, PendingIn=0
	expectedReceiverBalance := decimal.NewFromInt(100)
	expectedReceiverPendingIn := decimal.Zero

	if receiverBalance.Balance.Equal(expectedReceiverBalance) {
		fmt.Printf("  ✅ 接收方 Balance 正确: %s\n", receiverBalance.Balance.String())
	} else {
		fmt.Printf("  ❌ 接收方 Balance 错误: 期望 %s, 实际 %s\n", expectedReceiverBalance.String(), receiverBalance.Balance.String())
		t.Errorf("Receiver Balance mismatch: expected %s, got %s", expectedReceiverBalance.String(), receiverBalance.Balance.String())
	}

	if receiverBalance.PendingIn.Equal(expectedReceiverPendingIn) {
		fmt.Printf("  ✅ 接收方 PendingIn 正确: %s\n", receiverBalance.PendingIn.String())
	} else {
		fmt.Printf("  ❌ 接收方 PendingIn 错误: 期望 %s, 实际 %s\n", expectedReceiverPendingIn.String(), receiverBalance.PendingIn.String())
		t.Errorf("Receiver PendingIn mismatch: expected %s, got %s", expectedReceiverPendingIn.String(), receiverBalance.PendingIn.String())
	}

	fmt.Println("\n========================================")
}

// listUtxosForAddress2 列出指定地址的所有 UTXO
func listUtxosForAddress2(address, tickId string) {
	prefix := fmt.Sprintf("mrc20_in_%s_%s_", address, tickId)
	iter, _ := PebbleStore.Database.MrcDb.NewIter(nil)
	defer iter.Close()

	for iter.SeekGE([]byte(prefix)); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if len(key) < len(prefix) || key[:len(prefix)] != prefix {
			break
		}
		var utxo mrc20.Mrc20Utxo
		if err := json.Unmarshal(iter.Value(), &utxo); err != nil {
			continue
		}
		if utxo.ToAddress == address {
			statusName := getStatusName2(utxo.Status)
			fmt.Printf("    - %s: Amount=%s, Status=%s\n", utxo.TxPoint, utxo.AmtChange.String(), statusName)
		}
	}
}

func getStatusName2(status int) string {
	switch status {
	case 0:
		return "Available"
	case 1:
		return "TeleportPending"
	case 2:
		return "TransferPending"
	case -1:
		return "Spent"
	default:
		return "Unknown"
	}
}
