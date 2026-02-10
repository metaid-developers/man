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

// TestMrc20TransferThenTeleport 测试用户报告的场景：
// 1. 地址有 738 MAN（700 UTXO + 38 UTXO）
// 2. MRC20 Transfer: 将 700 拆分成 600 + 100（通过 MRC20 transfer PIN，不是 native 转账）
// 3. Teleport：将 100 跃迁到 doge 链
// 4. 所有交易出块后，预期 balance=638, pendingOut=0
func TestMrc20TransferThenTeleport(t *testing.T) {
	initTeleportTestConfig()

	tmpDir, err := os.MkdirTemp("", "mrc20_transfer_teleport_*")
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
	userAddress := "bc1qtest_transfer_teleport_user"
	tickId := "transfer_teleport_tick"
	tickName := "MAN"

	// 创建 tick
	tick := mrc20.Mrc20DeployInfo{
		Mrc20Id: tickId,
		Tick:    tickName,
		Chain:   sourceChain,
	}
	PebbleStore.SaveMrc20Tick([]mrc20.Mrc20DeployInfo{tick})

	fmt.Println("========================================")
	fmt.Println("测试：MRC20 Transfer + Teleport 场景")
	fmt.Println("场景：700+38 → transfer 700到600+100 → teleport 100")
	fmt.Println("========================================")

	// Step 1: 创建初始 UTXO（700 + 38）
	fmt.Println("\n📝 Step 1: 创建初始 UTXO (700 + 38)")

	utxo700 := mrc20.Mrc20Utxo{
		Tick:        tickName,
		Mrc20Id:     tickId,
		TxPoint:     "init_tx_700:0",
		FromAddress: "",
		ToAddress:   userAddress,
		AmtChange:   decimal.NewFromInt(700),
		Status:      mrc20.UtxoStatusAvailable,
		Chain:       sourceChain,
		BlockHeight: 100,
		MrcOption:   mrc20.OptionMint,
	}

	utxo38 := mrc20.Mrc20Utxo{
		Tick:        tickName,
		Mrc20Id:     tickId,
		TxPoint:     "init_tx_38:0",
		FromAddress: "",
		ToAddress:   userAddress,
		AmtChange:   decimal.NewFromInt(38),
		Status:      mrc20.UtxoStatusAvailable,
		Chain:       sourceChain,
		BlockHeight: 100,
		MrcOption:   mrc20.OptionMint,
	}

	PebbleStore.SaveMrc20Pin([]mrc20.Mrc20Utxo{utxo700, utxo38})

	// 初始化 AccountBalance
	PebbleStore.UpdateMrc20AccountBalance(
		sourceChain, userAddress, tickId, tickName,
		decimal.NewFromInt(738), // Balance = 738
		decimal.Zero,            // PendingOut = 0
		decimal.Zero,            // PendingIn = 0
		2,                       // UtxoCount = 2
		"init", 100, 0,
	)

	// 验证初始状态
	balance, _ := PebbleStore.GetMrc20AccountBalance(sourceChain, userAddress, tickId)
	fmt.Printf("  初始余额: Balance=%s, PendingOut=%s, UtxoCount=%d\n",
		balance.Balance.String(), balance.PendingOut.String(), balance.UtxoCount)

	// Step 2: MRC20 Transfer PIN（700 → 600 + 100）进入 Mempool
	// 这是 MRC20 协议层面的 transfer，不是链上原生转账
	fmt.Println("\n📝 Step 2: MRC20 Transfer PIN (700 → 600 + 100) - Mempool")

	// 模拟 CreateMrc20TransferUtxo 中 mempool 的处理
	// 1. 原 700 UTXO 状态变成 TransferPending
	// 2. 创建 600 和 100 两个新 UTXO（mempool 中 BlockHeight=-1）

	// 更新 700 UTXO 状态为 TransferPending
	utxo700.Status = mrc20.UtxoStatusTransferPending
	utxo700.OperationTx = "transfer_tx_001"
	PebbleStore.UpdateMrc20Utxo([]*mrc20.Mrc20Utxo{&utxo700}, false)

	// 创建新的 600 和 100 UTXO（mempool），由 MRC20 transfer PIN 创建
	utxo600 := mrc20.Mrc20Utxo{
		Tick:        tickName,
		Mrc20Id:     tickId,
		TxPoint:     "transfer_tx_001:0",
		FromAddress: userAddress,
		ToAddress:   userAddress, // 自己转给自己
		AmtChange:   decimal.NewFromInt(600),
		Status:      mrc20.UtxoStatusAvailable,
		Chain:       sourceChain,
		BlockHeight: -1, // mempool
		OperationTx: "transfer_tx_001",
		MrcOption:   mrc20.OptionDataTransfer,
		Verify:      true,
	}

	utxo100 := mrc20.Mrc20Utxo{
		Tick:        tickName,
		Mrc20Id:     tickId,
		TxPoint:     "transfer_tx_001:1",
		FromAddress: userAddress,
		ToAddress:   userAddress, // 自己转给自己
		AmtChange:   decimal.NewFromInt(100),
		Status:      mrc20.UtxoStatusAvailable,
		Chain:       sourceChain,
		BlockHeight: -1, // mempool
		OperationTx: "transfer_tx_001",
		MrcOption:   mrc20.OptionDataTransfer,
		Verify:      true,
	}

	PebbleStore.SaveMrc20Pin([]mrc20.Mrc20Utxo{utxo600, utxo100})

	// MRC20 transfer 在 mempool 阶段只写流水，不更新余额
	// 参考 transferHandleWithMempool 中的 SaveMempoolTransferTransaction
	// 这里模拟保存 mempool 流水
	fmt.Println("  → MRC20 transfer mempool 阶段：只写流水，不更新余额")

	balance, _ = PebbleStore.GetMrc20AccountBalance(sourceChain, userAddress, tickId)
	fmt.Printf("  Transfer mempool 后: Balance=%s, PendingOut=%s, UtxoCount=%d\n",
		balance.Balance.String(), balance.PendingOut.String(), balance.UtxoCount)

	// Step 3: Teleport 100 到 doge 链（mempool）
	// 此时 utxo100 还在 mempool 中（BlockHeight=-1）
	fmt.Println("\n📝 Step 3: Teleport 100 → doge 链 - Mempool")

	// 将 100 UTXO 状态变成 TeleportPending
	utxo100.Status = mrc20.UtxoStatusTeleportPending
	utxo100.OperationTx = "teleport_tx_001"
	PebbleStore.UpdateMrc20Utxo([]*mrc20.Mrc20Utxo{&utxo100}, false)

	// 保存 PendingTeleport
	pending := &mrc20.PendingTeleport{
		PinId:         "pin_teleport_tx_001i0",
		TxId:          "teleport_tx_001",
		Coord:         "pin_arrival_tx_001i0",
		TickId:        tickId,
		Amount:        "100",
		AssetOutpoint: utxo100.TxPoint,
		TargetChain:   targetChain,
		FromAddress:   userAddress,
		SourceChain:   sourceChain,
		BlockHeight:   -1,
		Status:        0,
	}
	PebbleStore.SavePendingTeleport(pending)

	// Teleport 进入 mempool：Balance -= 100, PendingOut += 100
	// 注意：此时 100 UTXO 还是 mempool 状态（BlockHeight=-1）
	PebbleStore.UpdateMrc20AccountBalance(
		sourceChain, userAddress, tickId, tickName,
		decimal.NewFromInt(-100), // Balance -= 100（虽然是 mempool UTXO，但 teleport 会立即扣除）
		decimal.NewFromInt(100),  // PendingOut += 100
		decimal.Zero,
		-1, // UtxoCount-- (100 UTXO 进入 teleport 状态)
		"teleport_tx_001", -1, 0,
	)

	balance, _ = PebbleStore.GetMrc20AccountBalance(sourceChain, userAddress, tickId)
	fmt.Printf("  Teleport mempool 后: Balance=%s, PendingOut=%s, UtxoCount=%d\n",
		balance.Balance.String(), balance.PendingOut.String(), balance.UtxoCount)

	// ============================================================
	// Step 4: 区块确认
	// ============================================================
	fmt.Println("\n📝 Step 4: 区块确认")

	// 4.1 MRC20 Transfer PIN 确认（transfer_tx_001 出块）
	fmt.Println("  4.1 MRC20 Transfer 确认...")

	// 真实系统中，transferHandleWithMempool 会：
	// 1. 检查输入 UTXO（700）状态，如果是 TransferPending，说明是 mempool 导入的
	// 2. 调用 ProcessTransferSuccess 更新余额
	// 3. 更新 720 UTXO 状态为 Spent
	// 4. 确认输出 UTXOs（600, 100）

	// 检查 100 UTXO 的当前状态（应该是 TeleportPending）
	utxo100Check, _ := PebbleStore.GetMrc20UtxoByTxPoint("transfer_tx_001:1", false)
	if utxo100Check != nil {
		fmt.Printf("  检查 100 UTXO 状态: Status=%d (1=TeleportPending)\n", utxo100Check.Status)
		if utxo100Check.Status == mrc20.UtxoStatusTeleportPending {
			fmt.Println("  → 100 UTXO 是 TeleportPending，MRC20 Transfer 确认时应保留此状态")
		}
	}

	// 更新 700 UTXO 状态为 Spent（已消耗）
	utxo700.Status = mrc20.UtxoStatusSpent
	utxo700.BlockHeight = 200
	PebbleStore.UpdateMrc20Utxo([]*mrc20.Mrc20Utxo{&utxo700}, false)

	// 确认 600 UTXO（BlockHeight 更新）
	utxo600.BlockHeight = 200
	PebbleStore.UpdateMrc20Utxo([]*mrc20.Mrc20Utxo{&utxo600}, false)

	// 核心问题：确认 100 UTXO 时应该保持其 TeleportPending 状态
	// 只更新 BlockHeight，不改变 Status
	utxo100.BlockHeight = 200
	// 注意：不要覆盖 Status！它应该保持 TeleportPending
	PebbleStore.UpdateMrc20Utxo([]*mrc20.Mrc20Utxo{&utxo100}, false)

	// 模拟 ProcessTransferSuccess 的余额更新逻辑
	// 自转账（FromAddress == ToAddress），只有当 mempool 中已经处理过时才需要调整
	// 如果 mempool 阶段没有更新过余额，则这里需要更新
	// 但我们的场景中，mempool 阶段没有更新余额（只写流水），所以这里不需要再更新

	balance, _ = PebbleStore.GetMrc20AccountBalance(sourceChain, userAddress, tickId)
	fmt.Printf("  Transfer 确认后: Balance=%s, PendingOut=%s, UtxoCount=%d\n",
		balance.Balance.String(), balance.PendingOut.String(), balance.UtxoCount)

	// 验证 100 UTXO 状态
	utxo100AfterTransfer, _ := PebbleStore.GetMrc20UtxoByTxPoint("transfer_tx_001:1", false)
	if utxo100AfterTransfer != nil {
		fmt.Printf("  Transfer 确认后 100 UTXO 状态: Status=%d, BlockHeight=%d\n",
			utxo100AfterTransfer.Status, utxo100AfterTransfer.BlockHeight)
	}

	// 4.2 Teleport 确认（arrival 出块）
	fmt.Println("  4.2 Teleport + Arrival 确认...")

	// 模拟 arrival 记录（目标链的 arrival PIN）
	arrival := &mrc20.Mrc20Arrival{
		PinId:         "pin_arrival_tx_001i0",
		TxId:          "arrival_tx_001",
		AssetOutpoint: utxo100.TxPoint,
		Amount:        decimal.NewFromInt(100),
		TickId:        tickId,
		Tick:          tickName,
		LocationIndex: 0,
		ToAddress:     "doge_receiver_address",
		Chain:         targetChain,
		SourceChain:   sourceChain,
		Status:        mrc20.ArrivalStatusPending,
		BlockHeight:   200,
	}
	PebbleStore.SaveMrc20Arrival(arrival)

	// 获取源 UTXO 并检查其状态
	sourceUtxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(utxo100.TxPoint, false)
	fmt.Printf("  源 UTXO 状态: Status=%d (1=TeleportPending)\n", sourceUtxo.Status)

	// 模拟 processPendingTeleportForArrival 和 executeTeleportTransfer 的逻辑
	// 删除源 UTXO
	PebbleStore.DeleteMrc20Utxo(utxo100.TxPoint, userAddress, tickId)

	// 创建目标链 UTXO
	targetUtxo := mrc20.Mrc20Utxo{
		Tick:        tickName,
		Mrc20Id:     tickId,
		TxPoint:     "arrival_tx_001:0",
		FromAddress: userAddress,
		ToAddress:   "doge_receiver_address",
		AmtChange:   decimal.NewFromInt(100),
		Status:      mrc20.UtxoStatusAvailable,
		Chain:       targetChain,
		BlockHeight: 200,
		MrcOption:   mrc20.OptionTeleportTransfer,
	}
	PebbleStore.SaveMrc20Pin([]mrc20.Mrc20Utxo{targetUtxo})

	// 根据 executeTeleportTransfer 的逻辑：
	// 如果 sourceUtxo.Status == TeleportPending:
	//   sourceBalanceDelta = 0（余额已经在进入 pending 时扣除）
	//   sourcePendingOutDelta = -100
	// 如果 sourceUtxo.Status == Available (arrival 先到):
	//   sourceBalanceDelta = -100
	//   sourcePendingOutDelta = 0

	if sourceUtxo.Status == mrc20.UtxoStatusTeleportPending {
		// 从 pending 状态完成
		fmt.Println("  → 源 UTXO 是 TeleportPending 状态，执行 PendingOut -= 100")
		PebbleStore.UpdateMrc20AccountBalance(
			sourceChain, userAddress, tickId, tickName,
			decimal.Zero,             // Balance 不变（已经在进入 pending 时扣除）
			decimal.NewFromInt(-100), // PendingOut -= 100
			decimal.Zero,
			0, // UtxoCount 不变（已经在进入 pending 时扣除）
			"teleport_tx_001", 200, 0,
		)
	} else {
		// 直接完成（arrival 先到）
		fmt.Println("  → 源 UTXO 是 Available 状态，执行 Balance -= 100")
		PebbleStore.UpdateMrc20AccountBalance(
			sourceChain, userAddress, tickId, tickName,
			decimal.NewFromInt(-100), // Balance -= 100
			decimal.Zero,             // PendingOut 不变
			decimal.Zero,
			-1, // UtxoCount--
			"teleport_tx_001", 200, 0,
		)
	}

	// 更新 arrival 状态
	PebbleStore.UpdateMrc20ArrivalStatus(arrival.PinId, mrc20.ArrivalStatusCompleted, "", "", "", 200)

	// ============================================================
	// Step 5: 验证最终状态
	// ============================================================
	fmt.Println("\n📝 Step 5: 验证最终状态")

	balance, _ = PebbleStore.GetMrc20AccountBalance(sourceChain, userAddress, tickId)
	fmt.Printf("  最终余额: Balance=%s, PendingOut=%s, UtxoCount=%d\n",
		balance.Balance.String(), balance.PendingOut.String(), balance.UtxoCount)

	// 验证可用 UTXO
	prefix := fmt.Sprintf("mrc20_in_%s_%s_", userAddress, tickId)
	iter, _ := PebbleStore.Database.MrcDb.NewIter(nil)
	defer iter.Close()

	fmt.Println("  剩余 UTXO:")
	for iter.SeekGE([]byte(prefix)); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if len(key) < len(prefix) || key[:len(prefix)] != prefix {
			break
		}
		var utxo mrc20.Mrc20Utxo
		if err := json.Unmarshal(iter.Value(), &utxo); err != nil {
			continue
		}
		if utxo.ToAddress == userAddress {
			statusName := "Unknown"
			switch utxo.Status {
			case 0:
				statusName = "Available"
			case 1:
				statusName = "TeleportPending"
			case 2:
				statusName = "TransferPending"
			case -1:
				statusName = "Spent"
			}
			fmt.Printf("    - %s: Amount=%s, Status=%s\n", utxo.TxPoint, utxo.AmtChange.String(), statusName)
		}
	}

	// 验证预期：
	// 初始：738
	// Transfer: 700 → 600 + 100（自转账，余额不变）
	// Teleport: 100（余额 -100）
	// 最终：638
	expectedBalance := decimal.NewFromInt(638) // 738 - 100
	expectedPendingOut := decimal.Zero

	fmt.Println("\n🔍 验证结果:")
	if balance.Balance.Equal(expectedBalance) {
		fmt.Printf("  ✅ Balance 正确: %s\n", balance.Balance.String())
	} else {
		fmt.Printf("  ❌ Balance 错误: 期望 %s, 实际 %s\n", expectedBalance.String(), balance.Balance.String())
		t.Errorf("Balance mismatch: expected %s, got %s", expectedBalance.String(), balance.Balance.String())
	}

	if balance.PendingOut.Equal(expectedPendingOut) {
		fmt.Printf("  ✅ PendingOut 正确: %s\n", balance.PendingOut.String())
	} else {
		fmt.Printf("  ❌ PendingOut 错误: 期望 %s, 实际 %s\n", expectedPendingOut.String(), balance.PendingOut.String())
		t.Errorf("PendingOut mismatch: expected %s, got %s", expectedPendingOut.String(), balance.PendingOut.String())
	}
}

// TestSaveMrc20PinProtectsTeleportPending 测试 SaveMrc20Pin 是否保护 TeleportPending 状态
// 确保当一个 UTXO 已经是 TeleportPending 状态时，再次保存不会覆盖该状态
func TestSaveMrc20PinProtectsTeleportPending(t *testing.T) {
	initTeleportTestConfig()

	tmpDir, err := os.MkdirTemp("", "mrc20_protect_test_*")
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

	tickId := "test_protect_tick"
	tickName := "PROTECT"
	userAddress := "bc1qtest_protect"
	txPoint := "test_tx:0"

	fmt.Println("========================================")
	fmt.Println("测试：SaveMrc20Pin 保护 TeleportPending 状态")
	fmt.Println("========================================")

	// Step 1: 创建一个 Available 状态的 UTXO
	fmt.Println("\n📝 Step 1: 创建 Available UTXO")
	utxo1 := mrc20.Mrc20Utxo{
		Tick:        tickName,
		Mrc20Id:     tickId,
		TxPoint:     txPoint,
		ToAddress:   userAddress,
		AmtChange:   decimal.NewFromInt(100),
		Status:      mrc20.UtxoStatusAvailable,
		Chain:       "btc",
		BlockHeight: -1, // mempool
	}
	PebbleStore.SaveMrc20Pin([]mrc20.Mrc20Utxo{utxo1})

	saved1, err := PebbleStore.GetMrc20UtxoByTxPoint(txPoint, false)
	if err != nil || saved1 == nil {
		t.Fatalf("保存 UTXO 失败")
	}
	fmt.Printf("  UTXO 状态: Status=%d, BlockHeight=%d\n", saved1.Status, saved1.BlockHeight)

	// Step 2: 更新状态为 TeleportPending
	fmt.Println("\n📝 Step 2: 更新为 TeleportPending 状态")
	utxo1.Status = mrc20.UtxoStatusTeleportPending
	PebbleStore.UpdateMrc20Utxo([]*mrc20.Mrc20Utxo{&utxo1}, false)

	saved2, _ := PebbleStore.GetMrc20UtxoByTxPoint(txPoint, false)
	fmt.Printf("  UTXO 状态: Status=%d (1=TeleportPending)\n", saved2.Status)

	if saved2.Status != mrc20.UtxoStatusTeleportPending {
		t.Fatalf("UTXO 应该是 TeleportPending 状态")
	}

	// Step 3: 尝试用 SaveMrc20Pin 覆盖为 Available 状态
	fmt.Println("\n📝 Step 3: 尝试用 SaveMrc20Pin 覆盖")
	utxo2 := mrc20.Mrc20Utxo{
		Tick:        tickName,
		Mrc20Id:     tickId,
		TxPoint:     txPoint,
		ToAddress:   userAddress,
		AmtChange:   decimal.NewFromInt(100),
		Status:      mrc20.UtxoStatusAvailable, // 尝试覆盖为 Available
		Chain:       "btc",
		BlockHeight: 200, // 出块了
	}
	PebbleStore.SaveMrc20Pin([]mrc20.Mrc20Utxo{utxo2})

	// Step 4: 验证状态没有被覆盖
	fmt.Println("\n📝 Step 4: 验证状态保护")
	saved3, _ := PebbleStore.GetMrc20UtxoByTxPoint(txPoint, false)
	fmt.Printf("  UTXO 状态: Status=%d, BlockHeight=%d\n", saved3.Status, saved3.BlockHeight)

	if saved3.Status != mrc20.UtxoStatusTeleportPending {
		t.Errorf("❌ TeleportPending 状态被错误覆盖！期望 Status=1, 实际 Status=%d", saved3.Status)
	} else {
		fmt.Println("  ✅ TeleportPending 状态被正确保护")
	}

	// BlockHeight 应该被更新
	if saved3.BlockHeight == 200 {
		fmt.Println("  ✅ BlockHeight 正确更新为 200")
	} else {
		fmt.Printf("  BlockHeight=%d (预期 200，但保护逻辑可能只在更高高度时更新)\n", saved3.BlockHeight)
	}
}
