package man

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"manindexer/adapter"
	"manindexer/common"
	"manindexer/mrc20"
	"manindexer/pebblestore"
	"manindexer/pin"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/shopspring/decimal"
)

// teleportTestInitOnce 确保 teleport 测试的配置只初始化一次
var teleportTestInitOnce sync.Once

func initTeleportTestConfig() {
	teleportTestInitOnce.Do(func() {
		common.InitConfig("../config_dev_regtest.toml")
		common.TestNet = "0"
		common.Chain = "doge"
	})
}

// TestMRC20TeleportFlow_TeleportFirst 测试 Teleport 先出块，然后 Arrival 出块的场景
// 流程:
// 1. 源链: 创建 teleport transfer PIN -> UTXO 状态变为 TeleportPending，发送方 PendingOut += amount
// 2. 目标链: 创建 arrival PIN -> 检测到 pending teleport，执行跃迁
// 3. 结果: 源 UTXO Spent，目标链创建新 UTXO，余额更新正确
func TestMRC20TeleportFlow_TeleportFirst(t *testing.T) {
	// 初始化配置（只会执行一次）
	initTeleportTestConfig()

	// 创建临时数据库
	tmpDir, err := os.MkdirTemp("", "mrc20_teleport_test_*")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Printf("使用临时目录: %s\n", tmpDir)

	// 初始化数据库
	db, err := pebblestore.NewDataBase(tmpDir, 1)
	if err != nil {
		t.Fatalf("初始化数据库失败: %v", err)
	}
	defer db.Close()

	PebbleStore = &PebbleData{
		Database: db,
	}

	// 确保 mrc20 模块启用
	common.Config.Module = []string{"mrc20"}

	// 测试参数
	sourceChain := "doge" // 源链
	targetChain := "btc"  // 目标链
	senderAddress := "DDnnM5GP3o87EDL42PifPMzrtB7xSZhhVg"
	receiverAddress := "D5ERdEN1gsouFSs7zsq7VYJxyWP6dP28H1" // 使用 Doge 地址格式以便 mock
	tickId := "teleport_tick_001"
	tickName := "TELEPORT"

	// 交易 ID - 注意：chainhash 存储是小端序，实际使用时需要反转
	sourceTxId := "0000000000000000000000000000000000000000000000000000000000000010"
	sourceUtxoTxPoint := sourceTxId + ":0"
	teleportTxId := "0000000000000000000000000000000000000000000000000000000000000020"
	arrivalTxId := "0000000000000000000000000000000000000000000000000000000000000030"
	arrivalPinId := "pin_" + arrivalTxId + "i0" // coord

	// Mock 交易缓存
	mockTxCache := make(map[string]*btcutil.Tx)
	mockTxCache[teleportTxId] = createMockTeleportTxWithInput(t, sourceTxId, 0)
	mockTxCache[arrivalTxId] = createMockArrivalTxWithReceiver(t)

	MockGetTransactionWithCache = func(chain string, txid string) (interface{}, error) {
		if tx, ok := mockTxCache[txid]; ok {
			return tx, nil
		}
		return nil, fmt.Errorf("mock: transaction not found: %s", txid)
	}
	defer func() { MockGetTransactionWithCache = nil }()

	// 设置 mock IndexerAdapter
	if IndexerAdapter == nil {
		IndexerAdapter = make(map[string]adapter.Indexer)
	}
	IndexerAdapter[sourceChain] = &MockTeleportIndexerAdapter{
		receiverAddress: receiverAddress,
		mockTxCache:     mockTxCache,
	}
	IndexerAdapter[targetChain] = &MockTeleportIndexerAdapter{
		receiverAddress: receiverAddress,
		mockTxCache:     mockTxCache,
	}

	// 设置 mock ChainAdapter（用于 getAddressFromOutputWithHeight）
	if ChainAdapter == nil {
		ChainAdapter = make(map[string]adapter.Chain)
	}
	ChainAdapter[sourceChain] = &MockChainAdapter{mockTxCache: mockTxCache}
	ChainAdapter[targetChain] = &MockChainAdapter{mockTxCache: mockTxCache}

	fmt.Println("\n========================================")
	fmt.Println("MRC20 Teleport 流程测试 (Teleport 先出块)")
	fmt.Println("========================================")

	// ========================================
	// Step 1: 前置数据 - Tick 和初始余额/UTXO
	// ========================================
	fmt.Println("\n📝 Step 1: 创建前置数据")

	// 1.1 创建 Tick 信息 (两条链都能查到)
	tickInfo := &mrc20.Mrc20DeployInfo{
		Tick:    tickName,
		Mrc20Id: tickId,
		Chain:   sourceChain,
	}
	PebbleStore.SaveMrc20Tick([]mrc20.Mrc20DeployInfo{*tickInfo})
	fmt.Printf("  ✓ Tick 已创建: %s (%s)\n", tickName, tickId)

	// 1.2 创建初始余额
	initialBalance := &mrc20.Mrc20AccountBalance{
		Address:          senderAddress,
		TickId:           tickId,
		Tick:             tickName,
		Balance:          decimal.NewFromInt(100),
		Chain:            sourceChain,
		LastUpdateHeight: 100,
		UtxoCount:        1,
	}
	PebbleStore.SaveMrc20AccountBalance(initialBalance)
	fmt.Printf("  ✓ 初始余额: %s = 100 (%s 链)\n", senderAddress, sourceChain)

	// 1.3 创建源 UTXO
	sourceUtxo := mrc20.Mrc20Utxo{
		Tick:        tickName,
		Mrc20Id:     tickId,
		TxPoint:     sourceUtxoTxPoint,
		PointValue:  546,
		BlockHeight: 100,
		MrcOption:   mrc20.OptionMint,
		ToAddress:   senderAddress,
		AmtChange:   decimal.NewFromInt(100),
		Status:      mrc20.UtxoStatusAvailable,
		Chain:       sourceChain,
		Verify:      true,
	}
	PebbleStore.SaveMrc20Pin([]mrc20.Mrc20Utxo{sourceUtxo})
	fmt.Printf("  ✓ 源 UTXO: %s (amt=100, chain=%s)\n", sourceUtxoTxPoint, sourceChain)

	// ========================================
	// Step 2: 源链 - Teleport Transfer 出块
	// ========================================
	fmt.Println("\n📝 Step 2: 源链 Teleport Transfer 出块")

	// 构造 teleport transfer PIN 数据
	teleportContent := mrc20.Mrc20TeleportTransferData{
		Id:     tickId,
		Amount: "100",        // teleport 必须转移整个 UTXO
		Coord:  arrivalPinId, // 目标链 arrival 的 PIN ID
		Chain:  targetChain,
		Type:   "teleport",
	}
	contentBody, _ := json.Marshal(teleportContent)

	teleportPin := &pin.PinInscription{
		Id:                 "pin_" + teleportTxId + "i0",
		GenesisTransaction: teleportTxId,
		Path:               "/ft/mrc20/transfer",
		ContentBody:        contentBody,
		ChainName:          sourceChain,
		Address:            senderAddress,
		GenesisHeight:      200, // 出块
		CreateAddress:      senderAddress,
		Output:             sourceUtxoTxPoint,
	}

	fmt.Printf("  Teleport PIN Tx: %s\n", teleportTxId)
	fmt.Printf("  转账: %s (%s) -> %s (%s), 金额: 50\n", senderAddress, sourceChain, receiverAddress, targetChain)
	fmt.Printf("  Coord (arrival PIN): %s\n", arrivalPinId)

	// 调用 handleMrc20 处理 teleport
	pinList := &[]*pin.PinInscription{teleportPin}
	txInList := &[]string{sourceUtxoTxPoint}

	PebbleStore.handleMrc20(sourceChain, 200, pinList, txInList)

	// 验证 teleport 后状态
	fmt.Println("\n  🔍 验证 teleport 后状态:")
	utxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(sourceUtxoTxPoint, false)
	if utxo != nil {
		fmt.Printf("    源 UTXO 状态: Status=%d (期望=%d TeleportPending)\n", utxo.Status, mrc20.UtxoStatusTeleportPending)
	}

	bal, _ := PebbleStore.GetMrc20AccountBalance(sourceChain, senderAddress, tickId)
	if bal != nil {
		fmt.Printf("    发送方余额: Balance=%s, PendingOut=%s, PendingIn=%s\n",
			bal.Balance.String(), bal.PendingOut.String(), bal.PendingIn.String())
	}

	// 检查是否有 pending teleport 记录
	pendingTeleport, err := PebbleStore.GetPendingTeleportByCoord(arrivalPinId)
	if err == nil && pendingTeleport != nil {
		fmt.Printf("    ✓ PendingTeleport 已创建: coord=%s, amount=%s\n", pendingTeleport.Coord, pendingTeleport.Amount)
	} else {
		fmt.Printf("    ✗ PendingTeleport 未找到: %v\n", err)
	}

	// ========================================
	// Step 3: 目标链 - Arrival 出块
	// ========================================
	fmt.Println("\n📝 Step 3: 目标链 Arrival 出块")

	// 构造 arrival PIN 数据
	arrivalContent := mrc20.Mrc20ArrivalData{
		AssetOutpoint: sourceUtxoTxPoint,           // 源链 UTXO
		Amount:        mrc20.FlexibleString("100"), // 必须是 UTXO 全部金额
		TickId:        tickId,
		LocationIndex: 0, // 目标交易的 output[0]
	}
	arrivalContentBody, _ := json.Marshal(arrivalContent)

	arrivalPin := &pin.PinInscription{
		Id:                 arrivalPinId,
		GenesisTransaction: arrivalTxId,
		Path:               "/ft/mrc20/arrival",
		ContentBody:        arrivalContentBody,
		ChainName:          targetChain,
		Address:            receiverAddress,
		GenesisHeight:      201,
		CreateAddress:      receiverAddress,
	}

	fmt.Printf("  Arrival PIN ID: %s\n", arrivalPinId)
	fmt.Printf("  Arrival Tx: %s\n", arrivalTxId)
	fmt.Printf("  Asset Outpoint: %s\n", sourceUtxoTxPoint)

	// 调用 handleMrc20 处理 arrival (path 会匹配到 arrivalHandle)
	arrivalPinList := &[]*pin.PinInscription{arrivalPin}
	arrivalTxInList := &[]string{} // arrival 不花费 UTXO

	PebbleStore.handleMrc20(targetChain, 201, arrivalPinList, arrivalTxInList)

	// ========================================
	// Step 4: 验证最终状态
	// ========================================
	fmt.Println("\n📝 Step 4: 验证最终状态")

	// 验证源链发送方余额
	senderBal, _ := PebbleStore.GetMrc20AccountBalance(sourceChain, senderAddress, tickId)
	if senderBal != nil {
		fmt.Printf("  源链发送方余额: Balance=%s, PendingOut=%s, PendingIn=%s\n",
			senderBal.Balance.String(), senderBal.PendingOut.String(), senderBal.PendingIn.String())
	}

	// 验证源 UTXO 状态
	finalUtxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(sourceUtxoTxPoint, false)
	if finalUtxo != nil {
		fmt.Printf("  源 UTXO 最终状态: Status=%d\n", finalUtxo.Status)
	}

	// 验证目标链新 UTXO
	targetUtxoPoint := fmt.Sprintf("%s:%d", arrivalTxId, 0)
	targetUtxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(targetUtxoPoint, false)
	if targetUtxo != nil {
		fmt.Printf("  目标链新 UTXO: %s, amt=%s, status=%d, toAddress=%s\n",
			targetUtxo.TxPoint, targetUtxo.AmtChange.String(), targetUtxo.Status, targetUtxo.ToAddress)
	} else {
		fmt.Println("  目标链新 UTXO: 未找到")
	}

	// 验证目标链接收方余额（使用实际 UTXO 的 ToAddress）
	var receiverBal *mrc20.Mrc20AccountBalance
	var actualReceiverAddress string
	if targetUtxo != nil {
		actualReceiverAddress = targetUtxo.ToAddress
		receiverBal, _ = PebbleStore.GetMrc20AccountBalance(targetChain, actualReceiverAddress, tickId)
	}
	if receiverBal != nil {
		fmt.Printf("  目标链接收方余额 (%s): Balance=%s, PendingOut=%s, PendingIn=%s\n",
			actualReceiverAddress, receiverBal.Balance.String(), receiverBal.PendingOut.String(), receiverBal.PendingIn.String())
	} else {
		fmt.Println("  目标链接收方余额: 未找到")
	}

	// 验证 PendingTeleport 状态
	finalPending, _ := PebbleStore.GetPendingTeleportByCoord(arrivalPinId)
	if finalPending != nil {
		fmt.Printf("  PendingTeleport 状态: Status=%d (期望=1 completed)\n", finalPending.Status)
	}

	// 验证 Arrival 状态
	arrival, _ := PebbleStore.GetMrc20ArrivalByPinId(arrivalPinId)
	if arrival != nil {
		fmt.Printf("  Arrival 状态: Status=%d (期望=%d completed)\n", arrival.Status, mrc20.ArrivalStatusCompleted)
	}

	// ========================================
	// 断言验证
	// ========================================
	fmt.Println("\n🔍 验证结果:")

	// 源链发送方余额应该减少 100 (全额跃迁)
	if senderBal != nil {
		expectedBalance := decimal.NewFromInt(0) // 100 - 100 = 0
		if !senderBal.Balance.Equal(expectedBalance) {
			t.Errorf("❌ 源链发送方余额错误: 期望=%s, 实际=%s", expectedBalance.String(), senderBal.Balance.String())
		} else {
			fmt.Printf("  ✅ 源链发送方余额正确: %s\n", senderBal.Balance.String())
		}
		// PendingOut 应该为 0（跃迁完成后清零）
		if !senderBal.PendingOut.IsZero() {
			t.Errorf("❌ 源链发送方 PendingOut 错误: 期望=0, 实际=%s", senderBal.PendingOut.String())
		} else {
			fmt.Printf("  ✅ 源链发送方 PendingOut 正确: 0\n")
		}
	} else {
		t.Errorf("❌ 源链发送方余额未找到")
	}

	// 目标链接收方余额应该增加 100 (全额跃迁)
	if receiverBal != nil {
		expectedBalance := decimal.NewFromInt(100)
		if !receiverBal.Balance.Equal(expectedBalance) {
			t.Errorf("❌ 目标链接收方余额错误: 期望=%s, 实际=%s", expectedBalance.String(), receiverBal.Balance.String())
		} else {
			fmt.Printf("  ✅ 目标链接收方余额正确: %s\n", receiverBal.Balance.String())
		}
	} else {
		t.Errorf("❌ 目标链接收方余额未找到")
	}

	// 源 UTXO 应该是 Spent
	if finalUtxo != nil && finalUtxo.Status != mrc20.UtxoStatusSpent {
		t.Errorf("❌ 源 UTXO 状态错误: 期望=%d (Spent), 实际=%d", mrc20.UtxoStatusSpent, finalUtxo.Status)
	} else if finalUtxo != nil {
		fmt.Printf("  ✅ 源 UTXO 状态正确: Spent\n")
	}

	// 目标链新 UTXO 应该存在且可用
	if targetUtxo != nil && targetUtxo.Status == mrc20.UtxoStatusAvailable {
		fmt.Printf("  ✅ 目标链新 UTXO 正确: Available\n")
	} else if targetUtxo != nil {
		t.Errorf("❌ 目标链新 UTXO 状态错误: 期望=%d (Available), 实际=%d", mrc20.UtxoStatusAvailable, targetUtxo.Status)
	} else {
		t.Errorf("❌ 目标链新 UTXO 未找到")
	}

	fmt.Println("\n========================================")
}

// TestMRC20TeleportFlow_ArrivalFirst 测试 Arrival 先出块，然后 Teleport Transfer 出块的场景
// 流程:
// 1. 目标链: 创建 arrival PIN (源 UTXO 还是 Available) -> arrival 保存为 pending 状态
// 2. 源链: 创建 teleport transfer PIN -> 检测到 arrival 已存在，直接执行跃迁
// 3. 结果: 源 UTXO Spent，目标链创建新 UTXO，余额更新正确
func TestMRC20TeleportFlow_ArrivalFirst(t *testing.T) {
	// 初始化配置（只会执行一次）
	initTeleportTestConfig()

	// 创建临时数据库
	tmpDir, err := os.MkdirTemp("", "mrc20_teleport_arrival_first_*")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Printf("使用临时目录: %s\n", tmpDir)

	// 初始化数据库
	db, err := pebblestore.NewDataBase(tmpDir, 1)
	if err != nil {
		t.Fatalf("初始化数据库失败: %v", err)
	}
	defer db.Close()

	PebbleStore = &PebbleData{
		Database: db,
	}

	// 确保 mrc20 模块启用
	common.Config.Module = []string{"mrc20"}

	// 测试参数
	sourceChain := "doge"
	targetChain := "btc"
	senderAddress := "DDnnM5GP3o87EDL42PifPMzrtB7xSZhhVg"
	receiverAddress := "D5ERdEN1gsouFSs7zsq7VYJxyWP6dP28H1" // 使用 Doge 地址格式
	tickId := "teleport_tick_002"
	tickName := "TELEPORT2"

	// 交易 ID
	sourceTxId := "0000000000000000000000000000000000000000000000000000000000000050"
	sourceUtxoTxPoint := sourceTxId + ":0"
	teleportTxId := "0000000000000000000000000000000000000000000000000000000000000060"
	arrivalTxId := "0000000000000000000000000000000000000000000000000000000000000070"
	arrivalPinId := "pin_" + arrivalTxId + "i0"

	// Mock 交易缓存
	mockTxCache := make(map[string]*btcutil.Tx)
	mockTxCache[teleportTxId] = createMockTeleportTxWithInput(t, sourceTxId, 0)
	mockTxCache[arrivalTxId] = createMockArrivalTxWithReceiver(t)

	MockGetTransactionWithCache = func(chain string, txid string) (interface{}, error) {
		if tx, ok := mockTxCache[txid]; ok {
			return tx, nil
		}
		return nil, fmt.Errorf("mock: transaction not found: %s", txid)
	}
	defer func() { MockGetTransactionWithCache = nil }()

	// 设置 mock IndexerAdapter
	if IndexerAdapter == nil {
		IndexerAdapter = make(map[string]adapter.Indexer)
	}
	IndexerAdapter[sourceChain] = &MockTeleportIndexerAdapter{
		receiverAddress: receiverAddress,
		mockTxCache:     mockTxCache,
	}
	IndexerAdapter[targetChain] = &MockTeleportIndexerAdapter{
		receiverAddress: receiverAddress,
		mockTxCache:     mockTxCache,
	}

	// 设置 mock ChainAdapter
	if ChainAdapter == nil {
		ChainAdapter = make(map[string]adapter.Chain)
	}
	ChainAdapter[sourceChain] = &MockChainAdapter{mockTxCache: mockTxCache}
	ChainAdapter[targetChain] = &MockChainAdapter{mockTxCache: mockTxCache}

	fmt.Println("\n========================================")
	fmt.Println("MRC20 Teleport 流程测试 (Arrival 先出块)")
	fmt.Println("========================================")

	// ========================================
	// Step 1: 前置数据
	// ========================================
	fmt.Println("\n📝 Step 1: 创建前置数据")

	tickInfo := &mrc20.Mrc20DeployInfo{
		Tick:    tickName,
		Mrc20Id: tickId,
		Chain:   sourceChain,
	}
	PebbleStore.SaveMrc20Tick([]mrc20.Mrc20DeployInfo{*tickInfo})
	fmt.Printf("  ✓ Tick 已创建: %s (%s)\n", tickName, tickId)

	initialBalance := &mrc20.Mrc20AccountBalance{
		Address:          senderAddress,
		TickId:           tickId,
		Tick:             tickName,
		Balance:          decimal.NewFromInt(100),
		Chain:            sourceChain,
		LastUpdateHeight: 100,
		UtxoCount:        1,
	}
	PebbleStore.SaveMrc20AccountBalance(initialBalance)
	fmt.Printf("  ✓ 初始余额: %s = 100 (%s 链)\n", senderAddress, sourceChain)

	sourceUtxo := mrc20.Mrc20Utxo{
		Tick:        tickName,
		Mrc20Id:     tickId,
		TxPoint:     sourceUtxoTxPoint,
		PointValue:  546,
		BlockHeight: 100,
		MrcOption:   mrc20.OptionMint,
		ToAddress:   senderAddress,
		AmtChange:   decimal.NewFromInt(100),
		Status:      mrc20.UtxoStatusAvailable,
		Chain:       sourceChain,
		Verify:      true,
	}
	PebbleStore.SaveMrc20Pin([]mrc20.Mrc20Utxo{sourceUtxo})
	fmt.Printf("  ✓ 源 UTXO: %s (amt=100)\n", sourceUtxoTxPoint)

	// ========================================
	// Step 2: 目标链 - Arrival 先出块
	// ========================================
	fmt.Println("\n📝 Step 2: 目标链 Arrival 先出块")

	arrivalContent := mrc20.Mrc20ArrivalData{
		AssetOutpoint: sourceUtxoTxPoint,
		Amount:        mrc20.FlexibleString("100"), // 必须是 UTXO 全部金额
		TickId:        tickId,
		LocationIndex: 0,
	}
	arrivalContentBody, _ := json.Marshal(arrivalContent)

	arrivalPin := &pin.PinInscription{
		Id:                 arrivalPinId,
		GenesisTransaction: arrivalTxId,
		Path:               "/ft/mrc20/arrival",
		ContentBody:        arrivalContentBody,
		ChainName:          targetChain,
		Address:            receiverAddress,
		GenesisHeight:      200,
		CreateAddress:      receiverAddress,
	}

	fmt.Printf("  Arrival PIN ID: %s\n", arrivalPinId)
	fmt.Printf("  Asset Outpoint: %s\n", sourceUtxoTxPoint)

	// 调用 handleMrc20 处理 arrival
	arrivalPinList := &[]*pin.PinInscription{arrivalPin}
	arrivalTxInList := &[]string{}

	PebbleStore.handleMrc20(targetChain, 200, arrivalPinList, arrivalTxInList)

	// 验证 arrival 后状态
	fmt.Println("\n  🔍 验证 arrival 后状态:")
	arrival, _ := PebbleStore.GetMrc20ArrivalByPinId(arrivalPinId)
	if arrival != nil {
		fmt.Printf("    Arrival 状态: Status=%d (期望=%d pending)\n", arrival.Status, mrc20.ArrivalStatusPending)
	} else {
		fmt.Println("    ✗ Arrival 未找到")
	}

	// 源 UTXO 应该还是 Available
	utxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(sourceUtxoTxPoint, false)
	if utxo != nil {
		fmt.Printf("    源 UTXO 状态: Status=%d (期望=%d Available)\n", utxo.Status, mrc20.UtxoStatusAvailable)
	}

	// ========================================
	// Step 3: 源链 - Teleport Transfer 出块
	// ========================================
	fmt.Println("\n📝 Step 3: 源链 Teleport Transfer 出块")

	teleportContent := mrc20.Mrc20TeleportTransferData{
		Id:     tickId,
		Amount: "100", // 全额跃迁
		Coord:  arrivalPinId,
		Chain:  targetChain,
		Type:   "teleport",
	}
	contentBody, _ := json.Marshal(teleportContent)

	teleportPin := &pin.PinInscription{
		Id:                 "pin_" + teleportTxId + "i0",
		GenesisTransaction: teleportTxId,
		Path:               "/ft/mrc20/transfer",
		ContentBody:        contentBody,
		ChainName:          sourceChain,
		Address:            senderAddress,
		GenesisHeight:      201,
		CreateAddress:      senderAddress,
		Output:             sourceUtxoTxPoint,
	}

	fmt.Printf("  Teleport PIN Tx: %s\n", teleportTxId)
	fmt.Printf("  Coord (arrival PIN): %s\n", arrivalPinId)

	// 调用 handleMrc20 处理 teleport
	pinList := &[]*pin.PinInscription{teleportPin}
	txInList := &[]string{sourceUtxoTxPoint}

	PebbleStore.handleMrc20(sourceChain, 201, pinList, txInList)

	// ========================================
	// Step 4: 验证最终状态
	// ========================================
	fmt.Println("\n📝 Step 4: 验证最终状态")

	// 源链发送方余额
	senderBal, _ := PebbleStore.GetMrc20AccountBalance(sourceChain, senderAddress, tickId)
	if senderBal != nil {
		fmt.Printf("  源链发送方余额: Balance=%s, PendingOut=%s, PendingIn=%s\n",
			senderBal.Balance.String(), senderBal.PendingOut.String(), senderBal.PendingIn.String())
	}

	// 源 UTXO 状态
	finalUtxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(sourceUtxoTxPoint, false)
	if finalUtxo != nil {
		fmt.Printf("  源 UTXO 最终状态: Status=%d\n", finalUtxo.Status)
	}

	// 目标链新 UTXO
	targetUtxoPoint := fmt.Sprintf("%s:%d", arrivalTxId, 0)
	targetUtxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(targetUtxoPoint, false)
	if targetUtxo != nil {
		fmt.Printf("  目标链新 UTXO: %s, amt=%s, status=%d, toAddress=%s\n",
			targetUtxo.TxPoint, targetUtxo.AmtChange.String(), targetUtxo.Status, targetUtxo.ToAddress)
	} else {
		fmt.Println("  目标链新 UTXO: 未找到")
	}

	// 目标链接收方余额（使用实际 UTXO 的 ToAddress）
	var receiverBal *mrc20.Mrc20AccountBalance
	var actualReceiverAddress string
	if targetUtxo != nil {
		actualReceiverAddress = targetUtxo.ToAddress
		receiverBal, _ = PebbleStore.GetMrc20AccountBalance(targetChain, actualReceiverAddress, tickId)
	}
	if receiverBal != nil {
		fmt.Printf("  目标链接收方余额 (%s): Balance=%s, PendingOut=%s, PendingIn=%s\n",
			actualReceiverAddress, receiverBal.Balance.String(), receiverBal.PendingOut.String(), receiverBal.PendingIn.String())
	} else {
		fmt.Println("  目标链接收方余额: 未找到")
	}

	// Arrival 状态
	finalArrival, _ := PebbleStore.GetMrc20ArrivalByPinId(arrivalPinId)
	if finalArrival != nil {
		fmt.Printf("  Arrival 状态: Status=%d\n", finalArrival.Status)
	}

	// ========================================
	// 断言验证
	// ========================================
	fmt.Println("\n🔍 验证结果:")

	// 源链发送方余额应该减少 100 (全额跃迁)
	if senderBal != nil {
		expectedBalance := decimal.NewFromInt(0) // 100 - 100 = 0
		if !senderBal.Balance.Equal(expectedBalance) {
			t.Errorf("❌ 源链发送方余额错误: 期望=%s, 实际=%s", expectedBalance.String(), senderBal.Balance.String())
		} else {
			fmt.Printf("  ✅ 源链发送方余额正确: %s\n", senderBal.Balance.String())
		}
	} else {
		t.Errorf("❌ 源链发送方余额未找到")
	}

	// 目标链接收方余额应该增加 100 (全额跃迁)
	if receiverBal != nil {
		expectedBalance := decimal.NewFromInt(100)
		if !receiverBal.Balance.Equal(expectedBalance) {
			t.Errorf("❌ 目标链接收方余额错误: 期望=%s, 实际=%s", expectedBalance.String(), receiverBal.Balance.String())
		} else {
			fmt.Printf("  ✅ 目标链接收方余额正确: %s\n", receiverBal.Balance.String())
		}
	} else {
		t.Errorf("❌ 目标链接收方余额未找到")
	}

	// 源 UTXO 应该是 Spent
	if finalUtxo != nil && finalUtxo.Status != mrc20.UtxoStatusSpent {
		t.Errorf("❌ 源 UTXO 状态错误: 期望=%d (Spent), 实际=%d", mrc20.UtxoStatusSpent, finalUtxo.Status)
	} else if finalUtxo != nil {
		fmt.Printf("  ✅ 源 UTXO 状态正确: Spent\n")
	}

	// 目标链新 UTXO 应该存在且可用
	if targetUtxo != nil && targetUtxo.Status == mrc20.UtxoStatusAvailable {
		fmt.Printf("  ✅ 目标链新 UTXO 正确: Available\n")
	} else if targetUtxo != nil {
		t.Errorf("❌ 目标链新 UTXO 状态错误: 期望=%d (Available), 实际=%d", mrc20.UtxoStatusAvailable, targetUtxo.Status)
	} else {
		t.Errorf("❌ 目标链新 UTXO 未找到")
	}

	fmt.Println("\n========================================")
}

// ========================================
// 辅助函数和 Mock
// ========================================

// createMockTeleportTxWithInput 创建模拟的 Teleport Transfer 交易，正确设置输入
func createMockTeleportTxWithInput(t *testing.T, sourceTxId string, sourceVout uint32) *btcutil.Tx {
	msgTx := wire.NewMsgTx(wire.TxVersion)

	// 解析源 UTXO txid - 需要反转字节序（chainhash 是小端序）
	prevHashBytes, err := hex.DecodeString(sourceTxId)
	if err != nil {
		t.Fatalf("解析 txid 失败: %v", err)
	}
	// 反转字节序
	for i, j := 0, len(prevHashBytes)-1; i < j; i, j = i+1, j-1 {
		prevHashBytes[i], prevHashBytes[j] = prevHashBytes[j], prevHashBytes[i]
	}
	prevHash, _ := chainhash.NewHash(prevHashBytes)
	outPoint := wire.NewOutPoint(prevHash, sourceVout)
	txIn := wire.NewTxIn(outPoint, nil, nil)
	msgTx.AddTxIn(txIn)

	// Teleport 交易的输出（PIN 输出）
	pkScript := []byte{
		0x76, 0xa9, 0x14,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
		0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14,
		0x88, 0xac,
	}
	txOut := wire.NewTxOut(546, pkScript)
	msgTx.AddTxOut(txOut)

	return btcutil.NewTx(msgTx)
}

// createMockArrivalTxWithReceiver 创建模拟的 Arrival 交易
func createMockArrivalTxWithReceiver(t *testing.T) *btcutil.Tx {
	msgTx := wire.NewMsgTx(wire.TxVersion)

	// Arrival 交易的输入（接收方的 funding tx）
	hashBytes, _ := hex.DecodeString("ff00000000000000000000000000000000000000000000000000000000000000")
	prevHash, _ := chainhash.NewHash(hashBytes)
	outPoint := wire.NewOutPoint(prevHash, 0)
	txIn := wire.NewTxIn(outPoint, nil, nil)
	msgTx.AddTxIn(txIn)

	// Output 0: 接收资产的输出（使用与测试中 receiverAddress 对应的 pkScript）
	// D5ERdEN1gsouFSs7zsq7VYJxyWP6dP28H1 的 pkScript:
	// 这是 P2PKH 脚本，pubkey hash = 0x01..0x14
	pkScript := []byte{
		0x76, 0xa9, 0x14,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
		0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14,
		0x88, 0xac,
	}
	txOut := wire.NewTxOut(546, pkScript)
	msgTx.AddTxOut(txOut)

	return btcutil.NewTx(msgTx)
}

// MockTeleportIndexerAdapter 是 teleport 测试用的索引器适配器
type MockTeleportIndexerAdapter struct {
	receiverAddress string
	mockTxCache     map[string]*btcutil.Tx
}

func (m *MockTeleportIndexerAdapter) InitIndexer() {}

func (m *MockTeleportIndexerAdapter) CatchPins(blockHeight int64) (*[]*pin.PinInscription, *[]string, *map[string]string) {
	return nil, nil, nil
}

func (m *MockTeleportIndexerAdapter) CatchPinsByTx(msgTx interface{}, blockHeight int64, timestamp int64, blockHash string, merkleRoot string, txIndex int) []*pin.PinInscription {
	return nil
}

func (m *MockTeleportIndexerAdapter) CatchMempoolPins(txList []interface{}) ([]*pin.PinInscription, []string) {
	return nil, nil
}

func (m *MockTeleportIndexerAdapter) CatchTransfer(idMap map[string]string) map[string]*pin.PinTransferInfo {
	return nil
}

func (m *MockTeleportIndexerAdapter) GetAddress(pkScript []byte) string {
	return m.receiverAddress
}

func (m *MockTeleportIndexerAdapter) ZmqRun(chanMsg chan pin.MempollChanMsg) {}

func (m *MockTeleportIndexerAdapter) GetBlockTxHash(blockHeight int64) ([]string, []string) {
	return nil, nil
}

func (m *MockTeleportIndexerAdapter) ZmqHashblock() {}

func (m *MockTeleportIndexerAdapter) CatchNativeMrc20Transfer(blockHeight int64, utxoList []*mrc20.Mrc20Utxo, mrc20TransferPinTx map[string]struct{}) []*mrc20.Mrc20Utxo {
	return nil
}

func (m *MockTeleportIndexerAdapter) CatchMempoolNativeMrc20Transfer(txList []interface{}, utxoList []*mrc20.Mrc20Utxo, mrc20TransferPinTx map[string]struct{}) []*mrc20.Mrc20Utxo {
	return nil
}

// MockChainAdapter 是测试用的链适配器
type MockChainAdapter struct {
	mockTxCache map[string]*btcutil.Tx
}

func (m *MockChainAdapter) InitChain() {}

func (m *MockChainAdapter) GetBlock(blockHeight int64) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MockChainAdapter) GetBlockTime(blockHeight int64) (int64, error) {
	return 0, nil
}

func (m *MockChainAdapter) GetTransaction(txId string) (interface{}, error) {
	if tx, ok := m.mockTxCache[txId]; ok {
		return tx, nil
	}
	return nil, fmt.Errorf("transaction not found: %s", txId)
}

func (m *MockChainAdapter) GetInitialHeight() int64 {
	return 0
}

func (m *MockChainAdapter) GetBestHeight() int64 {
	return 1000
}

func (m *MockChainAdapter) GetBlockMsg(height int64) *pin.BlockMsg {
	return nil
}

func (m *MockChainAdapter) GetMempoolTransactionList() ([]interface{}, error) {
	return nil, nil
}

func (m *MockChainAdapter) GetTxSizeAndFees(txHash string) (int64, int64, string, error) {
	return 0, 0, "", nil
}

// reverseBytes 反转字节数组（用于处理 txid 的字节序）
func reverseBytes(b []byte) []byte {
	result := make([]byte, len(b))
	for i := range b {
		result[i] = b[len(b)-1-i]
	}
	return result
}

// parseTxPoint 解析 txpoint 格式 "txid:vout"
func parseTxPoint(txPoint string) (string, uint32) {
	parts := strings.Split(txPoint, ":")
	if len(parts) != 2 {
		return "", 0
	}
	var vout uint32
	fmt.Sscanf(parts[1], "%d", &vout)
	return parts[0], vout
}

// TestMRC20TeleportFlow_MempoolBalance 测试 Teleport 在 mempool 阶段双方的余额变化
// 这个测试验证：
// 1. Teleport Transfer 进入 mempool 时 - 发送方和接收方的 pending 余额状态
// 2. Arrival 进入 mempool 时 - 接收方的 pending 余额状态
// 3. 最终确认后的状态
func TestMRC20TeleportFlow_MempoolBalance(t *testing.T) {
	initTeleportTestConfig()

	tmpDir, err := os.MkdirTemp("", "mrc20_teleport_mempool_*")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	db, err := pebblestore.NewDataBase(tmpDir, 1)
	if err != nil {
		t.Fatalf("初始化数据库失败: %v", err)
	}
	defer db.Close()

	PebbleStore = &PebbleData{
		Database: db,
	}

	common.Config.Module = []string{"mrc20"}

	// 测试参数
	sourceChain := "doge"
	targetChain := "btc"
	senderAddress := "DDnnM5GP3o87EDL42PifPMzrtB7xSZhhVg"
	receiverAddress := "D5ERdEN1gsouFSs7zsq7VYJxyWP6dP28H1"
	tickId := "teleport_mempool_tick"
	tickName := "TMEMPOOL"

	sourceTxId := "0000000000000000000000000000000000000000000000000000000000000100"
	sourceUtxoTxPoint := sourceTxId + ":0"
	teleportTxId := "0000000000000000000000000000000000000000000000000000000000000200"
	arrivalTxId := "0000000000000000000000000000000000000000000000000000000000000300"
	arrivalPinId := "pin_" + arrivalTxId + "i0"

	// Mock 交易缓存
	mockTxCache := make(map[string]*btcutil.Tx)
	mockTxCache[teleportTxId] = createMockTeleportTxWithInput(t, sourceTxId, 0)
	mockTxCache[arrivalTxId] = createMockArrivalTxWithReceiver(t)

	MockGetTransactionWithCache = func(chain string, txid string) (interface{}, error) {
		if tx, ok := mockTxCache[txid]; ok {
			return tx, nil
		}
		return nil, fmt.Errorf("mock: transaction not found: %s", txid)
	}
	defer func() { MockGetTransactionWithCache = nil }()

	// 设置 mock adapters
	if IndexerAdapter == nil {
		IndexerAdapter = make(map[string]adapter.Indexer)
	}
	IndexerAdapter[sourceChain] = &MockTeleportIndexerAdapter{
		receiverAddress: receiverAddress,
		mockTxCache:     mockTxCache,
	}
	IndexerAdapter[targetChain] = &MockTeleportIndexerAdapter{
		receiverAddress: receiverAddress,
		mockTxCache:     mockTxCache,
	}

	if ChainAdapter == nil {
		ChainAdapter = make(map[string]adapter.Chain)
	}
	ChainAdapter[sourceChain] = &MockChainAdapter{mockTxCache: mockTxCache}
	ChainAdapter[targetChain] = &MockChainAdapter{mockTxCache: mockTxCache}

	fmt.Println("\n========================================")
	fmt.Println("MRC20 Teleport Mempool 余额测试")
	fmt.Println("========================================")

	// Step 1: 创建前置数据
	fmt.Println("\n📝 Step 1: 创建前置数据")

	tickInfo := &mrc20.Mrc20DeployInfo{
		Tick:    tickName,
		Mrc20Id: tickId,
		Chain:   sourceChain,
	}
	PebbleStore.SaveMrc20Tick([]mrc20.Mrc20DeployInfo{*tickInfo})

	initialBalance := &mrc20.Mrc20AccountBalance{
		Address:          senderAddress,
		TickId:           tickId,
		Tick:             tickName,
		Balance:          decimal.NewFromInt(100),
		Chain:            sourceChain,
		LastUpdateHeight: 100,
		UtxoCount:        1,
	}
	PebbleStore.SaveMrc20AccountBalance(initialBalance)

	sourceUtxo := mrc20.Mrc20Utxo{
		Tick:        tickName,
		Mrc20Id:     tickId,
		TxPoint:     sourceUtxoTxPoint,
		PointValue:  546,
		BlockHeight: 100,
		MrcOption:   mrc20.OptionMint,
		ToAddress:   senderAddress,
		AmtChange:   decimal.NewFromInt(100),
		Status:      mrc20.UtxoStatusAvailable,
		Chain:       sourceChain,
		Verify:      true,
	}
	PebbleStore.SaveMrc20Pin([]mrc20.Mrc20Utxo{sourceUtxo})

	fmt.Printf("  ✓ 发送方初始余额: 100 (%s)\n", senderAddress)
	fmt.Printf("  ✓ 源 UTXO: %s (amt=100)\n", sourceUtxoTxPoint)

	// 验证初始状态
	senderBal, _ := PebbleStore.GetMrc20AccountBalance(sourceChain, senderAddress, tickId)
	fmt.Printf("  📊 初始状态 - 发送方: Balance=%s, PendingOut=%s, PendingIn=%s\n",
		senderBal.Balance.String(), senderBal.PendingOut.String(), senderBal.PendingIn.String())

	// ========================================
	// Step 2: Teleport Transfer 进入 Mempool
	// ========================================
	fmt.Println("\n📝 Step 2: Teleport Transfer 进入 Mempool (handleMempoolPin)")

	teleportContent := mrc20.Mrc20TeleportTransferData{
		Id:     tickId,
		Amount: "100",
		Coord:  arrivalPinId,
		Chain:  targetChain,
		Type:   "teleport",
	}
	contentBody, _ := json.Marshal(teleportContent)

	teleportPin := &pin.PinInscription{
		Id:                 "pin_" + teleportTxId + "i0",
		GenesisTransaction: teleportTxId,
		Path:               "/ft/mrc20/transfer",
		ContentBody:        contentBody,
		ChainName:          sourceChain,
		Address:            senderAddress,
		GenesisHeight:      -1, // mempool
		CreateAddress:      senderAddress,
		Output:             sourceUtxoTxPoint,
	}

	// 调用 handleMempoolPin
	handleMempoolPin(teleportPin)

	// 验证 mempool 后状态
	fmt.Println("\n  🔍 Teleport Transfer 进入 mempool 后的状态:")

	// 源 UTXO 状态
	utxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(sourceUtxoTxPoint, false)
	if utxo != nil {
		statusName := map[int]string{0: "Available", 1: "TeleportPending", 2: "TransferPending", -1: "Spent"}
		fmt.Printf("    源 UTXO 状态: %s (%d)\n", statusName[utxo.Status], utxo.Status)
	}

	// 发送方余额
	senderBal, _ = PebbleStore.GetMrc20AccountBalance(sourceChain, senderAddress, tickId)
	if senderBal != nil {
		fmt.Printf("    发送方 (%s): Balance=%s, PendingOut=%s, PendingIn=%s\n",
			sourceChain, senderBal.Balance.String(), senderBal.PendingOut.String(), senderBal.PendingIn.String())
	}

	// 接收方在目标链的余额（此时应该还没有）
	receiverBal, _ := PebbleStore.GetMrc20AccountBalance(targetChain, receiverAddress, tickId)
	if receiverBal != nil {
		fmt.Printf("    接收方 (%s): Balance=%s, PendingOut=%s, PendingIn=%s\n",
			targetChain, receiverBal.Balance.String(), receiverBal.PendingOut.String(), receiverBal.PendingIn.String())
	} else {
		fmt.Printf("    接收方 (%s): 余额记录未创建（正常，需要 arrival）\n", targetChain)
	}

	// 检查 PendingTeleport
	pending, _ := PebbleStore.GetPendingTeleportByCoord(arrivalPinId)
	if pending != nil {
		fmt.Printf("    ✓ PendingTeleport 已创建: coord=%s, amount=%s\n", pending.Coord, pending.Amount)
	}

	// ========================================
	// Step 3: Arrival 进入 Mempool
	// ========================================
	fmt.Println("\n📝 Step 3: Arrival 进入 Mempool (handleMempoolPin)")

	arrivalContent := mrc20.Mrc20ArrivalData{
		AssetOutpoint: sourceUtxoTxPoint,
		Amount:        mrc20.FlexibleString("100"),
		TickId:        tickId,
		LocationIndex: 0,
	}
	arrivalContentBody, _ := json.Marshal(arrivalContent)

	arrivalPin := &pin.PinInscription{
		Id:                 arrivalPinId,
		GenesisTransaction: arrivalTxId,
		Path:               "/ft/mrc20/arrival",
		ContentBody:        arrivalContentBody,
		ChainName:          targetChain,
		Address:            receiverAddress,
		GenesisHeight:      -1, // mempool
		CreateAddress:      receiverAddress,
	}

	// 调用 handleMempoolPin for arrival
	handleMempoolPin(arrivalPin)

	// 验证 arrival mempool 后状态
	fmt.Println("\n  🔍 Arrival 进入 mempool 后的状态:")

	// 发送方余额
	senderBal, _ = PebbleStore.GetMrc20AccountBalance(sourceChain, senderAddress, tickId)
	if senderBal != nil {
		fmt.Printf("    发送方 (%s): Balance=%s, PendingOut=%s, PendingIn=%s\n",
			sourceChain, senderBal.Balance.String(), senderBal.PendingOut.String(), senderBal.PendingIn.String())
	}

	// 查找 TeleportPendingIn 来获取接收方地址
	pendingInRecord, _ := PebbleStore.GetTeleportPendingInByCoord(arrivalPinId)
	var actualReceiverAddress string
	if pendingInRecord != nil {
		actualReceiverAddress = pendingInRecord.ToAddress
		fmt.Printf("    ✓ TeleportPendingIn 已创建: 地址=%s, 金额=%s\n", pendingInRecord.ToAddress, pendingInRecord.Amount.String())
	}

	// 接收方余额（应该有 PendingIn）
	if actualReceiverAddress != "" {
		receiverBal, _ = PebbleStore.GetMrc20AccountBalance(targetChain, actualReceiverAddress, tickId)
		if receiverBal != nil {
			fmt.Printf("    接收方 (%s): Balance=%s, PendingOut=%s, PendingIn=%s\n",
				targetChain, receiverBal.Balance.String(), receiverBal.PendingOut.String(), receiverBal.PendingIn.String())
		}
	}

	// 验证 mempool 阶段的正确状态
	fmt.Println("\n  📊 验证 mempool 阶段状态:")

	// 发送方: Balance=0, PendingOut=100, PendingIn=0
	if senderBal != nil && senderBal.Balance.Equal(decimal.Zero) && senderBal.PendingOut.Equal(decimal.NewFromInt(100)) {
		fmt.Printf("    ✅ 发送方 mempool 状态正确: Balance=0, PendingOut=100\n")
	} else if senderBal != nil {
		t.Errorf("❌ 发送方 mempool 状态错误: Balance=%s, PendingOut=%s (期望 Balance=0, PendingOut=100)",
			senderBal.Balance.String(), senderBal.PendingOut.String())
	}

	// 接收方: Balance=0, PendingOut=0, PendingIn=100
	if actualReceiverAddress != "" {
		receiverBal, _ = PebbleStore.GetMrc20AccountBalance(targetChain, actualReceiverAddress, tickId)
		if receiverBal != nil && receiverBal.Balance.Equal(decimal.Zero) && receiverBal.PendingIn.Equal(decimal.NewFromInt(100)) {
			fmt.Printf("    ✅ 接收方 mempool 状态正确: Balance=0, PendingIn=100\n")
		} else if receiverBal != nil {
			t.Errorf("❌ 接收方 mempool 状态错误: Balance=%s, PendingIn=%s (期望 Balance=0, PendingIn=100)",
				receiverBal.Balance.String(), receiverBal.PendingIn.String())
		}
	}

	// 目标链新 UTXO 应该还不存在（mempool 阶段不创建）
	targetUtxoPoint := fmt.Sprintf("%s:%d", arrivalTxId, 0)
	targetUtxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(targetUtxoPoint, false)
	if targetUtxo == nil {
		fmt.Printf("    ✅ 目标链 UTXO 未创建（正常，等待确认）\n")
	} else {
		fmt.Printf("    ⚠️ 目标链 UTXO 已创建: %s (状态=%d)\n", targetUtxoPoint, targetUtxo.Status)
	} // ========================================
	// Step 4: 区块确认
	// ========================================
	fmt.Println("\n📝 Step 4: 区块确认 (handleMrc20)")

	// 先确认 teleport
	teleportPinConfirmed := &pin.PinInscription{
		Id:                 "pin_" + teleportTxId + "i0",
		GenesisTransaction: teleportTxId,
		Path:               "/ft/mrc20/transfer",
		ContentBody:        contentBody,
		ChainName:          sourceChain,
		Address:            senderAddress,
		GenesisHeight:      200,
		CreateAddress:      senderAddress,
		Output:             sourceUtxoTxPoint,
	}

	pinList := &[]*pin.PinInscription{teleportPinConfirmed}
	txInList := &[]string{sourceUtxoTxPoint}

	PebbleStore.handleMrc20(sourceChain, 200, pinList, txInList)

	// 再确认 arrival
	arrivalPinConfirmed := &pin.PinInscription{
		Id:                 arrivalPinId,
		GenesisTransaction: arrivalTxId,
		Path:               "/ft/mrc20/arrival",
		ContentBody:        arrivalContentBody,
		ChainName:          targetChain,
		Address:            receiverAddress,
		GenesisHeight:      201,
		CreateAddress:      receiverAddress,
	}

	arrivalPinList := &[]*pin.PinInscription{arrivalPinConfirmed}
	arrivalTxInList := &[]string{}

	PebbleStore.handleMrc20(targetChain, 201, arrivalPinList, arrivalTxInList)

	// ========================================
	// Step 5: 验证最终状态
	// ========================================
	fmt.Println("\n📝 Step 5: 确认后的最终状态")

	// 发送方余额
	senderBal, _ = PebbleStore.GetMrc20AccountBalance(sourceChain, senderAddress, tickId)
	if senderBal != nil {
		fmt.Printf("    发送方 (%s): Balance=%s, PendingOut=%s, PendingIn=%s, LastUpdateHeight=%d\n",
			sourceChain, senderBal.Balance.String(), senderBal.PendingOut.String(), senderBal.PendingIn.String(), senderBal.LastUpdateHeight)
	}

	// 源 UTXO 最终状态
	finalUtxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(sourceUtxoTxPoint, false)
	if finalUtxo != nil {
		statusName := map[int]string{0: "Available", 1: "TeleportPending", 2: "TransferPending", -1: "Spent"}
		fmt.Printf("    源 UTXO 状态: %s (%d), BlockHeight=%d\n", statusName[finalUtxo.Status], finalUtxo.Status, finalUtxo.BlockHeight)
	}

	// 目标链新 UTXO
	targetUtxo, _ = PebbleStore.GetMrc20UtxoByTxPoint(targetUtxoPoint, false)
	if targetUtxo != nil {
		actualReceiverAddress = targetUtxo.ToAddress
		statusName := map[int]string{0: "Available", 1: "TeleportPending", 2: "TransferPending", -1: "Spent"}
		fmt.Printf("    目标链新 UTXO: %s, 状态=%s, 地址=%s, BlockHeight=%d\n", targetUtxoPoint, statusName[targetUtxo.Status], actualReceiverAddress, targetUtxo.BlockHeight)
	}

	// 接收方余额
	if actualReceiverAddress != "" {
		receiverBal, _ = PebbleStore.GetMrc20AccountBalance(targetChain, actualReceiverAddress, tickId)
		if receiverBal != nil {
			fmt.Printf("    接收方 (%s): Balance=%s, PendingOut=%s, PendingIn=%s, LastUpdateHeight=%d\n",
				targetChain, receiverBal.Balance.String(), receiverBal.PendingOut.String(), receiverBal.PendingIn.String(), receiverBal.LastUpdateHeight)
		}
	}

	// 检查 Arrival 状态
	arrivalRecord, _ := PebbleStore.GetMrc20ArrivalByPinId(arrivalPinId)
	if arrivalRecord != nil {
		fmt.Printf("    Arrival 记录: Status=%d, BlockHeight=%d, ToAddress=%s\n", arrivalRecord.Status, arrivalRecord.BlockHeight, arrivalRecord.ToAddress)
	}

	// 检查 PendingTeleport 状态
	pendingRecord, _ := PebbleStore.GetPendingTeleportByCoord(arrivalPinId)
	if pendingRecord != nil {
		fmt.Printf("    PendingTeleport 记录: Status=%d, BlockHeight=%d\n", pendingRecord.Status, pendingRecord.BlockHeight)
	}

	// 检查 TeleportPendingIn 是否被正确删除
	pendingIn, _ := PebbleStore.GetTeleportPendingInByCoord(arrivalPinId)
	if pendingIn == nil {
		fmt.Printf("    ✓ TeleportPendingIn 已正确删除\n")
	} else {
		fmt.Printf("    ⚠️ TeleportPendingIn 仍存在: Amount=%s\n", pendingIn.Amount.String())
	}

	// ========================================
	// 断言验证
	// ========================================
	fmt.Println("\n🔍 验证结果:")

	// 发送方余额应该是 0
	if senderBal != nil && senderBal.Balance.Equal(decimal.Zero) {
		fmt.Printf("  ✅ 发送方余额正确: 0\n")
	} else if senderBal != nil {
		t.Errorf("❌ 发送方余额错误: 期望=0, 实际=%s", senderBal.Balance.String())
	}

	// 验证发送方余额的 LastUpdateHeight (应该是 arrival 的区块高度 201)
	if senderBal != nil {
		if senderBal.LastUpdateHeight == 201 {
			fmt.Printf("  ✅ 发送方余额更新高度正确: %d\n", senderBal.LastUpdateHeight)
		} else {
			t.Errorf("❌ 发送方余额更新高度错误: 期望=201, 实际=%d", senderBal.LastUpdateHeight)
		}
	}

	// 接收方余额应该是 100
	if receiverBal != nil && receiverBal.Balance.Equal(decimal.NewFromInt(100)) {
		fmt.Printf("  ✅ 接收方余额正确: 100\n")
	} else if receiverBal != nil {
		t.Errorf("❌ 接收方余额错误: 期望=100, 实际=%s", receiverBal.Balance.String())
	} else {
		t.Errorf("❌ 接收方余额未找到")
	}

	// 验证目标链 UTXO 区块高度 (应该是 arrival 的区块高度 201)
	if targetUtxo != nil {
		if targetUtxo.BlockHeight == 201 {
			fmt.Printf("  ✅ 目标链 UTXO 区块高度正确: %d\n", targetUtxo.BlockHeight)
		} else {
			t.Errorf("❌ 目标链 UTXO 区块高度错误: 期望=201, 实际=%d", targetUtxo.BlockHeight)
		}
	}

	// 验证接收方余额的 LastUpdateHeight (应该是 arrival 的区块高度 201)
	if receiverBal != nil {
		if receiverBal.LastUpdateHeight == 201 {
			fmt.Printf("  ✅ 接收方余额更新高度正确: %d\n", receiverBal.LastUpdateHeight)
		} else {
			t.Errorf("❌ 接收方余额更新高度错误: 期望=201, 实际=%d", receiverBal.LastUpdateHeight)
		}
	}

	// 验证 Arrival 状态 (应该是 1=completed)
	if arrivalRecord != nil {
		if arrivalRecord.Status == 1 {
			fmt.Printf("  ✅ Arrival 状态正确: completed (%d)\n", arrivalRecord.Status)
		} else {
			t.Errorf("❌ Arrival 状态错误: 期望=1(completed), 实际=%d", arrivalRecord.Status)
		}
		// 验证 Arrival 区块高度
		if arrivalRecord.BlockHeight == 201 {
			fmt.Printf("  ✅ Arrival 区块高度正确: %d\n", arrivalRecord.BlockHeight)
		} else {
			t.Errorf("❌ Arrival 区块高度错误: 期望=201, 实际=%d", arrivalRecord.BlockHeight)
		}
	}

	// 验证 TeleportPendingIn 已被删除
	if pendingIn != nil {
		t.Errorf("❌ TeleportPendingIn 应该被删除，但仍存在: Amount=%s", pendingIn.Amount.String())
	} else {
		fmt.Printf("  ✅ TeleportPendingIn 已正确删除\n")
	}

	fmt.Println("\n========================================")
}
