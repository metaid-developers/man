package man

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
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
	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"
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

// ========================================
// API-perspective balance helpers
// ========================================

// APIBalanceResult mirrors what the API returns to clients
type APIBalanceResult struct {
	Balance    decimal.Decimal
	PendingIn  decimal.Decimal
	PendingOut decimal.Decimal
	UtxoCount  int
}

// queryBalanceLikeAPI replicates the (post-fix) API balance calculation:
//   Balance    = AccountBalance.Balance  (DB cache, equals SUM of Available UTXOs)
//   PendingOut = scan UTXO table for Status=TeleportPending|TransferPending
//   PendingIn  = TeleportPendingIn table + TransferPendingIn table
func queryBalanceLikeAPI(chain, address, tickId string) *APIBalanceResult {
	result := &APIBalanceResult{}

	// 1. Read AccountBalance
	bal, err := PebbleStore.GetMrc20AccountBalance(chain, address, tickId)
	if err != nil || bal == nil {
		return result
	}
	result.Balance = bal.Balance
	result.UtxoCount = bal.UtxoCount

	// 2. Scan UTXOs for pendingOut (Status=1 or 2)
	prefix := []byte(fmt.Sprintf("mrc20_in_%s_%s_", address, tickId))
	iter, err := PebbleStore.Database.MrcDb.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err == nil {
		for iter.First(); iter.Valid(); iter.Next() {
			var utxo mrc20.Mrc20Utxo
			if err := sonic.Unmarshal(iter.Value(), &utxo); err != nil {
				continue
			}
			if utxo.ToAddress != address {
				continue
			}
			if utxo.Status == mrc20.UtxoStatusTeleportPending || utxo.Status == mrc20.UtxoStatusTransferPending {
				amt := utxo.AmtChange
				if amt.LessThan(decimal.Zero) {
					amt = amt.Neg()
				}
				result.PendingOut = result.PendingOut.Add(amt)
			}
		}
		iter.Close()
	}

	// 3. TeleportPendingIn
	teleportPendingIns, _ := PebbleStore.GetTeleportPendingInByAddress(address)
	for _, p := range teleportPendingIns {
		if p.TickId == tickId && p.Chain == chain {
			result.PendingIn = result.PendingIn.Add(p.Amount)
		}
	}

	// 4. TransferPendingIn
	transferPendingIns, _ := PebbleStore.GetTransferPendingInByAddress(address)
	for _, p := range transferPendingIns {
		if p.TickId == tickId && p.Chain == chain {
			result.PendingIn = result.PendingIn.Add(p.Amount)
		}
	}

	return result
}

// assertAPIBalance asserts API-perspective balance and prints diagnostic info on failure
func assertAPIBalance(t *testing.T, label, chain, address, tickId string,
	wantBalance, wantPendingIn, wantPendingOut decimal.Decimal) {
	t.Helper()

	got := queryBalanceLikeAPI(chain, address, tickId)

	ok := true
	if !got.Balance.Equal(wantBalance) {
		ok = false
	}
	if !got.PendingIn.Equal(wantPendingIn) {
		ok = false
	}
	if !got.PendingOut.Equal(wantPendingOut) {
		ok = false
	}

	if !ok {
		// dump DB internals for debugging
		dbBal, _ := PebbleStore.GetMrc20AccountBalance(chain, address, tickId)
		dbInfo := "nil"
		if dbBal != nil {
			dbInfo = fmt.Sprintf("Balance=%s PendingOut=%s PendingIn=%s UtxoCount=%d",
				dbBal.Balance, dbBal.PendingOut, dbBal.PendingIn, dbBal.UtxoCount)
		}

		t.Errorf("[%s] API balance mismatch for %s@%s/%s\n"+
			"  want: bal=%s pIn=%s pOut=%s\n"+
			"  got:  bal=%s pIn=%s pOut=%s\n"+
			"  DB:   %s",
			label, address, chain, tickId,
			wantBalance, wantPendingIn, wantPendingOut,
			got.Balance, got.PendingIn, got.PendingOut,
			dbInfo)
	} else {
		t.Logf("[%s] %s@%s: bal=%s pIn=%s pOut=%s",
			label, address, chain, got.Balance, got.PendingIn, got.PendingOut)
	}
}

// ========================================
// Test: Teleport First, then Arrival
// ========================================

func TestMRC20TeleportFlow_TeleportFirst(t *testing.T) {
	initTeleportTestConfig()

	tmpDir, err := os.MkdirTemp("", "mrc20_teleport_test_*")
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

	sourceChain := "doge"
	targetChain := "btc"
	senderAddress := "DDnnM5GP3o87EDL42PifPMzrtB7xSZhhVg"
	receiverAddress := "D5ERdEN1gsouFSs7zsq7VYJxyWP6dP28H1"
	tickId := "teleport_tick_001"
	tickName := "TELEPORT"

	sourceTxId := "0000000000000000000000000000000000000000000000000000000000000010"
	sourceUtxoTxPoint := sourceTxId + ":0"
	teleportTxId := "0000000000000000000000000000000000000000000000000000000000000020"
	arrivalTxId := "0000000000000000000000000000000000000000000000000000000000000030"
	arrivalPinId := "pin_" + arrivalTxId + "i0"

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

	if IndexerAdapter == nil {
		IndexerAdapter = make(map[string]adapter.Indexer)
	}
	IndexerAdapter[sourceChain] = &MockTeleportIndexerAdapter{receiverAddress: receiverAddress, mockTxCache: mockTxCache}
	IndexerAdapter[targetChain] = &MockTeleportIndexerAdapter{receiverAddress: receiverAddress, mockTxCache: mockTxCache}

	if ChainAdapter == nil {
		ChainAdapter = make(map[string]adapter.Chain)
	}
	ChainAdapter[sourceChain] = &MockChainAdapter{mockTxCache: mockTxCache}
	ChainAdapter[targetChain] = &MockChainAdapter{mockTxCache: mockTxCache}

	// ── Stage 0: Setup initial data ──
	t.Log("── Stage 0: Setup initial data")

	PebbleStore.SaveMrc20Tick([]mrc20.Mrc20DeployInfo{{
		Tick: tickName, Mrc20Id: tickId, Chain: sourceChain,
	}})

	PebbleStore.SaveMrc20AccountBalance(&mrc20.Mrc20AccountBalance{
		Address: senderAddress, TickId: tickId, Tick: tickName,
		Balance: decimal.NewFromInt(100), Chain: sourceChain,
		LastUpdateHeight: 100, UtxoCount: 1,
	})

	PebbleStore.SaveMrc20Pin([]mrc20.Mrc20Utxo{{
		Tick: tickName, Mrc20Id: tickId, TxPoint: sourceUtxoTxPoint,
		PointValue: 546, BlockHeight: 100, MrcOption: mrc20.OptionMint,
		ToAddress: senderAddress, AmtChange: decimal.NewFromInt(100),
		Status: mrc20.UtxoStatusAvailable, Chain: sourceChain, Verify: true,
	}})

	assertAPIBalance(t, "Stage0-sender", sourceChain, senderAddress, tickId,
		decimal.NewFromInt(100), decimal.Zero, decimal.Zero)

	// ── Stage 1: Teleport transfer 出块 ──
	t.Log("── Stage 1: Teleport transfer 出块")

	teleportContent := mrc20.Mrc20TeleportTransferData{
		Id: tickId, Amount: "100", Coord: arrivalPinId, Chain: targetChain, Type: "teleport",
	}
	contentBody, _ := json.Marshal(teleportContent)

	teleportPin := &pin.PinInscription{
		Id: "pin_" + teleportTxId + "i0", GenesisTransaction: teleportTxId,
		Path: "/ft/mrc20/transfer", ContentBody: contentBody,
		ChainName: sourceChain, Address: senderAddress, GenesisHeight: 200,
		CreateAddress: senderAddress, Output: sourceUtxoTxPoint,
	}

	pinList := &[]*pin.PinInscription{teleportPin}
	txInList := &[]string{sourceUtxoTxPoint}
	PebbleStore.handleMrc20(sourceChain, 200, pinList, txInList)

	// After teleport: sender UTXO is TeleportPending, balance=0, pendingOut=100
	assertAPIBalance(t, "Stage1-sender", sourceChain, senderAddress, tickId,
		decimal.Zero, decimal.Zero, decimal.NewFromInt(100))

	// Verify pending teleport record exists
	pendingTeleport, err := PebbleStore.GetPendingTeleportByCoord(arrivalPinId)
	if err != nil || pendingTeleport == nil {
		t.Fatalf("PendingTeleport not found after teleport: %v", err)
	}

	// ── Stage 2: Arrival 出块 ──
	t.Log("── Stage 2: Arrival 出块")

	arrivalContent := mrc20.Mrc20ArrivalData{
		AssetOutpoint: sourceUtxoTxPoint, Amount: mrc20.FlexibleString("100"),
		TickId: tickId, LocationIndex: 0,
	}
	arrivalContentBody, _ := json.Marshal(arrivalContent)

	arrivalPin := &pin.PinInscription{
		Id: arrivalPinId, GenesisTransaction: arrivalTxId,
		Path: "/ft/mrc20/arrival", ContentBody: arrivalContentBody,
		ChainName: targetChain, Address: receiverAddress, GenesisHeight: 201,
		CreateAddress: receiverAddress,
	}

	arrivalPinList := &[]*pin.PinInscription{arrivalPin}
	arrivalTxInList := &[]string{}
	PebbleStore.handleMrc20(targetChain, 201, arrivalPinList, arrivalTxInList)

	// After arrival completes teleport: sender spent, receiver gets 100
	assertAPIBalance(t, "Stage2-sender", sourceChain, senderAddress, tickId,
		decimal.Zero, decimal.Zero, decimal.Zero)

	// Find actual receiver address from the target UTXO
	targetUtxoPoint := fmt.Sprintf("%s:%d", arrivalTxId, 0)
	targetUtxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(targetUtxoPoint, false)
	actualReceiverAddr := receiverAddress
	if targetUtxo != nil {
		actualReceiverAddr = targetUtxo.ToAddress
	}

	assertAPIBalance(t, "Stage2-receiver", targetChain, actualReceiverAddr, tickId,
		decimal.NewFromInt(100), decimal.Zero, decimal.Zero)

	// Verify final states
	// executeTeleportTransfer deletes the source UTXO from DB (not just marks Spent)
	finalUtxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(sourceUtxoTxPoint, false)
	if finalUtxo != nil {
		t.Errorf("source UTXO should be deleted after teleport, but still exists with status=%d", finalUtxo.Status)
	}
	if targetUtxo == nil || targetUtxo.Status != mrc20.UtxoStatusAvailable {
		t.Errorf("target UTXO should be Available, got %v", targetUtxo)
	}

	// Verify teleport record
	teleportExists := PebbleStore.CheckTeleportExists(arrivalPinId)
	if !teleportExists {
		t.Error("Teleport record should exist after completion")
	}

	// Verify arrival is completed
	arrivalRecord, _ := PebbleStore.GetMrc20ArrivalByPinId(arrivalPinId)
	if arrivalRecord == nil || arrivalRecord.Status != mrc20.ArrivalStatusCompleted {
		t.Errorf("Arrival should be Completed, got %v", arrivalRecord)
	}
}

// ========================================
// Test: Arrival First, then Teleport
// ========================================

func TestMRC20TeleportFlow_ArrivalFirst(t *testing.T) {
	initTeleportTestConfig()

	tmpDir, err := os.MkdirTemp("", "mrc20_teleport_arrival_first_*")
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

	sourceChain := "doge"
	targetChain := "btc"
	senderAddress := "DDnnM5GP3o87EDL42PifPMzrtB7xSZhhVg"
	receiverAddress := "D5ERdEN1gsouFSs7zsq7VYJxyWP6dP28H1"
	tickId := "teleport_tick_002"
	tickName := "TELEPORT2"

	sourceTxId := "0000000000000000000000000000000000000000000000000000000000000050"
	sourceUtxoTxPoint := sourceTxId + ":0"
	teleportTxId := "0000000000000000000000000000000000000000000000000000000000000060"
	arrivalTxId := "0000000000000000000000000000000000000000000000000000000000000070"
	arrivalPinId := "pin_" + arrivalTxId + "i0"

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

	if IndexerAdapter == nil {
		IndexerAdapter = make(map[string]adapter.Indexer)
	}
	IndexerAdapter[sourceChain] = &MockTeleportIndexerAdapter{receiverAddress: receiverAddress, mockTxCache: mockTxCache}
	IndexerAdapter[targetChain] = &MockTeleportIndexerAdapter{receiverAddress: receiverAddress, mockTxCache: mockTxCache}

	if ChainAdapter == nil {
		ChainAdapter = make(map[string]adapter.Chain)
	}
	ChainAdapter[sourceChain] = &MockChainAdapter{mockTxCache: mockTxCache}
	ChainAdapter[targetChain] = &MockChainAdapter{mockTxCache: mockTxCache}

	// ── Stage 0: Setup initial data ──
	t.Log("── Stage 0: Setup initial data")

	PebbleStore.SaveMrc20Tick([]mrc20.Mrc20DeployInfo{{
		Tick: tickName, Mrc20Id: tickId, Chain: sourceChain,
	}})

	PebbleStore.SaveMrc20AccountBalance(&mrc20.Mrc20AccountBalance{
		Address: senderAddress, TickId: tickId, Tick: tickName,
		Balance: decimal.NewFromInt(100), Chain: sourceChain,
		LastUpdateHeight: 100, UtxoCount: 1,
	})

	PebbleStore.SaveMrc20Pin([]mrc20.Mrc20Utxo{{
		Tick: tickName, Mrc20Id: tickId, TxPoint: sourceUtxoTxPoint,
		PointValue: 546, BlockHeight: 100, MrcOption: mrc20.OptionMint,
		ToAddress: senderAddress, AmtChange: decimal.NewFromInt(100),
		Status: mrc20.UtxoStatusAvailable, Chain: sourceChain, Verify: true,
	}})

	assertAPIBalance(t, "Stage0-sender", sourceChain, senderAddress, tickId,
		decimal.NewFromInt(100), decimal.Zero, decimal.Zero)

	// ── Stage 1: Arrival 先出块 (source UTXO still Available → arrival pending) ──
	t.Log("── Stage 1: Arrival 先出块")

	arrivalContent := mrc20.Mrc20ArrivalData{
		AssetOutpoint: sourceUtxoTxPoint, Amount: mrc20.FlexibleString("100"),
		TickId: tickId, LocationIndex: 0,
	}
	arrivalContentBody, _ := json.Marshal(arrivalContent)

	arrivalPin := &pin.PinInscription{
		Id: arrivalPinId, GenesisTransaction: arrivalTxId,
		Path: "/ft/mrc20/arrival", ContentBody: arrivalContentBody,
		ChainName: targetChain, Address: receiverAddress, GenesisHeight: 200,
		CreateAddress: receiverAddress,
	}

	arrivalPinList := &[]*pin.PinInscription{arrivalPin}
	arrivalTxInList := &[]string{}
	PebbleStore.handleMrc20(targetChain, 200, arrivalPinList, arrivalTxInList)

	// Source UTXO should still be Available; arrival is pending
	assertAPIBalance(t, "Stage1-sender", sourceChain, senderAddress, tickId,
		decimal.NewFromInt(100), decimal.Zero, decimal.Zero)

	arrival, _ := PebbleStore.GetMrc20ArrivalByPinId(arrivalPinId)
	if arrival == nil || arrival.Status != mrc20.ArrivalStatusPending {
		t.Fatalf("Arrival should be Pending, got %v", arrival)
	}

	// ── Stage 2: Teleport Transfer 出块 (detects arrival → completes teleport) ──
	t.Log("── Stage 2: Teleport Transfer 出块")

	teleportContent := mrc20.Mrc20TeleportTransferData{
		Id: tickId, Amount: "100", Coord: arrivalPinId, Chain: targetChain, Type: "teleport",
	}
	contentBody, _ := json.Marshal(teleportContent)

	teleportPin := &pin.PinInscription{
		Id: "pin_" + teleportTxId + "i0", GenesisTransaction: teleportTxId,
		Path: "/ft/mrc20/transfer", ContentBody: contentBody,
		ChainName: sourceChain, Address: senderAddress, GenesisHeight: 201,
		CreateAddress: senderAddress, Output: sourceUtxoTxPoint,
	}

	pinList := &[]*pin.PinInscription{teleportPin}
	txInList := &[]string{sourceUtxoTxPoint}
	PebbleStore.handleMrc20(sourceChain, 201, pinList, txInList)

	// After teleport completes: sender=0, receiver=100
	assertAPIBalance(t, "Stage2-sender", sourceChain, senderAddress, tickId,
		decimal.Zero, decimal.Zero, decimal.Zero)

	targetUtxoPoint := fmt.Sprintf("%s:%d", arrivalTxId, 0)
	targetUtxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(targetUtxoPoint, false)
	actualReceiverAddr := receiverAddress
	if targetUtxo != nil {
		actualReceiverAddr = targetUtxo.ToAddress
	}

	assertAPIBalance(t, "Stage2-receiver", targetChain, actualReceiverAddr, tickId,
		decimal.NewFromInt(100), decimal.Zero, decimal.Zero)

	// Verify final states
	// executeTeleportTransfer deletes the source UTXO from DB
	finalUtxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(sourceUtxoTxPoint, false)
	if finalUtxo != nil {
		t.Errorf("source UTXO should be deleted after teleport, but still exists with status=%d", finalUtxo.Status)
	}
	if targetUtxo == nil || targetUtxo.Status != mrc20.UtxoStatusAvailable {
		t.Errorf("target UTXO should be Available, got %v", targetUtxo)
	}

	// Verify teleport record
	teleportExists := PebbleStore.CheckTeleportExists(arrivalPinId)
	if !teleportExists {
		t.Error("Teleport record should exist after completion")
	}

	// Verify arrival is completed
	arrivalRecordFinal, _ := PebbleStore.GetMrc20ArrivalByPinId(arrivalPinId)
	if arrivalRecordFinal == nil || arrivalRecordFinal.Status != mrc20.ArrivalStatusCompleted {
		t.Errorf("Arrival should be Completed, got %v", arrivalRecordFinal)
	}
}

// ========================================
// Test: Mempool Balance (5-stage flow)
// ========================================

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

	PebbleStore = &PebbleData{Database: db}
	common.Config.Module = []string{"mrc20"}

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

	if IndexerAdapter == nil {
		IndexerAdapter = make(map[string]adapter.Indexer)
	}
	IndexerAdapter[sourceChain] = &MockTeleportIndexerAdapter{receiverAddress: receiverAddress, mockTxCache: mockTxCache}
	IndexerAdapter[targetChain] = &MockTeleportIndexerAdapter{receiverAddress: receiverAddress, mockTxCache: mockTxCache}

	if ChainAdapter == nil {
		ChainAdapter = make(map[string]adapter.Chain)
	}
	ChainAdapter[sourceChain] = &MockChainAdapter{mockTxCache: mockTxCache}
	ChainAdapter[targetChain] = &MockChainAdapter{mockTxCache: mockTxCache}

	// ── Stage 0: Initial state ──
	t.Log("── Stage 0: Initial state")

	PebbleStore.SaveMrc20Tick([]mrc20.Mrc20DeployInfo{{
		Tick: tickName, Mrc20Id: tickId, Chain: sourceChain,
	}})

	PebbleStore.SaveMrc20AccountBalance(&mrc20.Mrc20AccountBalance{
		Address: senderAddress, TickId: tickId, Tick: tickName,
		Balance: decimal.NewFromInt(100), Chain: sourceChain,
		LastUpdateHeight: 100, UtxoCount: 1,
	})

	PebbleStore.SaveMrc20Pin([]mrc20.Mrc20Utxo{{
		Tick: tickName, Mrc20Id: tickId, TxPoint: sourceUtxoTxPoint,
		PointValue: 546, BlockHeight: 100, MrcOption: mrc20.OptionMint,
		ToAddress: senderAddress, AmtChange: decimal.NewFromInt(100),
		Status: mrc20.UtxoStatusAvailable, Chain: sourceChain, Verify: true,
	}})

	assertAPIBalance(t, "Stage0-sender", sourceChain, senderAddress, tickId,
		decimal.NewFromInt(100), decimal.Zero, decimal.Zero)

	// ── Stage 1: Teleport Transfer enters mempool ──
	t.Log("── Stage 1: Teleport Transfer enters mempool")

	teleportContent := mrc20.Mrc20TeleportTransferData{
		Id: tickId, Amount: "100", Coord: arrivalPinId, Chain: targetChain, Type: "teleport",
	}
	contentBody, _ := json.Marshal(teleportContent)

	teleportPin := &pin.PinInscription{
		Id: "pin_" + teleportTxId + "i0", GenesisTransaction: teleportTxId,
		Path: "/ft/mrc20/transfer", ContentBody: contentBody,
		ChainName: sourceChain, Address: senderAddress, GenesisHeight: -1,
		CreateAddress: senderAddress, Output: sourceUtxoTxPoint,
	}

	handleMempoolPin(teleportPin)

	// sender: bal=0 (UTXO now TeleportPending), pOut=100, pIn=0
	assertAPIBalance(t, "Stage1-sender", sourceChain, senderAddress, tickId,
		decimal.Zero, decimal.Zero, decimal.NewFromInt(100))

	// ── Stage 2: Arrival enters mempool ──
	t.Log("── Stage 2: Arrival enters mempool")

	arrivalContent := mrc20.Mrc20ArrivalData{
		AssetOutpoint: sourceUtxoTxPoint, Amount: mrc20.FlexibleString("100"),
		TickId: tickId, LocationIndex: 0,
	}
	arrivalContentBody, _ := json.Marshal(arrivalContent)

	arrivalPin := &pin.PinInscription{
		Id: arrivalPinId, GenesisTransaction: arrivalTxId,
		Path: "/ft/mrc20/arrival", ContentBody: arrivalContentBody,
		ChainName: targetChain, Address: receiverAddress, GenesisHeight: -1,
		CreateAddress: receiverAddress,
	}

	handleMempoolPin(arrivalPin)

	// sender unchanged
	assertAPIBalance(t, "Stage2-sender", sourceChain, senderAddress, tickId,
		decimal.Zero, decimal.Zero, decimal.NewFromInt(100))

	// receiver: should have pendingIn=100 from TeleportPendingIn table
	pendingInRecord, _ := PebbleStore.GetTeleportPendingInByCoord(arrivalPinId)
	var actualReceiverAddr string
	if pendingInRecord != nil {
		actualReceiverAddr = pendingInRecord.ToAddress
		t.Logf("  TeleportPendingIn created: addr=%s amount=%s", pendingInRecord.ToAddress, pendingInRecord.Amount)
	}
	if actualReceiverAddr != "" {
		assertAPIBalance(t, "Stage2-receiver", targetChain, actualReceiverAddr, tickId,
			decimal.Zero, decimal.NewFromInt(100), decimal.Zero)
	}

	// ── Stage 3: Teleport confirms (出块) ──
	t.Log("── Stage 3: Teleport confirms (出块)")

	teleportPinConfirmed := &pin.PinInscription{
		Id: "pin_" + teleportTxId + "i0", GenesisTransaction: teleportTxId,
		Path: "/ft/mrc20/transfer", ContentBody: contentBody,
		ChainName: sourceChain, Address: senderAddress, GenesisHeight: 200,
		CreateAddress: senderAddress, Output: sourceUtxoTxPoint,
	}

	pinList := &[]*pin.PinInscription{teleportPinConfirmed}
	txInList := &[]string{sourceUtxoTxPoint}
	PebbleStore.handleMrc20(sourceChain, 200, pinList, txInList)

	// sender: still pending (teleport side confirmed, but arrival not yet confirmed)
	assertAPIBalance(t, "Stage3-sender", sourceChain, senderAddress, tickId,
		decimal.Zero, decimal.Zero, decimal.NewFromInt(100))

	// receiver: still pending
	if actualReceiverAddr != "" {
		assertAPIBalance(t, "Stage3-receiver", targetChain, actualReceiverAddr, tickId,
			decimal.Zero, decimal.NewFromInt(100), decimal.Zero)
	}

	// ── Stage 4: Arrival confirms (出块) → teleport completes ──
	t.Log("── Stage 4: Arrival confirms (出块)")

	arrivalPinConfirmed := &pin.PinInscription{
		Id: arrivalPinId, GenesisTransaction: arrivalTxId,
		Path: "/ft/mrc20/arrival", ContentBody: arrivalContentBody,
		ChainName: targetChain, Address: receiverAddress, GenesisHeight: 201,
		CreateAddress: receiverAddress,
	}

	arrivalPinList := &[]*pin.PinInscription{arrivalPinConfirmed}
	arrivalTxInList := &[]string{}
	PebbleStore.handleMrc20(targetChain, 201, arrivalPinList, arrivalTxInList)

	// sender: fully spent, all pending cleared
	assertAPIBalance(t, "Stage4-sender", sourceChain, senderAddress, tickId,
		decimal.Zero, decimal.Zero, decimal.Zero)

	// receiver: balance=100, pending cleared
	targetUtxoPoint := fmt.Sprintf("%s:%d", arrivalTxId, 0)
	targetUtxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(targetUtxoPoint, false)
	if targetUtxo != nil {
		actualReceiverAddr = targetUtxo.ToAddress
	}
	assertAPIBalance(t, "Stage4-receiver", targetChain, actualReceiverAddr, tickId,
		decimal.NewFromInt(100), decimal.Zero, decimal.Zero)

	// Verify final artifacts
	arrivalRecord, _ := PebbleStore.GetMrc20ArrivalByPinId(arrivalPinId)
	if arrivalRecord == nil || arrivalRecord.Status != mrc20.ArrivalStatusCompleted {
		t.Errorf("Arrival should be Completed, got %v", arrivalRecord)
	}

	pendingIn, _ := PebbleStore.GetTeleportPendingInByCoord(arrivalPinId)
	if pendingIn != nil {
		t.Errorf("TeleportPendingIn should be deleted, but still exists: Amount=%s", pendingIn.Amount)
	}

	if targetUtxo != nil && targetUtxo.BlockHeight != 201 {
		t.Errorf("target UTXO block height should be 201, got %d", targetUtxo.BlockHeight)
	}
}

// ========================================
// Test: Mempool Arrival First (5-stage flow)
// Arrival enters mempool → Teleport enters mempool → Arrival confirms → Teleport confirms
// ========================================

func TestMRC20TeleportFlow_MempoolArrivalFirst(t *testing.T) {
	initTeleportTestConfig()

	tmpDir, err := os.MkdirTemp("", "mrc20_teleport_mempool_arrival_first_*")
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

	sourceChain := "doge"
	targetChain := "btc"
	senderAddress := "DDnnM5GP3o87EDL42PifPMzrtB7xSZhhVg"
	receiverAddress := "D5ERdEN1gsouFSs7zsq7VYJxyWP6dP28H1"
	tickId := "teleport_arrival_first_tick"
	tickName := "TARRFIRST"

	sourceTxId := "0000000000000000000000000000000000000000000000000000000000000400"
	sourceUtxoTxPoint := sourceTxId + ":0"
	teleportTxId := "0000000000000000000000000000000000000000000000000000000000000500"
	arrivalTxId := "0000000000000000000000000000000000000000000000000000000000000600"
	arrivalPinId := "pin_" + arrivalTxId + "i0"

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

	if IndexerAdapter == nil {
		IndexerAdapter = make(map[string]adapter.Indexer)
	}
	IndexerAdapter[sourceChain] = &MockTeleportIndexerAdapter{receiverAddress: receiverAddress, mockTxCache: mockTxCache}
	IndexerAdapter[targetChain] = &MockTeleportIndexerAdapter{receiverAddress: receiverAddress, mockTxCache: mockTxCache}

	if ChainAdapter == nil {
		ChainAdapter = make(map[string]adapter.Chain)
	}
	ChainAdapter[sourceChain] = &MockChainAdapter{mockTxCache: mockTxCache}
	ChainAdapter[targetChain] = &MockChainAdapter{mockTxCache: mockTxCache}

	// ── Stage 0: Initial state ──
	t.Log("── Stage 0: Initial state")

	PebbleStore.SaveMrc20Tick([]mrc20.Mrc20DeployInfo{{
		Tick: tickName, Mrc20Id: tickId, Chain: sourceChain,
	}})

	PebbleStore.SaveMrc20AccountBalance(&mrc20.Mrc20AccountBalance{
		Address: senderAddress, TickId: tickId, Tick: tickName,
		Balance: decimal.NewFromInt(100), Chain: sourceChain,
		LastUpdateHeight: 100, UtxoCount: 1,
	})

	PebbleStore.SaveMrc20Pin([]mrc20.Mrc20Utxo{{
		Tick: tickName, Mrc20Id: tickId, TxPoint: sourceUtxoTxPoint,
		PointValue: 546, BlockHeight: 100, MrcOption: mrc20.OptionMint,
		ToAddress: senderAddress, AmtChange: decimal.NewFromInt(100),
		Status: mrc20.UtxoStatusAvailable, Chain: sourceChain, Verify: true,
	}})

	assertAPIBalance(t, "Stage0-sender", sourceChain, senderAddress, tickId,
		decimal.NewFromInt(100), decimal.Zero, decimal.Zero)

	// ── Stage 1: Arrival enters mempool ──
	t.Log("── Stage 1: Arrival enters mempool")

	arrivalContent := mrc20.Mrc20ArrivalData{
		AssetOutpoint: sourceUtxoTxPoint, Amount: mrc20.FlexibleString("100"),
		TickId: tickId, LocationIndex: 0,
	}
	arrivalContentBody, _ := json.Marshal(arrivalContent)

	arrivalPin := &pin.PinInscription{
		Id: arrivalPinId, GenesisTransaction: arrivalTxId,
		Path: "/ft/mrc20/arrival", ContentBody: arrivalContentBody,
		ChainName: targetChain, Address: receiverAddress, GenesisHeight: -1,
		CreateAddress: receiverAddress,
	}

	handleMempoolPin(arrivalPin)

	// Source UTXO still Available, no teleport yet → sender unchanged
	sourceUtxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(sourceUtxoTxPoint, false)
	if sourceUtxo == nil || sourceUtxo.Status != mrc20.UtxoStatusAvailable {
		t.Fatalf("Stage1: source UTXO should still be Available, got %v", sourceUtxo)
	}
	assertAPIBalance(t, "Stage1-sender", sourceChain, senderAddress, tickId,
		decimal.NewFromInt(100), decimal.Zero, decimal.Zero)

	// Arrival should be Pending in mempool
	arrival, _ := PebbleStore.GetMrc20ArrivalByPinId(arrivalPinId)
	if arrival == nil || arrival.Status != mrc20.ArrivalStatusPending {
		t.Fatalf("Stage1: arrival should be Pending, got %v", arrival)
	}
	if arrival.BlockHeight != -1 {
		t.Errorf("Stage1: arrival BlockHeight should be -1 (mempool), got %d", arrival.BlockHeight)
	}

	// No TeleportPendingIn yet (no teleport happened)
	pendingIn, _ := PebbleStore.GetTeleportPendingInByCoord(arrivalPinId)
	if pendingIn != nil {
		t.Errorf("Stage1: TeleportPendingIn should NOT exist yet, but found amount=%s", pendingIn.Amount)
	}

	// Receiver has no balance yet
	receiverBal := queryBalanceLikeAPI(targetChain, receiverAddress, tickId)
	t.Logf("  Stage1-receiver: bal=%s pIn=%s pOut=%s", receiverBal.Balance, receiverBal.PendingIn, receiverBal.PendingOut)

	// ── Stage 2: Teleport enters mempool ──
	t.Log("── Stage 2: Teleport enters mempool")

	teleportContent := mrc20.Mrc20TeleportTransferData{
		Id: tickId, Amount: "100", Coord: arrivalPinId, Chain: targetChain, Type: "teleport",
	}
	contentBody, _ := json.Marshal(teleportContent)

	teleportPin := &pin.PinInscription{
		Id: "pin_" + teleportTxId + "i0", GenesisTransaction: teleportTxId,
		Path: "/ft/mrc20/transfer", ContentBody: contentBody,
		ChainName: sourceChain, Address: senderAddress, GenesisHeight: -1,
		CreateAddress: senderAddress, Output: sourceUtxoTxPoint,
	}

	handleMempoolPin(teleportPin)

	// Source UTXO should be TeleportPending
	sourceUtxo, _ = PebbleStore.GetMrc20UtxoByTxPoint(sourceUtxoTxPoint, false)
	if sourceUtxo == nil || sourceUtxo.Status != mrc20.UtxoStatusTeleportPending {
		status := -999
		if sourceUtxo != nil {
			status = sourceUtxo.Status
		}
		t.Fatalf("Stage2: source UTXO should be TeleportPending(1), got status=%d", status)
	}

	// Sender: bal=0 (UTXO pending), pOut=100
	assertAPIBalance(t, "Stage2-sender", sourceChain, senderAddress, tickId,
		decimal.Zero, decimal.Zero, decimal.NewFromInt(100))

	// PendingTeleport should exist
	pendingTeleport, err := PebbleStore.GetPendingTeleportByCoord(arrivalPinId)
	if err != nil || pendingTeleport == nil {
		t.Fatalf("Stage2: PendingTeleport should exist, err=%v", err)
	}
	if pendingTeleport.Status != 0 {
		t.Errorf("Stage2: PendingTeleport status should be 0 (pending), got %d", pendingTeleport.Status)
	}

	// TeleportPendingIn should now exist (receiver's pending balance)
	pendingIn, _ = PebbleStore.GetTeleportPendingInByCoord(arrivalPinId)
	var actualReceiverAddr string
	if pendingIn != nil {
		actualReceiverAddr = pendingIn.ToAddress
		t.Logf("  TeleportPendingIn created: addr=%s amount=%s", pendingIn.ToAddress, pendingIn.Amount)
	} else {
		t.Error("Stage2: TeleportPendingIn should exist")
	}

	// Receiver: bal=0, pIn=100 (from TeleportPendingIn)
	if actualReceiverAddr != "" {
		assertAPIBalance(t, "Stage2-receiver", targetChain, actualReceiverAddr, tickId,
			decimal.Zero, decimal.NewFromInt(100), decimal.Zero)
	}

	// ── Stage 3: Arrival confirms (出块 height=200) ──
	// When arrival confirms and PendingTeleport exists, teleport should execute
	t.Log("── Stage 3: Arrival confirms (出块)")

	arrivalPinConfirmed := &pin.PinInscription{
		Id: arrivalPinId, GenesisTransaction: arrivalTxId,
		Path: "/ft/mrc20/arrival", ContentBody: arrivalContentBody,
		ChainName: targetChain, Address: receiverAddress, GenesisHeight: 200,
		CreateAddress: receiverAddress,
	}

	arrivalPinList := &[]*pin.PinInscription{arrivalPinConfirmed}
	arrivalTxInList := &[]string{}
	PebbleStore.handleMrc20(targetChain, 200, arrivalPinList, arrivalTxInList)

	// After arrival confirms: processPendingTeleportForArrival runs and executes teleport
	// because arrival.BlockHeight > 0 → isMempool=false

	// Source UTXO should be deleted (executeTeleportTransfer deletes it)
	sourceUtxo, _ = PebbleStore.GetMrc20UtxoByTxPoint(sourceUtxoTxPoint, false)
	if sourceUtxo != nil {
		t.Errorf("Stage3: source UTXO should be deleted after teleport, but still exists with status=%d", sourceUtxo.Status)
	}

	// Sender: fully settled, PendingOut cleared
	assertAPIBalance(t, "Stage3-sender", sourceChain, senderAddress, tickId,
		decimal.Zero, decimal.Zero, decimal.Zero)

	// Target UTXO should be created
	targetUtxoPoint := fmt.Sprintf("%s:%d", arrivalTxId, 0)
	targetUtxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(targetUtxoPoint, false)
	if targetUtxo == nil {
		t.Fatal("Stage3: target UTXO should exist after teleport completion")
	}
	if targetUtxo.Status != mrc20.UtxoStatusAvailable {
		t.Errorf("Stage3: target UTXO should be Available, got status=%d", targetUtxo.Status)
	}
	actualReceiverAddr = targetUtxo.ToAddress

	// Receiver: bal=100, PendingIn cleared
	assertAPIBalance(t, "Stage3-receiver", targetChain, actualReceiverAddr, tickId,
		decimal.NewFromInt(100), decimal.Zero, decimal.Zero)

	// Arrival should be Completed
	arrival, _ = PebbleStore.GetMrc20ArrivalByPinId(arrivalPinId)
	if arrival == nil || arrival.Status != mrc20.ArrivalStatusCompleted {
		t.Errorf("Stage3: arrival should be Completed, got %v", arrival)
	}

	// TeleportPendingIn should be deleted
	pendingIn, _ = PebbleStore.GetTeleportPendingInByCoord(arrivalPinId)
	if pendingIn != nil {
		t.Errorf("Stage3: TeleportPendingIn should be deleted, but still exists: amount=%s", pendingIn.Amount)
	}

	// PendingTeleport should be completed
	pendingTeleport, _ = PebbleStore.GetPendingTeleportByCoord(arrivalPinId)
	if pendingTeleport != nil && pendingTeleport.Status != 1 {
		t.Errorf("Stage3: PendingTeleport status should be 1 (completed), got %d", pendingTeleport.Status)
	}

	// Teleport record should exist
	if !PebbleStore.CheckTeleportExists(arrivalPinId) {
		t.Error("Stage3: Teleport record should exist after completion")
	}

	// ── Stage 4: Teleport confirms (出块 height=201) ──
	// Teleport already completed in Stage 3, this should be a no-op
	t.Log("── Stage 4: Teleport confirms (出块, should be no-op)")

	teleportPinConfirmed := &pin.PinInscription{
		Id: "pin_" + teleportTxId + "i0", GenesisTransaction: teleportTxId,
		Path: "/ft/mrc20/transfer", ContentBody: contentBody,
		ChainName: sourceChain, Address: senderAddress, GenesisHeight: 201,
		CreateAddress: senderAddress, Output: sourceUtxoTxPoint,
	}

	pinList := &[]*pin.PinInscription{teleportPinConfirmed}
	txInList := &[]string{sourceUtxoTxPoint}
	PebbleStore.handleMrc20(sourceChain, 201, pinList, txInList)

	// All balances should remain unchanged
	assertAPIBalance(t, "Stage4-sender", sourceChain, senderAddress, tickId,
		decimal.Zero, decimal.Zero, decimal.Zero)

	assertAPIBalance(t, "Stage4-receiver", targetChain, actualReceiverAddr, tickId,
		decimal.NewFromInt(100), decimal.Zero, decimal.Zero)

	// Target UTXO still available
	targetUtxo, _ = PebbleStore.GetMrc20UtxoByTxPoint(targetUtxoPoint, false)
	if targetUtxo == nil || targetUtxo.Status != mrc20.UtxoStatusAvailable {
		t.Errorf("Stage4: target UTXO should still be Available")
	}

	t.Log("── All stages passed: Mempool arrival-first teleport flow verified")
}

// ========================================
// Test: ArrivalBlockFirst_TeleportMempool
// Reproduces the real-world stuck teleport bug:
// 1. DOGE arrival enters mempool, then confirms in block
// 2. BTC teleport enters mempool AFTER arrival block
// 3. BTC teleport confirms in block
// Without the fix, the teleport would be stuck forever
// because no code path triggers processPendingTeleportForArrival
// ========================================

func TestMRC20TeleportFlow_ArrivalBlockFirst_TeleportMempool(t *testing.T) {
	initTeleportTestConfig()

	tmpDir, err := os.MkdirTemp("", "mrc20_teleport_arrival_block_first_*")
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

	sourceChain := "btc"
	targetChain := "doge"
	senderAddress := "bc1q3h9twrcz7s5mz7q2eu6pneex446tp3v5wmtq68"
	receiverAddress := "DDnnM5GP3o87EDL42PifPMzrtB7xSZhhVg"
	tickId := "teleport_arrival_block_first_tick"
	tickName := "TABF"

	sourceTxId := "0000000000000000000000000000000000000000000000000000000000000a00"
	sourceUtxoTxPoint := sourceTxId + ":0"
	teleportTxId := "0000000000000000000000000000000000000000000000000000000000000b00"
	arrivalTxId := "0000000000000000000000000000000000000000000000000000000000000c00"
	arrivalPinId := "pin_" + arrivalTxId + "i0"

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

	if IndexerAdapter == nil {
		IndexerAdapter = make(map[string]adapter.Indexer)
	}
	IndexerAdapter[sourceChain] = &MockTeleportIndexerAdapter{receiverAddress: receiverAddress, mockTxCache: mockTxCache}
	IndexerAdapter[targetChain] = &MockTeleportIndexerAdapter{receiverAddress: receiverAddress, mockTxCache: mockTxCache}

	if ChainAdapter == nil {
		ChainAdapter = make(map[string]adapter.Chain)
	}
	ChainAdapter[sourceChain] = &MockChainAdapter{mockTxCache: mockTxCache}
	ChainAdapter[targetChain] = &MockChainAdapter{mockTxCache: mockTxCache}

	// ── Stage 0: Setup initial data ──
	t.Log("── Stage 0: Setup initial data")

	PebbleStore.SaveMrc20Tick([]mrc20.Mrc20DeployInfo{{
		Tick: tickName, Mrc20Id: tickId, Chain: sourceChain,
	}})

	PebbleStore.SaveMrc20AccountBalance(&mrc20.Mrc20AccountBalance{
		Address: senderAddress, TickId: tickId, Tick: tickName,
		Balance: decimal.NewFromInt(100), Chain: sourceChain,
		LastUpdateHeight: 100, UtxoCount: 1,
	})

	PebbleStore.SaveMrc20Pin([]mrc20.Mrc20Utxo{{
		Tick: tickName, Mrc20Id: tickId, TxPoint: sourceUtxoTxPoint,
		PointValue: 546, BlockHeight: 100, MrcOption: mrc20.OptionMint,
		ToAddress: senderAddress, AmtChange: decimal.NewFromInt(100),
		Status: mrc20.UtxoStatusAvailable, Chain: sourceChain, Verify: true,
	}})

	assertAPIBalance(t, "Stage0-sender", sourceChain, senderAddress, tickId,
		decimal.NewFromInt(100), decimal.Zero, decimal.Zero)

	// ── Stage 1: DOGE arrival enters mempool ──
	t.Log("── Stage 1: DOGE arrival enters mempool")

	arrivalContent := mrc20.Mrc20ArrivalData{
		AssetOutpoint: sourceUtxoTxPoint, Amount: mrc20.FlexibleString("100"),
		TickId: tickId, LocationIndex: 0,
	}
	arrivalContentBody, _ := json.Marshal(arrivalContent)

	arrivalPinMempool := &pin.PinInscription{
		Id: arrivalPinId, GenesisTransaction: arrivalTxId,
		Path: "/ft/mrc20/arrival", ContentBody: arrivalContentBody,
		ChainName: targetChain, Address: receiverAddress, GenesisHeight: -1,
		CreateAddress: receiverAddress,
	}

	handleMempoolPin(arrivalPinMempool)

	// Arrival created in mempool, no PendingTeleport yet → sender unchanged
	assertAPIBalance(t, "Stage1-sender", sourceChain, senderAddress, tickId,
		decimal.NewFromInt(100), decimal.Zero, decimal.Zero)

	arrival, _ := PebbleStore.GetMrc20ArrivalByPinId(arrivalPinId)
	if arrival == nil || arrival.Status != mrc20.ArrivalStatusPending {
		t.Fatalf("Stage1: Arrival should be Pending, got %v", arrival)
	}
	if arrival.BlockHeight != -1 {
		t.Fatalf("Stage1: Arrival BlockHeight should be -1 (mempool), got %d", arrival.BlockHeight)
	}

	// ── Stage 2: DOGE arrival confirms in block (BEFORE BTC teleport) ──
	t.Log("── Stage 2: DOGE arrival confirms in block")

	arrivalPinConfirmed := &pin.PinInscription{
		Id: arrivalPinId, GenesisTransaction: arrivalTxId,
		Path: "/ft/mrc20/arrival", ContentBody: arrivalContentBody,
		ChainName: targetChain, Address: receiverAddress, GenesisHeight: 6000000,
		CreateAddress: receiverAddress, Timestamp: 1700000000,
	}

	arrivalPinList := &[]*pin.PinInscription{arrivalPinConfirmed}
	arrivalTxInList := &[]string{}
	PebbleStore.handleMrc20(targetChain, 6000000, arrivalPinList, arrivalTxInList)

	// Arrival updated to block height, but no PendingTeleport → nothing to complete
	arrival, _ = PebbleStore.GetMrc20ArrivalByPinId(arrivalPinId)
	if arrival == nil || arrival.BlockHeight != 6000000 {
		t.Fatalf("Stage2: Arrival BlockHeight should be 6000000, got %v", arrival)
	}

	// Sender still unchanged (no teleport processed yet)
	assertAPIBalance(t, "Stage2-sender", sourceChain, senderAddress, tickId,
		decimal.NewFromInt(100), decimal.Zero, decimal.Zero)

	// ── Stage 3: BTC teleport enters mempool (arrival already confirmed!) ──
	t.Log("── Stage 3: BTC teleport enters mempool (arrival already in block)")

	teleportContent := mrc20.Mrc20TeleportTransferData{
		Id: tickId, Amount: "100", Coord: arrivalPinId, Chain: targetChain, Type: "teleport",
	}
	contentBody, _ := json.Marshal(teleportContent)

	teleportPinMempool := &pin.PinInscription{
		Id: "pin_" + teleportTxId + "i0", GenesisTransaction: teleportTxId,
		Path: "/ft/mrc20/transfer", ContentBody: contentBody,
		ChainName: sourceChain, Address: senderAddress, GenesisHeight: -1,
		CreateAddress: senderAddress, Output: sourceUtxoTxPoint,
	}

	handleMempoolPin(teleportPinMempool)

	// sender: bal=0, pOut=100 (UTXO now TeleportPending)
	assertAPIBalance(t, "Stage3-sender", sourceChain, senderAddress, tickId,
		decimal.Zero, decimal.Zero, decimal.NewFromInt(100))

	// PendingTeleport should exist with BlockHeight=-1
	pendingTeleport, err := PebbleStore.GetPendingTeleportByCoord(arrivalPinId)
	if err != nil || pendingTeleport == nil {
		t.Fatalf("Stage3: PendingTeleport should exist, err=%v", err)
	}
	if pendingTeleport.BlockHeight != -1 {
		t.Errorf("Stage3: PendingTeleport.BlockHeight should be -1 (mempool), got %d", pendingTeleport.BlockHeight)
	}

	// TeleportPendingIn should exist (receiver has pendingIn=100)
	pendingIn, _ := PebbleStore.GetTeleportPendingInByCoord(arrivalPinId)
	if pendingIn == nil {
		t.Fatal("Stage3: TeleportPendingIn should exist")
	}

	var actualReceiverAddr string
	if pendingIn != nil {
		actualReceiverAddr = pendingIn.ToAddress
	}
	if actualReceiverAddr != "" {
		assertAPIBalance(t, "Stage3-receiver", targetChain, actualReceiverAddr, tickId,
			decimal.Zero, decimal.NewFromInt(100), decimal.Zero)
	}

	// ── Stage 4: BTC teleport confirms in block ──
	// THIS IS THE KEY STAGE: without the fix, this would skip and leave teleport stuck
	t.Log("── Stage 4: BTC teleport confirms in block (the fix triggers completion)")

	teleportPinConfirmed := &pin.PinInscription{
		Id: "pin_" + teleportTxId + "i0", GenesisTransaction: teleportTxId,
		Path: "/ft/mrc20/transfer", ContentBody: contentBody,
		ChainName: sourceChain, Address: senderAddress, GenesisHeight: 935800,
		CreateAddress: senderAddress, Output: sourceUtxoTxPoint,
	}

	pinList := &[]*pin.PinInscription{teleportPinConfirmed}
	txInList := &[]string{sourceUtxoTxPoint}
	PebbleStore.handleMrc20(sourceChain, 935800, pinList, txInList)

	// sender: fully spent, all pending cleared
	assertAPIBalance(t, "Stage4-sender", sourceChain, senderAddress, tickId,
		decimal.Zero, decimal.Zero, decimal.Zero)

	// receiver: balance=100, all pending cleared
	targetUtxoPoint := fmt.Sprintf("%s:%d", arrivalTxId, 0)
	targetUtxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(targetUtxoPoint, false)
	if targetUtxo != nil {
		actualReceiverAddr = targetUtxo.ToAddress
	}
	assertAPIBalance(t, "Stage4-receiver", targetChain, actualReceiverAddr, tickId,
		decimal.NewFromInt(100), decimal.Zero, decimal.Zero)

	// Verify source UTXO is deleted
	finalUtxo, _ := PebbleStore.GetMrc20UtxoByTxPoint(sourceUtxoTxPoint, false)
	if finalUtxo != nil {
		t.Errorf("Stage4: source UTXO should be deleted, but exists with status=%d", finalUtxo.Status)
	}

	// Verify target UTXO is Available
	if targetUtxo == nil || targetUtxo.Status != mrc20.UtxoStatusAvailable {
		t.Errorf("Stage4: target UTXO should be Available, got %v", targetUtxo)
	}

	// Verify arrival is Completed
	arrival, _ = PebbleStore.GetMrc20ArrivalByPinId(arrivalPinId)
	if arrival == nil || arrival.Status != mrc20.ArrivalStatusCompleted {
		t.Errorf("Stage4: arrival should be Completed, got %v", arrival)
	}

	// Verify TeleportPendingIn is deleted
	pendingIn, _ = PebbleStore.GetTeleportPendingInByCoord(arrivalPinId)
	if pendingIn != nil {
		t.Errorf("Stage4: TeleportPendingIn should be deleted, still exists: amount=%s blockHeight=%d",
			pendingIn.Amount, pendingIn.BlockHeight)
	}

	// Verify PendingTeleport is completed (status=1)
	pendingTeleport, _ = PebbleStore.GetPendingTeleportByCoord(arrivalPinId)
	if pendingTeleport != nil && pendingTeleport.Status != 1 {
		t.Errorf("Stage4: PendingTeleport should be completed (status=1), got status=%d", pendingTeleport.Status)
	}

	// Verify teleport record exists
	if !PebbleStore.CheckTeleportExists(arrivalPinId) {
		t.Error("Stage4: teleport record should exist after completion")
	}

	t.Log("── All stages passed: arrival-block-first + teleport-mempool flow verified")
}

// ========================================
// Mock helpers
// ========================================

func createMockTeleportTxWithInput(t *testing.T, sourceTxId string, sourceVout uint32) *btcutil.Tx {
	msgTx := wire.NewMsgTx(wire.TxVersion)

	prevHashBytes, err := hex.DecodeString(sourceTxId)
	if err != nil {
		t.Fatalf("解析 txid 失败: %v", err)
	}
	for i, j := 0, len(prevHashBytes)-1; i < j; i, j = i+1, j-1 {
		prevHashBytes[i], prevHashBytes[j] = prevHashBytes[j], prevHashBytes[i]
	}
	prevHash, _ := chainhash.NewHash(prevHashBytes)
	outPoint := wire.NewOutPoint(prevHash, sourceVout)
	txIn := wire.NewTxIn(outPoint, nil, nil)
	msgTx.AddTxIn(txIn)

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

func createMockArrivalTxWithReceiver(t *testing.T) *btcutil.Tx {
	msgTx := wire.NewMsgTx(wire.TxVersion)

	hashBytes, _ := hex.DecodeString("ff00000000000000000000000000000000000000000000000000000000000000")
	prevHash, _ := chainhash.NewHash(hashBytes)
	outPoint := wire.NewOutPoint(prevHash, 0)
	txIn := wire.NewTxIn(outPoint, nil, nil)
	msgTx.AddTxIn(txIn)

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

