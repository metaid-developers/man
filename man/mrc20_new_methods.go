package man

import (
	"encoding/json"
	"fmt"
	"log"
	"manindexer/mrc20"
	"manindexer/pin"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"
	"github.com/shopspring/decimal"
)

// splitIndexKey 分割索引键
// 用于处理格式如 {tickId}_{blockHeight}_{timestamp}_{txPoint} 的字符串
// txPoint 可能包含 : 字符，所以不能简单用 _ 分割
func splitIndexKey(s string) []string {
	return strings.Split(s, "_")
}

// ============== AccountBalance 相关方法 ==============

// SaveMrc20AccountBalance 保存账户余额记录
func (pd *PebbleData) SaveMrc20AccountBalance(balance *mrc20.Mrc20AccountBalance) error {
	data, err := sonic.Marshal(balance)
	if err != nil {
		return err
	}

	// 主键: balance_{chain}_{address}_{tickId}
	key := []byte(fmt.Sprintf("balance_%s_%s_%s", balance.Chain, balance.Address, balance.TickId))
	return pd.Database.MrcDb.Set(key, data, pebble.Sync)
}

// GetMrc20AccountBalance 获取账户余额
func (pd *PebbleData) GetMrc20AccountBalance(chain, address, tickId string) (*mrc20.Mrc20AccountBalance, error) {
	key := []byte(fmt.Sprintf("balance_%s_%s_%s", chain, address, tickId))
	data, closer, err := pd.Database.MrcDb.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	var balance mrc20.Mrc20AccountBalance
	if err := sonic.Unmarshal(data, &balance); err != nil {
		return nil, err
	}
	return &balance, nil
}

// UpdateMrc20AccountBalance 更新账户余额 (原子操作)
// 参数:
//
//	deltaBalance: 余额变化量 (可为负)
//	deltaPendingOut: PendingOut 变化量
//	deltaPendingIn: PendingIn 变化量
//	deltaUtxoCount: UTXO 数量变化
func (pd *PebbleData) UpdateMrc20AccountBalance(
	chain, address, tickId, tick string,
	deltaBalance, deltaPendingOut, deltaPendingIn decimal.Decimal,
	deltaUtxoCount int,
	txId string, blockHeight, timestamp int64,
) error {
	// 获取现有余额或创建新记录
	balance, err := pd.GetMrc20AccountBalance(chain, address, tickId)
	if err != nil {
		if err == pebble.ErrNotFound {
			// 创建新记录
			balance = &mrc20.Mrc20AccountBalance{
				Address:          address,
				TickId:           tickId,
				Tick:             tick,
				Balance:          decimal.Zero,
				PendingOut:       decimal.Zero,
				PendingIn:        decimal.Zero,
				Chain:            chain,
				LastUpdateTx:     "",
				LastUpdateHeight: 0,
				LastUpdateTime:   0,
				UtxoCount:        0,
			}
		} else {
			return err
		}
	}

	// 更新余额
	balance.Balance = balance.Balance.Add(deltaBalance)
	balance.PendingOut = balance.PendingOut.Add(deltaPendingOut)
	balance.PendingIn = balance.PendingIn.Add(deltaPendingIn)
	balance.UtxoCount += deltaUtxoCount
	balance.LastUpdateTx = txId
	balance.LastUpdateHeight = blockHeight
	balance.LastUpdateTime = timestamp

	// 保存
	return pd.SaveMrc20AccountBalance(balance)
}

// GetMrc20AccountAllBalances 获取地址的所有 tick 余额
func (pd *PebbleData) GetMrc20AccountAllBalances(chain, address string) ([]*mrc20.Mrc20AccountBalance, error) {
	prefix := []byte(fmt.Sprintf("balance_%s_%s_", chain, address))
	iter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var balances []*mrc20.Mrc20AccountBalance
	for iter.First(); iter.Valid(); iter.Next() {
		var balance mrc20.Mrc20AccountBalance
		if err := sonic.Unmarshal(iter.Value(), &balance); err != nil {
			log.Printf("Failed to unmarshal balance: %v", err)
			continue
		}
		balances = append(balances, &balance)
	}

	return balances, nil
}

// ============== Transaction 流水表相关方法 ==============

// SaveMrc20Transaction 保存交易流水记录
// Key 设计（按地址视角记录每个 UTXO 的流入/流出）：
//   - 主键: tx_{txPoint}  (txPoint 是全局唯一的，包含 txid)
//   - 按 tick 查: tx_tick_{tickId}_{blockHeight}_{timestamp}_{txPoint}
//   - 按地址+tick 查: tx_addr_{address}_{tickId}_{blockHeight}_{timestamp}_{txPoint}
//
// 注意：
//   - Direction 字段表示流水方向: "in" (收入) / "out" (支出)
//   - Address 字段表示从谁的视角记录这条流水
//   - chain 信息存储在记录的 Chain 字段中
func (pd *PebbleData) SaveMrc20Transaction(tx *mrc20.Mrc20Transaction) error {
	data, err := sonic.Marshal(tx)
	if err != nil {
		return err
	}

	// 使用 TxPoint 作为主键的一部分
	txPointForKey := tx.TxPoint

	batch := pd.Database.MrcDb.NewBatch()
	defer batch.Close()

	// 主键: tx_{txPoint} (不含 chain，txPoint 本身是唯一的)
	key := []byte(fmt.Sprintf("tx_%s", txPointForKey))
	batch.Set(key, data, pebble.Sync)

	// 按 tick 查历史索引: tx_tick_{tickId}_{blockHeight}_{timestamp}_{txPoint}
	// blockHeight 采用特殊编码：-1 使用 "999999999999"（倒序查询时排在最前），其他用 12 位正数
	blockHeightStr := ""
	if tx.BlockHeight == -1 {
		blockHeightStr = "999999999999" // mempool 记录，倒序时排最前
	} else {
		blockHeightStr = fmt.Sprintf("%012d", tx.BlockHeight)
	}
	tickKey := []byte(fmt.Sprintf("tx_tick_%s_%s_%012d_%s", tx.TickId, blockHeightStr, tx.Timestamp, txPointForKey))
	batch.Set(tickKey, key, pebble.Sync)

	// 按地址查索引: tx_addr_{address}_{tickId}_{blockHeight}_{timestamp}_{txPoint}
	// 使用 Address 字段（流水关联的地址）
	if tx.Address != "" {
		addrKey := []byte(fmt.Sprintf("tx_addr_%s_%s_%s_%012d_%s", tx.Address, tx.TickId, blockHeightStr, tx.Timestamp, txPointForKey))
		batch.Set(addrKey, key, pebble.Sync)
	}

	return batch.Commit(pebble.Sync)
}

// UpdateMrc20TransactionBlockHeight 更新流水记录的区块高度
// 用于 mempool 交易出块后更新 BlockHeight（从 -1 更新为实际区块高度）
func (pd *PebbleData) UpdateMrc20TransactionBlockHeight(txId string, blockHeight int64) error {
	// 遍历查找该 txId 相关的所有流水记录
	// 主键格式: tx_{txPoint}，其中 txPoint = txId:vout 或 txId:vout_out
	prefix := []byte(fmt.Sprintf("tx_%s:", txId))

	iter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	batch := pd.Database.MrcDb.NewBatch()
	defer batch.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		var tx mrc20.Mrc20Transaction
		if err := sonic.Unmarshal(iter.Value(), &tx); err != nil {
			continue
		}

		// 只更新 BlockHeight = -1 的记录（mempool 阶段创建的）
		if tx.BlockHeight != -1 {
			continue
		}

		oldBlockHeight := tx.BlockHeight
		tx.BlockHeight = blockHeight

		// 序列化新数据
		newData, err := sonic.Marshal(&tx)
		if err != nil {
			log.Printf("[ERROR] Marshal transaction failed: %v", err)
			continue
		}

		// 更新主记录
		batch.Set(key, newData, pebble.Sync)

		// 删除旧的索引（带旧的 blockHeight=-1）
		// 注意：blockHeight=-1 使用 "999999999999" 表示 mempool
		oldBlockHeightStr := ""
		if oldBlockHeight == -1 {
			oldBlockHeightStr = "999999999999" // mempool 阶段
		} else {
			oldBlockHeightStr = fmt.Sprintf("%012d", oldBlockHeight)
		}
		oldTickKey := []byte(fmt.Sprintf("tx_tick_%s_%s_%012d_%s", tx.TickId, oldBlockHeightStr, tx.Timestamp, tx.TxPoint))
		batch.Delete(oldTickKey, pebble.Sync)

		if tx.Address != "" {
			oldAddrKey := []byte(fmt.Sprintf("tx_addr_%s_%s_%s_%012d_%s", tx.Address, tx.TickId, oldBlockHeightStr, tx.Timestamp, tx.TxPoint))
			batch.Delete(oldAddrKey, pebble.Sync)
		}

		// 创建新的索引（带新的 blockHeight）
		newBlockHeightStr := fmt.Sprintf("%012d", blockHeight)
		newTickKey := []byte(fmt.Sprintf("tx_tick_%s_%s_%012d_%s", tx.TickId, newBlockHeightStr, tx.Timestamp, tx.TxPoint))
		batch.Set(newTickKey, key, pebble.Sync)

		if tx.Address != "" {
			newAddrKey := []byte(fmt.Sprintf("tx_addr_%s_%s_%s_%012d_%s", tx.Address, tx.TickId, newBlockHeightStr, tx.Timestamp, tx.TxPoint))
			batch.Set(newAddrKey, key, pebble.Sync)
		}

		//log.Printf("[MRC20] Updated transaction %s blockHeight: %d -> %d", tx.TxPoint, oldBlockHeight, blockHeight)
	}

	return batch.Commit(pebble.Sync)
}

// GetMrc20TransactionHistory 查询交易历史
// 参数:
//
//	address: 地址 (可选，空表示查询 tick 的所有交易)
//	tickId: tick ID (可选，空表示所有 tick)
//	limit: 分页大小
//	offset: 分页偏移
//
// 注意：不区分链，所有链的交易历史在一起查询，Chain 信息在返回的记录中
func (pd *PebbleData) GetMrc20TransactionHistory(
	address, tickId string,
	limit, offset int,
) ([]*mrc20.Mrc20Transaction, int64, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}

	var transactions []*mrc20.Mrc20Transaction
	var total int64

	// 如果 address 为空，查询该 tick 的所有交易（使用 tx_tick 索引）
	if address == "" {
		allPrefix := []byte(fmt.Sprintf("tx_tick_%s_", tickId))

		// 第一遍：统计总数
		countIter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
			LowerBound: allPrefix,
			UpperBound: append(allPrefix, 0xff),
		})
		if err != nil {
			return nil, 0, err
		}
		for countIter.First(); countIter.Valid(); countIter.Next() {
			total++
		}
		countIter.Close()

		// 第二遍：分页读取（从后往前，按时间降序）
		dataIter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
			LowerBound: allPrefix,
			UpperBound: append(allPrefix, 0xff),
		})
		if err != nil {
			return nil, 0, err
		}
		defer dataIter.Close()

		// 从最后一条开始，跳过 offset 条
		skipped := 0
		for dataIter.Last(); dataIter.Valid() && skipped < offset; dataIter.Prev() {
			skipped++
		}

		// 读取 limit 条
		fetched := 0
		for dataIter.Valid() && fetched < limit {
			txKey := dataIter.Value()
			data, closer, err := pd.Database.MrcDb.Get(txKey)
			if err == nil {
				var tx mrc20.Mrc20Transaction
				if err := sonic.Unmarshal(data, &tx); err == nil {
					transactions = append(transactions, &tx)
					fetched++
				}
				closer.Close()
			}
			dataIter.Prev()
		}
	} else if tickId == "" {
		// tickId 为空：查询该地址所有 tick 的交易 (使用 tx_addr_{address}_ 前缀)
		addrPrefix := []byte(fmt.Sprintf("tx_addr_%s_", address))

		// 第一遍：统计总数
		countIter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
			LowerBound: addrPrefix,
			UpperBound: append(addrPrefix, 0xff),
		})
		if err != nil {
			return nil, 0, err
		}
		for countIter.First(); countIter.Valid(); countIter.Next() {
			total++
		}
		countIter.Close()

		// 第二遍：分页读取（从后往前，按时间降序）
		dataIter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
			LowerBound: addrPrefix,
			UpperBound: append(addrPrefix, 0xff),
		})
		if err != nil {
			return nil, 0, err
		}
		defer dataIter.Close()

		skipped := 0
		for dataIter.Last(); dataIter.Valid() && skipped < offset; dataIter.Prev() {
			skipped++
		}

		fetched := 0
		for dataIter.Valid() && fetched < limit {
			txKey := dataIter.Value()
			data, closer, err := pd.Database.MrcDb.Get(txKey)
			if err == nil {
				var tx mrc20.Mrc20Transaction
				if err := sonic.Unmarshal(data, &tx); err == nil {
					transactions = append(transactions, &tx)
					fetched++
				}
				closer.Close()
			}
			dataIter.Prev()
		}
	} else {
		// 查询指定地址+tickId 的交易：使用 tx_addr_{address}_{tickId}_ 前缀
		addrTickPrefix := []byte(fmt.Sprintf("tx_addr_%s_%s_", address, tickId))

		// 第一遍：统计总数
		countIter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
			LowerBound: addrTickPrefix,
			UpperBound: append(addrTickPrefix, 0xff),
		})
		if err != nil {
			return nil, 0, err
		}
		for countIter.First(); countIter.Valid(); countIter.Next() {
			total++
		}
		countIter.Close()

		// 第二遍：分页读取（从后往前，按时间降序）
		dataIter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
			LowerBound: addrTickPrefix,
			UpperBound: append(addrTickPrefix, 0xff),
		})
		if err != nil {
			return nil, 0, err
		}
		defer dataIter.Close()

		skipped := 0
		for dataIter.Last(); dataIter.Valid() && skipped < offset; dataIter.Prev() {
			skipped++
		}

		fetched := 0
		for dataIter.Valid() && fetched < limit {
			txKey := dataIter.Value()
			data, closer, err := pd.Database.MrcDb.Get(txKey)
			if err == nil {
				var tx mrc20.Mrc20Transaction
				if err := sonic.Unmarshal(data, &tx); err == nil {
					transactions = append(transactions, &tx)
					fetched++
				}
				closer.Close()
			}
			dataIter.Prev()
		}
	}

	return transactions, total, nil
}

// ============== 辅助方法：基于 UTXO 计算余额 (用于验证) ==============

// RecalculateAccountBalance 重新计算账户余额 (从 UTXO 聚合)
func (pd *PebbleData) RecalculateAccountBalance(chain, address, tickId string) (*mrc20.Mrc20AccountBalance, error) {
	balance := decimal.Zero
	pendingOut := decimal.Zero
	utxoCount := 0

	// 扫描地址的所有 UTXO
	prefix := []byte(fmt.Sprintf("mrc20_in_%s_%s_", address, tickId))
	iter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var utxo mrc20.Mrc20Utxo
		if err := sonic.Unmarshal(iter.Value(), &utxo); err != nil {
			continue
		}

		if utxo.Status == mrc20.UtxoStatusAvailable {
			balance = balance.Add(utxo.AmtChange)
			utxoCount++
		} else if utxo.Status == mrc20.UtxoStatusTeleportPending {
			// teleport pending 状态
			pendingOut = pendingOut.Add(utxo.AmtChange)
		} else if utxo.Status == mrc20.UtxoStatusTransferPending {
			// 普通 transfer/native_transfer 的 mempool pending 状态
			pendingOut = pendingOut.Add(utxo.AmtChange)
		}
	}

	// 计算 PendingIn (从 TeleportPendingIn 表)
	pendingIn := decimal.Zero
	pendingInList, err := pd.GetTeleportPendingInByAddress(address)
	if err == nil {
		for _, p := range pendingInList {
			if p.TickId == tickId && p.Chain == chain {
				pendingIn = pendingIn.Add(p.Amount)
			}
		}
	}

	// 计算 TransferPendingIn (普通转账接收方的待入账余额)
	transferPendingInList, err := pd.GetTransferPendingInByAddress(address)
	if err == nil {
		for _, p := range transferPendingInList {
			if p.TickId == tickId && p.Chain == chain {
				pendingIn = pendingIn.Add(p.Amount)
			}
		}
	}

	tick, _ := pd.GetMrc20TickInfo(tickId, "")
	tickName := ""
	if tick.Mrc20Id != "" {
		tickName = tick.Tick
	}

	return &mrc20.Mrc20AccountBalance{
		Address:    address,
		TickId:     tickId,
		Tick:       tickName,
		Balance:    balance,
		PendingOut: pendingOut,
		PendingIn:  pendingIn,
		Chain:      chain,
		UtxoCount:  utxoCount,
	}, nil
}

// ============== 辅助方法：UTXO BlockHeight 更新 ==============

// UpdateUtxosBlockHeight 更新 UTXO 的 BlockHeight（从 -1 更新为实际区块高度）
// 用于处理 mempool 阶段已创建、出块时需要确认的 UTXO
func (pd *PebbleData) UpdateUtxosBlockHeight(utxos []*mrc20.Mrc20Utxo, blockHeight int64) error {
	if len(utxos) == 0 {
		return nil
	}

	// 筛选需要更新的 UTXO（BlockHeight=-1 且 Status=0）
	var toUpdate []mrc20.Mrc20Utxo
	for _, utxo := range utxos {
		if utxo.BlockHeight == -1 && utxo.Status == mrc20.UtxoStatusAvailable {
			updated := *utxo
			updated.BlockHeight = blockHeight
			toUpdate = append(toUpdate, updated)
		}
	}

	if len(toUpdate) == 0 {
		return nil
	}

	// 重新保存更新后的 UTXO
	return pd.SaveMrc20Pin(toUpdate)
}

// ============== 辅助方法：UTXO 删除 ==============

// DeleteMrc20Utxo 删除 UTXO 记录 (spent 后调用)
// 删除所有相关索引：主记录、mrc20_in、available_utxo
func (pd *PebbleData) DeleteMrc20Utxo(txPoint, address, tickId string) error {
	// 先读取 UTXO 获取 chain 和 FromAddress 信息
	utxo, err := pd.GetMrc20UtxoByTxPoint(txPoint, false)
	chain := ""
	if err == nil && utxo != nil {
		chain = utxo.Chain
	}

	batch := pd.Database.MrcDb.NewBatch()
	defer batch.Close()

	// 删除主键
	mainKey := []byte(fmt.Sprintf("mrc20_utxo_%s", txPoint))
	batch.Delete(mainKey, pebble.Sync)

	// 删除 mrc20_in 索引（ToAddress）
	inKey := []byte(fmt.Sprintf("mrc20_in_%s_%s_%s", address, tickId, txPoint))
	batch.Delete(inKey, pebble.Sync)

	// 删除 FromAddress 的索引（如果存在且与 ToAddress 不同）
	if utxo != nil && utxo.FromAddress != "" && utxo.FromAddress != address {
		outKey := []byte(fmt.Sprintf("mrc20_in_%s_%s_%s", utxo.FromAddress, tickId, txPoint))
		batch.Delete(outKey, pebble.Sync)
	}

	// 删除 available_utxo 索引
	if chain != "" {
		availableKey := []byte(fmt.Sprintf("available_utxo_%s_%s_%s_%s", chain, address, tickId, txPoint))
		batch.Delete(availableKey, pebble.Sync)
	}

	//log.Printf("[MRC20] DeleteMrc20Utxo: txPoint=%s, address=%s, tickId=%s", txPoint, address, tickId)
	return batch.Commit(pebble.Sync)
}

// ============== 批量操作辅助 ==============

// BatchUpdateMrc20State 批量更新 MRC20 状态 (UTXO + Balance + Transaction)
// 用于保证原子性
type Mrc20StateUpdate struct {
	UtxosToSave   []*mrc20.Mrc20Utxo
	UtxosToDelete []struct {
		TxPoint string
		Address string
		TickId  string
	}
	BalanceUpdates []*mrc20.Mrc20AccountBalance
	Transactions   []*mrc20.Mrc20Transaction
}

func (pd *PebbleData) BatchUpdateMrc20State(update *Mrc20StateUpdate) error {
	batch := pd.Database.MrcDb.NewBatch()
	defer batch.Close()

	// 1. 保存 UTXO
	for _, utxo := range update.UtxosToSave {
		data, err := sonic.Marshal(utxo)
		if err != nil {
			return err
		}
		key := []byte(fmt.Sprintf("mrc20_utxo_%s", utxo.TxPoint))
		batch.Set(key, data, pebble.Sync)

		// ToAddress 索引
		if utxo.ToAddress != "" {
			inKey := []byte(fmt.Sprintf("mrc20_in_%s_%s_%s", utxo.ToAddress, utxo.Mrc20Id, utxo.TxPoint))
			batch.Set(inKey, data, pebble.Sync)

			// available_utxo 索引（只有 status=0 的 UTXO）
			if utxo.Status == mrc20.UtxoStatusAvailable {
				availableKey := []byte(fmt.Sprintf("available_utxo_%s_%s_%s_%s", utxo.Chain, utxo.ToAddress, utxo.Mrc20Id, utxo.TxPoint))
				batch.Set(availableKey, data, pebble.Sync)
			}
		}

		// FromAddress 索引（当发送方存在且与接收方不同时）
		if utxo.FromAddress != "" && utxo.FromAddress != utxo.ToAddress {
			outKey := []byte(fmt.Sprintf("mrc20_in_%s_%s_%s", utxo.FromAddress, utxo.Mrc20Id, utxo.TxPoint))
			batch.Set(outKey, data, pebble.Sync)
		}
	}

	// 2. 删除 UTXO
	for _, del := range update.UtxosToDelete {
		mainKey := []byte(fmt.Sprintf("mrc20_utxo_%s", del.TxPoint))
		batch.Delete(mainKey, pebble.Sync)
		inKey := []byte(fmt.Sprintf("mrc20_in_%s_%s_%s", del.Address, del.TickId, del.TxPoint))
		batch.Delete(inKey, pebble.Sync)
		// 注意：需要从 UTXO 获取 chain 信息来删除 available_utxo 索引
		// 这里简化处理，在实际使用时应该提供 chain 信息
	}

	// 3. 更新余额
	for _, balance := range update.BalanceUpdates {
		data, err := sonic.Marshal(balance)
		if err != nil {
			return err
		}
		key := []byte(fmt.Sprintf("balance_%s_%s_%s", balance.Chain, balance.Address, balance.TickId))
		batch.Set(key, data, pebble.Sync)
	}

	// 4. 保存交易流水
	for _, tx := range update.Transactions {
		data, err := sonic.Marshal(tx)
		if err != nil {
			return err
		}

		// 使用 TxPoint 作为主键
		txPointForKey := tx.TxPoint

		// 主键: tx_{txPoint}
		key := []byte(fmt.Sprintf("tx_%s", txPointForKey))
		batch.Set(key, data, pebble.Sync)

		// 按 tick 查历史索引: tx_tick_{tickId}_{blockHeight}_{timestamp}_{txPoint}
		tickKey := []byte(fmt.Sprintf("tx_tick_%s_%012d_%012d_%s", tx.TickId, tx.BlockHeight, tx.Timestamp, txPointForKey))
		batch.Set(tickKey, key, pebble.Sync)

		// 按地址查索引: tx_addr_{address}_{tickId}_{blockHeight}_{timestamp}_{txPoint}
		if tx.Address != "" {
			addrKey := []byte(fmt.Sprintf("tx_addr_%s_%s_%012d_%012d_%s", tx.Address, tx.TickId, tx.BlockHeight, tx.Timestamp, txPointForKey))
			batch.Set(addrKey, key, pebble.Sync)
		}
	}

	return batch.Commit(pebble.Sync)
}

// ============== 业务辅助函数 ==============

// ProcessMintSuccess 处理 Mint 成功后的状态更新
func (pd *PebbleData) ProcessMintSuccess(utxo *mrc20.Mrc20Utxo) error {
	// 1. 更新账户余额
	err := pd.UpdateMrc20AccountBalance(
		utxo.Chain,
		utxo.ToAddress,
		utxo.Mrc20Id,
		utxo.Tick,
		utxo.AmtChange, // deltaBalance
		decimal.Zero,   // deltaPendingOut
		decimal.Zero,   // deltaPendingIn
		1,              // deltaUtxoCount
		utxo.OperationTx,
		utxo.BlockHeight,
		utxo.Timestamp,
	)
	if err != nil {
		log.Printf("UpdateMrc20AccountBalance error: %v", err)
		return err
	}

	// 2. 写入交易流水 (mint 只有一条收入记录)
	createdUtxos, _ := json.Marshal([]string{utxo.TxPoint})
	tx := &mrc20.Mrc20Transaction{
		TxId:         utxo.OperationTx,
		TxPoint:      utxo.TxPoint,
		PinId:        utxo.PinId,
		TickId:       utxo.Mrc20Id,
		Tick:         utxo.Tick,
		TxType:       "mint",
		Direction:    "in", // mint 是收入
		Address:      utxo.ToAddress,
		FromAddress:  "",
		ToAddress:    utxo.ToAddress,
		Amount:       utxo.AmtChange,
		IsChange:     false,
		Chain:        utxo.Chain,
		BlockHeight:  utxo.BlockHeight,
		Timestamp:    utxo.Timestamp,
		CreatedUtxos: string(createdUtxos),
		Msg:          utxo.Msg,
		Status:       1,
	}
	return pd.SaveMrc20Transaction(tx)
}

// ProcessMintFailure 处理 Mint 失败后的记录保存
// 仅写入失败的流水记录，不更新余额
func (pd *PebbleData) ProcessMintFailure(utxo *mrc20.Mrc20Utxo) error {
	// 从 TxPoint 提取 TxId (格式: txid:vout)
	txId := utxo.OperationTx
	if txId == "" && strings.Contains(utxo.TxPoint, ":") {
		txId = strings.Split(utxo.TxPoint, ":")[0]
	}

	// 为失败的 mint 创建交易流水记录
	tx := &mrc20.Mrc20Transaction{
		TxId:        txId,
		TxPoint:     utxo.TxPoint,
		TxIndex:     0, // UTXO 没有 TxIndex 字段，使用默认值
		PinId:       utxo.PinId,
		TickId:      utxo.Mrc20Id,
		Tick:        utxo.Tick,
		TxType:      "mint",
		Direction:   "in", // mint 总是收入方向
		Address:     utxo.ToAddress,
		ToAddress:   utxo.ToAddress,
		Amount:      utxo.AmtChange,
		IsChange:    false,
		Chain:       utxo.Chain,
		BlockHeight: utxo.BlockHeight,
		Timestamp:   utxo.Timestamp,
		Msg:         utxo.Msg,
		Status:      -1, // 失败状态
	}
	return pd.SaveMrc20Transaction(tx)
}

// ProcessTransferFailure 处理 Transfer 失败后的记录保存
// 仅写入失败的流水记录，不更新余额
func (pd *PebbleData) ProcessTransferFailure(utxos []*mrc20.Mrc20Utxo) error {
	for _, utxo := range utxos {
		// 从 TxPoint 提取 TxId (格式: txid:vout)
		txId := utxo.OperationTx
		if txId == "" && strings.Contains(utxo.TxPoint, ":") {
			txId = strings.Split(utxo.TxPoint, ":")[0]
		}

		// 为失败的 transfer 创建交易流水记录
		tx := &mrc20.Mrc20Transaction{
			TxId:        txId,
			TxPoint:     utxo.TxPoint,
			TxIndex:     0, // UTXO 没有 TxIndex 字段，使用默认值
			PinId:       utxo.PinId,
			TickId:      utxo.Mrc20Id,
			Tick:        utxo.Tick,
			TxType:      "transfer",
			Direction:   "in", // 失败的转账通常归还给第一个输出地址
			Address:     utxo.ToAddress,
			FromAddress: utxo.FromAddress,
			ToAddress:   utxo.ToAddress,
			Amount:      utxo.AmtChange,
			IsChange:    false,
			Chain:       utxo.Chain,
			BlockHeight: utxo.BlockHeight,
			Timestamp:   utxo.Timestamp,
			Msg:         utxo.Msg,
			Status:      -1, // 失败状态
		}
		if err := pd.SaveMrc20Transaction(tx); err != nil {
			log.Printf("ProcessTransferFailure: SaveMrc20Transaction error: %v", err)
			return err
		}
	}
	return nil
}

// ProcessTransferSuccess 处理 Transfer 成功后的状态更新
// 包括：标记 spent UTXO、更新余额、写入流水
func (pd *PebbleData) ProcessTransferSuccess(
	pinNode *pin.PinInscription,
	spentUtxos []*mrc20.Mrc20Utxo,
	createdUtxos []*mrc20.Mrc20Utxo,
) error {
	// 收集需要更新的余额
	balanceUpdates := make(map[string]*mrc20.Mrc20AccountBalance) // key: chain_address_tickId

	// 1. 标记发送方的 spent UTXO 并更新余额
	for _, utxo := range spentUtxos {
		// 标记 UTXO 为已消费（不删除，用于回滚）
		err := pd.MarkUtxoAsSpent(utxo.TxPoint, utxo.ToAddress, utxo.Mrc20Id, utxo.Chain, pinNode.GenesisHeight)
		if err != nil {
			log.Printf("[ERROR] MarkUtxoAsSpent failed for %s: %v", utxo.TxPoint, err)
		}

		// 更新发送方余额
		key := fmt.Sprintf("%s_%s_%s", utxo.Chain, utxo.ToAddress, utxo.Mrc20Id)
		if _, exists := balanceUpdates[key]; !exists {
			balance, _ := pd.GetMrc20AccountBalance(utxo.Chain, utxo.ToAddress, utxo.Mrc20Id)
			if balance == nil {
				balance = &mrc20.Mrc20AccountBalance{
					Address: utxo.ToAddress,
					TickId:  utxo.Mrc20Id,
					Tick:    utxo.Tick,
					Chain:   utxo.Chain,
				}
			}
			balanceUpdates[key] = balance
		}
		balanceUpdates[key].Balance = balanceUpdates[key].Balance.Sub(utxo.AmtChange)
		balanceUpdates[key].UtxoCount--
		balanceUpdates[key].LastUpdateTx = pinNode.GenesisTransaction
		balanceUpdates[key].LastUpdateHeight = pinNode.GenesisHeight
		balanceUpdates[key].LastUpdateTime = pinNode.Timestamp
	}

	// 2. 处理接收方余额 (增加)
	for _, utxo := range createdUtxos {
		key := fmt.Sprintf("%s_%s_%s", utxo.Chain, utxo.ToAddress, utxo.Mrc20Id)
		if _, exists := balanceUpdates[key]; !exists {
			balance, _ := pd.GetMrc20AccountBalance(utxo.Chain, utxo.ToAddress, utxo.Mrc20Id)
			if balance == nil {
				balance = &mrc20.Mrc20AccountBalance{
					Address: utxo.ToAddress,
					TickId:  utxo.Mrc20Id,
					Tick:    utxo.Tick,
					Chain:   utxo.Chain,
				}
			}
			balanceUpdates[key] = balance
		}
		balanceUpdates[key].Balance = balanceUpdates[key].Balance.Add(utxo.AmtChange)
		balanceUpdates[key].UtxoCount++
		balanceUpdates[key].LastUpdateTx = pinNode.GenesisTransaction
		balanceUpdates[key].LastUpdateHeight = pinNode.GenesisHeight
		balanceUpdates[key].LastUpdateTime = pinNode.Timestamp
	}

	// 3. 保存余额
	for _, balance := range balanceUpdates {
		if err := pd.SaveMrc20AccountBalance(balance); err != nil {
			log.Printf("SaveMrc20AccountBalance error: %v", err)
			return err
		}
	}

	// 4. 写入 UTXO 流水 - 每个地址每个 UTXO 变动一条记录
	// 构建 spent 和 created 列表
	spentList := []string{}
	createdList := []string{}
	for _, u := range spentUtxos {
		spentList = append(spentList, u.TxPoint)
	}
	for _, u := range createdUtxos {
		createdList = append(createdList, u.TxPoint)
	}
	spentJson, _ := json.Marshal(spentList)
	createdJson, _ := json.Marshal(createdList)

	// 获取发送方地址
	fromAddress := ""
	if len(spentUtxos) > 0 {
		fromAddress = spentUtxos[0].ToAddress // spent UTXO 的 ToAddress 就是发送方
	}

	// 收集所有非找零的接收方（真正的转账目标）
	var realRecipients []string
	for _, utxo := range createdUtxos {
		if utxo.ToAddress != fromAddress {
			realRecipients = append(realRecipients, utxo.ToAddress)
		}
	}
	// 拼接接收方地址（用于 Out 记录显示）
	toAddressForOut := ""
	if len(realRecipients) == 1 {
		toAddressForOut = realRecipients[0]
	} else if len(realRecipients) > 1 {
		toAddressForOut = realRecipients[0] + fmt.Sprintf(" (+%d)", len(realRecipients)-1)
	}

	// 4.1 为发送方记录支出流水 (每个 spent UTXO 一条 out 记录)
	for _, utxo := range spentUtxos {
		tx := &mrc20.Mrc20Transaction{
			TxId:         pinNode.GenesisTransaction,
			TxPoint:      utxo.TxPoint + "_out", // 使用 _out 后缀区分同一 UTXO 的支出记录
			PinId:        pinNode.Id,
			TickId:       utxo.Mrc20Id,
			Tick:         utxo.Tick,
			TxType:       "transfer",
			Direction:    "out", // 支出
			Address:      utxo.ToAddress,
			FromAddress:  utxo.ToAddress,
			ToAddress:    toAddressForOut, // 显示真正的接收方
			Amount:       utxo.AmtChange,
			IsChange:     false,
			Chain:        pinNode.ChainName,
			BlockHeight:  pinNode.GenesisHeight,
			Timestamp:    pinNode.Timestamp,
			SpentUtxos:   string(spentJson),
			CreatedUtxos: string(createdJson),
			Msg:          utxo.Msg,
			Status:       1,
		}
		if err := pd.SaveMrc20Transaction(tx); err != nil {
			log.Printf("SaveMrc20Transaction (out) error: %v", err)
			return err
		}
	}

	// 4.2 为接收方记录收入流水 (每个 created UTXO 一条 in 记录)
	for _, utxo := range createdUtxos {
		// 判断是否是找零 (接收方地址 == 发送方地址)
		isChange := utxo.ToAddress == fromAddress

		tx := &mrc20.Mrc20Transaction{
			TxId:         pinNode.GenesisTransaction,
			TxPoint:      utxo.TxPoint,
			PinId:        pinNode.Id,
			TickId:       utxo.Mrc20Id,
			Tick:         utxo.Tick,
			TxType:       utxo.MrcOption,
			Direction:    "in", // 收入
			Address:      utxo.ToAddress,
			FromAddress:  fromAddress,
			ToAddress:    utxo.ToAddress,
			Amount:       utxo.AmtChange,
			IsChange:     isChange,
			Chain:        pinNode.ChainName,
			BlockHeight:  pinNode.GenesisHeight,
			Timestamp:    pinNode.Timestamp,
			SpentUtxos:   string(spentJson),
			CreatedUtxos: string(createdJson),
			Msg:          utxo.Msg,
			Status:       utxo.Status,
		}
		if err := pd.SaveMrc20Transaction(tx); err != nil {
			log.Printf("SaveMrc20Transaction (in) error: %v", err)
			return err
		}

		// 删除 TransferPendingIn 记录（如果存在，表示 mempool 阶段创建的）
		if !isChange {
			if err := pd.DeleteTransferPendingIn(utxo.TxPoint, utxo.ToAddress); err != nil {
				// 不阻断主流程，只记录日志
				log.Printf("DeleteTransferPendingIn warning: %v", err)
			}
		}
	}

	return nil
}

// SaveMempoolNativeTransferTransaction mempool 阶段保存 native transfer 流水记录（只写流水，不更新余额）
// BlockHeight 设置为 -1，出块后由 UpdateMrc20TransactionBlockHeight 更新
func (pd *PebbleData) SaveMempoolNativeTransferTransaction(
	txId, chainName string,
	spentUtxos []*mrc20.Mrc20Utxo,
	createdUtxos []*mrc20.Mrc20Utxo,
) error {
	// 获取时间戳
	var timestamp int64
	if len(createdUtxos) > 0 {
		timestamp = createdUtxos[0].Timestamp
	}

	// 构建 spent 和 created 列表
	spentList := []string{}
	createdList := []string{}
	for _, u := range spentUtxos {
		spentList = append(spentList, u.TxPoint)
	}
	for _, u := range createdUtxos {
		createdList = append(createdList, u.TxPoint)
	}
	spentJson, _ := json.Marshal(spentList)
	createdJson, _ := json.Marshal(createdList)

	// 获取发送方地址
	fromAddress := ""
	if len(spentUtxos) > 0 {
		fromAddress = spentUtxos[0].ToAddress
	}

	// 收集所有非找零的接收方（真正的转账目标）
	var realRecipients []string
	for _, utxo := range createdUtxos {
		if utxo.ToAddress != fromAddress {
			realRecipients = append(realRecipients, utxo.ToAddress)
		}
	}
	// 拼接接收方地址（用于 Out 记录显示）
	toAddressForOut := ""
	if len(realRecipients) == 1 {
		toAddressForOut = realRecipients[0]
	} else if len(realRecipients) > 1 {
		toAddressForOut = realRecipients[0] + fmt.Sprintf(" (+%d)", len(realRecipients)-1)
	}

	// 为发送方记录支出流水 (每个 spent UTXO 一条 out 记录)
	for _, utxo := range spentUtxos {
		tx := &mrc20.Mrc20Transaction{
			TxId:         txId,
			TxPoint:      utxo.TxPoint + "_out",
			PinId:        "",
			TickId:       utxo.Mrc20Id,
			Tick:         utxo.Tick,
			TxType:       "native_transfer",
			Direction:    "out",
			Address:      utxo.ToAddress,
			FromAddress:  utxo.ToAddress,
			ToAddress:    toAddressForOut,
			Amount:       utxo.AmtChange,
			IsChange:     false,
			Chain:        chainName,
			BlockHeight:  -1, // mempool 阶段标记为 -1
			Timestamp:    timestamp,
			SpentUtxos:   string(spentJson),
			CreatedUtxos: string(createdJson),
			Msg:          utxo.Msg,
			Status:       1,
		}
		if err := pd.SaveMrc20Transaction(tx); err != nil {
			log.Printf("SaveMempoolNativeTransferTransaction (out) error: %v", err)
			return err
		}
	}

	// 为接收方记录收入流水 (每个 created UTXO 一条 in 记录)
	for _, utxo := range createdUtxos {
		isChange := utxo.ToAddress == fromAddress

		tx := &mrc20.Mrc20Transaction{
			TxId:         txId,
			TxPoint:      utxo.TxPoint,
			PinId:        "",
			TickId:       utxo.Mrc20Id,
			Tick:         utxo.Tick,
			TxType:       "native_transfer",
			Direction:    "in",
			Address:      utxo.ToAddress,
			FromAddress:  fromAddress,
			ToAddress:    utxo.ToAddress,
			Amount:       utxo.AmtChange,
			IsChange:     isChange,
			Chain:        chainName,
			BlockHeight:  -1, // mempool 阶段标记为 -1
			Timestamp:    timestamp,
			SpentUtxos:   string(spentJson),
			CreatedUtxos: string(createdJson),
			Msg:          utxo.Msg,
			Status:       utxo.Status,
		}
		if err := pd.SaveMrc20Transaction(tx); err != nil {
			log.Printf("SaveMempoolNativeTransferTransaction (in) error: %v", err)
			return err
		}

		// 保存 TransferPendingIn 记录（用于接收方的 PendingInBalance）
		// 包括找零：发送方的找零也需要计入 pendingIn，因为公式是 可用余额 = Balance + pendingIn - pendingOut
		pendingInRecord := &mrc20.TransferPendingIn{
			TxPoint:     utxo.TxPoint,
			TxId:        txId,
			ToAddress:   utxo.ToAddress,
			TickId:      utxo.Mrc20Id,
			Tick:        utxo.Tick,
			Amount:      utxo.AmtChange,
			Chain:       chainName,
			FromAddress: fromAddress,
			TxType:      "native_transfer",
			BlockHeight: -1,
			Timestamp:   timestamp,
		}
		if err := pd.SaveTransferPendingIn(pendingInRecord); err != nil {
			log.Printf("SaveMempoolNativeTransferTransaction SaveTransferPendingIn error: %v", err)
			// 不阻断主流程
		}
	}

	//log.Printf("[MRC20] SaveMempoolNativeTransferTransaction: txId=%s, spent=%d, created=%d", txId, len(spentUtxos), len(createdUtxos))
	return nil
}

// SaveMempoolTransferTransaction mempool 阶段保存 transfer PIN 流水记录（只写流水，不更新余额）
// BlockHeight 设置为 -1，出块后由 UpdateMrc20TransactionBlockHeight 更新
func (pd *PebbleData) SaveMempoolTransferTransaction(
	pinNode *pin.PinInscription,
	spentUtxos []*mrc20.Mrc20Utxo,
	createdUtxos []*mrc20.Mrc20Utxo,
) error {
	// 构建 spent 和 created 列表
	spentList := []string{}
	createdList := []string{}
	for _, u := range spentUtxos {
		spentList = append(spentList, u.TxPoint)
	}
	for _, u := range createdUtxos {
		createdList = append(createdList, u.TxPoint)
	}
	spentJson, _ := json.Marshal(spentList)
	createdJson, _ := json.Marshal(createdList)

	// 获取发送方地址
	fromAddress := ""
	if len(spentUtxos) > 0 {
		fromAddress = spentUtxos[0].ToAddress
	}

	// 收集所有非找零的接收方（真正的转账目标）
	var realRecipients []string
	for _, utxo := range createdUtxos {
		if utxo.ToAddress != fromAddress {
			realRecipients = append(realRecipients, utxo.ToAddress)
		}
	}
	// 拼接接收方地址（用于 Out 记录显示）
	toAddressForOut := ""
	if len(realRecipients) == 1 {
		toAddressForOut = realRecipients[0]
	} else if len(realRecipients) > 1 {
		toAddressForOut = realRecipients[0] + fmt.Sprintf(" (+%d)", len(realRecipients)-1)
	}

	// 为发送方记录支出流水 (每个 spent UTXO 一条 out 记录)
	for _, utxo := range spentUtxos {
		tx := &mrc20.Mrc20Transaction{
			TxId:         pinNode.GenesisTransaction,
			TxPoint:      utxo.TxPoint + "_out",
			PinId:        pinNode.Id,
			TickId:       utxo.Mrc20Id,
			Tick:         utxo.Tick,
			TxType:       "transfer",
			Direction:    "out",
			Address:      utxo.ToAddress,
			FromAddress:  utxo.ToAddress,
			ToAddress:    toAddressForOut,
			Amount:       utxo.AmtChange,
			IsChange:     false,
			Chain:        pinNode.ChainName,
			BlockHeight:  -1, // mempool 阶段标记为 -1
			Timestamp:    pinNode.Timestamp,
			SpentUtxos:   string(spentJson),
			CreatedUtxos: string(createdJson),
			Msg:          utxo.Msg,
			Status:       1,
		}
		if err := pd.SaveMrc20Transaction(tx); err != nil {
			log.Printf("SaveMempoolTransferTransaction (out) error: %v", err)
			return err
		}
	}

	// 为接收方记录收入流水 (每个 created UTXO 一条 in 记录)
	for _, utxo := range createdUtxos {
		isChange := utxo.ToAddress == fromAddress

		tx := &mrc20.Mrc20Transaction{
			TxId:         pinNode.GenesisTransaction,
			TxPoint:      utxo.TxPoint,
			PinId:        pinNode.Id,
			TickId:       utxo.Mrc20Id,
			Tick:         utxo.Tick,
			TxType:       utxo.MrcOption,
			Direction:    "in",
			Address:      utxo.ToAddress,
			FromAddress:  fromAddress,
			ToAddress:    utxo.ToAddress,
			Amount:       utxo.AmtChange,
			IsChange:     isChange,
			Chain:        pinNode.ChainName,
			BlockHeight:  -1, // mempool 阶段标记为 -1
			Timestamp:    pinNode.Timestamp,
			SpentUtxos:   string(spentJson),
			CreatedUtxos: string(createdJson),
			Msg:          utxo.Msg,
			Status:       utxo.Status,
		}
		if err := pd.SaveMrc20Transaction(tx); err != nil {
			log.Printf("SaveMempoolTransferTransaction (in) error: %v", err)
			return err
		}

		// 保存 TransferPendingIn 记录（用于接收方的 PendingInBalance）
		// 包括找零：发送方的找零也需要计入 pendingIn，因为公式是 可用余额 = Balance + pendingIn - pendingOut
		pendingInRecord := &mrc20.TransferPendingIn{
			TxPoint:     utxo.TxPoint,
			TxId:        pinNode.GenesisTransaction,
			ToAddress:   utxo.ToAddress,
			TickId:      utxo.Mrc20Id,
			Tick:        utxo.Tick,
			Amount:      utxo.AmtChange,
			Chain:       pinNode.ChainName,
			FromAddress: fromAddress,
			TxType:      "transfer",
			BlockHeight: -1,
			Timestamp:   pinNode.Timestamp,
		}
		if err := pd.SaveTransferPendingIn(pendingInRecord); err != nil {
			log.Printf("SaveMempoolTransferTransaction SaveTransferPendingIn error: %v", err)
			// 不阻断主流程
		}
	}

	//log.Printf("[MRC20] SaveMempoolTransferTransaction: pinId=%s, spent=%d, created=%d", pinNode.Id, len(spentUtxos), len(createdUtxos))
	return nil
}

// ProcessNativeTransferSuccess 处理 Native Transfer 成功后的状态更新
// 与 ProcessTransferSuccess 类似，但不需要 PIN 信息
func (pd *PebbleData) ProcessNativeTransferSuccess(
	txId, chainName string,
	blockHeight int64,
	spentUtxos []*mrc20.Mrc20Utxo,
	createdUtxos []*mrc20.Mrc20Utxo,
) error {
	log.Printf("[DEBUG] ProcessNativeTransferSuccess: txId=%s, height=%d, spent=%d, created=%d", txId, blockHeight, len(spentUtxos), len(createdUtxos))

	// 获取时间戳
	var timestamp int64
	if len(createdUtxos) > 0 {
		timestamp = createdUtxos[0].Timestamp
	}

	// 收集需要更新的余额
	balanceUpdates := make(map[string]*mrc20.Mrc20AccountBalance)

	// 1. 标记发送方的 spent UTXO 并更新余额
	for _, utxo := range spentUtxos {
		log.Printf("[DEBUG] ProcessNativeTransferSuccess: processing spent UTXO %s, addr=%s, amt=%s", utxo.TxPoint, utxo.ToAddress, utxo.AmtChange)
		// 标记 UTXO 为已消费（不删除，用于回滚）
		err := pd.MarkUtxoAsSpent(utxo.TxPoint, utxo.ToAddress, utxo.Mrc20Id, utxo.Chain, blockHeight)
		if err != nil {
			log.Printf("[ERROR] MarkUtxoAsSpent failed for native transfer %s: %v", utxo.TxPoint, err)
		}

		// 更新发送方余额
		key := fmt.Sprintf("%s_%s_%s", utxo.Chain, utxo.ToAddress, utxo.Mrc20Id)
		if _, exists := balanceUpdates[key]; !exists {
			balance, _ := pd.GetMrc20AccountBalance(utxo.Chain, utxo.ToAddress, utxo.Mrc20Id)
			if balance == nil {
				balance = &mrc20.Mrc20AccountBalance{
					Address: utxo.ToAddress,
					TickId:  utxo.Mrc20Id,
					Tick:    utxo.Tick,
					Chain:   utxo.Chain,
				}
			}
			log.Printf("[DEBUG] ProcessNativeTransferSuccess: sender %s current balance=%s", utxo.ToAddress, balance.Balance)
			balanceUpdates[key] = balance
		}
		oldBalance := balanceUpdates[key].Balance
		balanceUpdates[key].Balance = balanceUpdates[key].Balance.Sub(utxo.AmtChange)
		log.Printf("[DEBUG] ProcessNativeTransferSuccess: sender %s balance %s - %s = %s", utxo.ToAddress, oldBalance, utxo.AmtChange, balanceUpdates[key].Balance)
		balanceUpdates[key].UtxoCount--
		balanceUpdates[key].LastUpdateTx = txId
		balanceUpdates[key].LastUpdateHeight = blockHeight
		balanceUpdates[key].LastUpdateTime = timestamp
	}

	// 2. 处理接收方余额 (增加) 并更新 createdUtxos 的 BlockHeight
	var updatedUtxos []mrc20.Mrc20Utxo
	for _, utxo := range createdUtxos {
		log.Printf("[DEBUG] ProcessNativeTransferSuccess: processing created UTXO %s, addr=%s, amt=%s", utxo.TxPoint, utxo.ToAddress, utxo.AmtChange)
		key := fmt.Sprintf("%s_%s_%s", utxo.Chain, utxo.ToAddress, utxo.Mrc20Id)
		if _, exists := balanceUpdates[key]; !exists {
			balance, _ := pd.GetMrc20AccountBalance(utxo.Chain, utxo.ToAddress, utxo.Mrc20Id)
			if balance == nil {
				balance = &mrc20.Mrc20AccountBalance{
					Address: utxo.ToAddress,
					TickId:  utxo.Mrc20Id,
					Tick:    utxo.Tick,
					Chain:   utxo.Chain,
				}
			}
			log.Printf("[DEBUG] ProcessNativeTransferSuccess: receiver %s current balance=%s", utxo.ToAddress, balance.Balance)
			balanceUpdates[key] = balance
		}
		oldBalance := balanceUpdates[key].Balance
		balanceUpdates[key].Balance = balanceUpdates[key].Balance.Add(utxo.AmtChange)
		log.Printf("[DEBUG] ProcessNativeTransferSuccess: receiver %s balance %s + %s = %s", utxo.ToAddress, oldBalance, utxo.AmtChange, balanceUpdates[key].Balance)
		balanceUpdates[key].UtxoCount++
		balanceUpdates[key].LastUpdateTx = txId
		balanceUpdates[key].LastUpdateHeight = blockHeight
		balanceUpdates[key].LastUpdateTime = timestamp

		// 更新 UTXO 的 BlockHeight（从 -1 更新为实际区块高度）
		utxo.BlockHeight = blockHeight
		updatedUtxos = append(updatedUtxos, *utxo)
	}

	// 2.5 保存更新后的 createdUtxos（更新 BlockHeight）
	if len(updatedUtxos) > 0 {
		if err := pd.SaveMrc20Pin(updatedUtxos); err != nil {
			log.Printf("SaveMrc20Pin for createdUtxos error: %v", err)
			return err
		}
	}

	// 3. 保存余额
	for _, balance := range balanceUpdates {
		if err := pd.SaveMrc20AccountBalance(balance); err != nil {
			log.Printf("SaveMrc20AccountBalance error: %v", err)
			return err
		}
	}

	// 4. 写入 UTXO 流水 - 每个地址每个 UTXO 变动一条记录
	// 构建 spent 和 created 列表
	spentList := []string{}
	createdList := []string{}
	for _, u := range spentUtxos {
		spentList = append(spentList, u.TxPoint)
	}
	for _, u := range createdUtxos {
		createdList = append(createdList, u.TxPoint)
	}
	spentJson, _ := json.Marshal(spentList)
	createdJson, _ := json.Marshal(createdList)

	// 获取发送方地址
	fromAddress := ""
	if len(spentUtxos) > 0 {
		fromAddress = spentUtxos[0].ToAddress
	}

	// 收集所有非找零的接收方（真正的转账目标）
	var realRecipients []string
	for _, utxo := range createdUtxos {
		if utxo.ToAddress != fromAddress {
			realRecipients = append(realRecipients, utxo.ToAddress)
		}
	}
	// 拼接接收方地址（用于 Out 记录显示）
	toAddressForOut := ""
	if len(realRecipients) == 1 {
		toAddressForOut = realRecipients[0]
	} else if len(realRecipients) > 1 {
		toAddressForOut = realRecipients[0] + fmt.Sprintf(" (+%d)", len(realRecipients)-1)
	}

	// 4.1 为发送方记录支出流水 (每个 spent UTXO 一条 out 记录)
	for _, utxo := range spentUtxos {
		tx := &mrc20.Mrc20Transaction{
			TxId:         txId,
			TxPoint:      utxo.TxPoint + "_out",
			PinId:        "",
			TickId:       utxo.Mrc20Id,
			Tick:         utxo.Tick,
			TxType:       "native_transfer",
			Direction:    "out",
			Address:      utxo.ToAddress,
			FromAddress:  utxo.ToAddress,
			ToAddress:    toAddressForOut, // 显示真正的接收方
			Amount:       utxo.AmtChange,
			IsChange:     false,
			Chain:        chainName,
			BlockHeight:  blockHeight,
			Timestamp:    timestamp,
			SpentUtxos:   string(spentJson),
			CreatedUtxos: string(createdJson),
			Msg:          utxo.Msg,
			Status:       1,
		}
		if err := pd.SaveMrc20Transaction(tx); err != nil {
			log.Printf("SaveMrc20Transaction (out) error: %v", err)
			return err
		}
	}

	// 4.2 为接收方记录收入流水 (每个 created UTXO 一条 in 记录)
	for _, utxo := range createdUtxos {
		isChange := utxo.ToAddress == fromAddress

		tx := &mrc20.Mrc20Transaction{
			TxId:         txId,
			TxPoint:      utxo.TxPoint,
			PinId:        "",
			TickId:       utxo.Mrc20Id,
			Tick:         utxo.Tick,
			TxType:       "native_transfer",
			Direction:    "in",
			Address:      utxo.ToAddress,
			FromAddress:  fromAddress,
			ToAddress:    utxo.ToAddress,
			Amount:       utxo.AmtChange,
			IsChange:     isChange,
			Chain:        chainName,
			BlockHeight:  blockHeight,
			Timestamp:    timestamp,
			SpentUtxos:   string(spentJson),
			CreatedUtxos: string(createdJson),
			Msg:          utxo.Msg,
			Status:       utxo.Status,
		}
		if err := pd.SaveMrc20Transaction(tx); err != nil {
			log.Printf("SaveMrc20Transaction (in) error: %v", err)
			return err
		}

		// 删除 TransferPendingIn 记录（如果存在，表示 mempool 阶段创建的）
		if !isChange {
			if err := pd.DeleteTransferPendingIn(utxo.TxPoint, utxo.ToAddress); err != nil {
				// 不阻断主流程，只记录日志
				log.Printf("DeleteTransferPendingIn warning: %v", err)
			}
		}
	}

	return nil
}
