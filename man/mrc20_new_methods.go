package man

import (
	"encoding/json"
	"fmt"
	"log"
	"manindexer/mrc20"
	"manindexer/pin"
	"sort"

	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"
	"github.com/shopspring/decimal"
)

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
// Key 设计：
//   - 主键: tx_{chain}_{txPoint}  (txPoint 是唯一的)
//   - 按 tick 查: tx_tick_{chain}_{tickId}_{blockHeight}_{timestamp}_{txPoint}
//   - 按地址+tick 查发送: tx_from_{chain}_{address}_{tickId}_{blockHeight}_{timestamp}_{txPoint}
//   - 按地址+tick 查接收: tx_to_{chain}_{address}_{tickId}_{blockHeight}_{timestamp}_{txPoint}
func (pd *PebbleData) SaveMrc20Transaction(tx *mrc20.Mrc20Transaction) error {
	data, err := sonic.Marshal(tx)
	if err != nil {
		return err
	}

	// 从 CreatedUtxos 中提取 txPoint 作为主键
	txPointForKey := tx.TxId // fallback
	if tx.CreatedUtxos != "" && tx.CreatedUtxos != "[]" {
		var utxos []string
		if err := sonic.UnmarshalString(tx.CreatedUtxos, &utxos); err == nil && len(utxos) > 0 {
			txPointForKey = utxos[0]
		}
	}

	batch := pd.Database.MrcDb.NewBatch()
	defer batch.Close()

	// 主键: tx_{chain}_{txPoint}
	key := []byte(fmt.Sprintf("tx_%s_%s", tx.Chain, txPointForKey))
	batch.Set(key, data, pebble.Sync)

	// 按 tick 查历史索引: tx_tick_{chain}_{tickId}_{blockHeight}_{timestamp}_{txPoint}
	tickKey := []byte(fmt.Sprintf("tx_tick_%s_%s_%012d_%012d_%s", tx.Chain, tx.TickId, tx.BlockHeight, tx.Timestamp, txPointForKey))
	batch.Set(tickKey, key, pebble.Sync)

	// 地址发送索引: tx_from_{chain}_{fromAddress}_{tickId}_{blockHeight}_{timestamp}_{txPoint}
	if tx.FromAddress != "" {
		fromKey := []byte(fmt.Sprintf("tx_from_%s_%s_%s_%012d_%012d_%s", tx.Chain, tx.FromAddress, tx.TickId, tx.BlockHeight, tx.Timestamp, txPointForKey))
		batch.Set(fromKey, key, pebble.Sync)
	}

	// 地址接收索引: tx_to_{chain}_{toAddress}_{tickId}_{blockHeight}_{timestamp}_{txPoint}
	if tx.ToAddress != "" {
		toKey := []byte(fmt.Sprintf("tx_to_%s_%s_%s_%012d_%012d_%s", tx.Chain, tx.ToAddress, tx.TickId, tx.BlockHeight, tx.Timestamp, txPointForKey))
		batch.Set(toKey, key, pebble.Sync)
	}

	return batch.Commit(pebble.Sync)
}

// GetMrc20TransactionHistory 查询交易历史
// 参数:
//
//	address: 地址 (必填)
//	tickId: tick ID (可选，空表示所有)
//	chain: 链名称 (必填)
//	limit: 分页大小
//	offset: 分页偏移
func (pd *PebbleData) GetMrc20TransactionHistory(
	address, tickId, chain string,
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
		allPrefix := []byte(fmt.Sprintf("tx_tick_%s_%s_", chain, tickId))

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

		// 第二遍：分页读取（从后往前，按 TxIndex 降序）
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
	} else {
		// 查询指定地址的交易：需要合并 from 和 to 索引，去重后排序
		type txEntry struct {
			primaryKey  string
			blockHeight int64
			timestamp   int64
			txPoint     string
		}
		primaryKeyMap := make(map[string]bool) // 用于去重
		var entries []txEntry

		// 从 tx_from 索引读取
		fromPrefix := []byte(fmt.Sprintf("tx_from_%s_%s_%s_", chain, address, tickId))
		fromIter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
			LowerBound: fromPrefix,
			UpperBound: append(fromPrefix, 0xff),
		})
		if err != nil {
			return nil, 0, err
		}
		for fromIter.First(); fromIter.Valid(); fromIter.Next() {
			primaryKey := string(fromIter.Value())
			if !primaryKeyMap[primaryKey] {
				primaryKeyMap[primaryKey] = true
				// 从索引键解析 blockHeight, timestamp, txPoint
				// 格式: tx_from_{chain}_{address}_{tickId}_{blockHeight}_{timestamp}_{txPoint}
				indexKey := string(fromIter.Key())
				var bh, ts int64
				var txp string
				// 跳过前缀部分，解析后面的数字
				prefixLen := len(fmt.Sprintf("tx_from_%s_%s_%s_", chain, address, tickId))
				if len(indexKey) > prefixLen {
					remaining := indexKey[prefixLen:]
					fmt.Sscanf(remaining, "%012d_%012d_%s", &bh, &ts, &txp)
				}
				entries = append(entries, txEntry{primaryKey: primaryKey, blockHeight: bh, timestamp: ts, txPoint: txp})
			}
		}
		fromIter.Close()

		// 从 tx_to 索引读取
		toPrefix := []byte(fmt.Sprintf("tx_to_%s_%s_%s_", chain, address, tickId))
		toIter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
			LowerBound: toPrefix,
			UpperBound: append(toPrefix, 0xff),
		})
		if err != nil {
			return nil, 0, err
		}
		for toIter.First(); toIter.Valid(); toIter.Next() {
			primaryKey := string(toIter.Value())
			if !primaryKeyMap[primaryKey] {
				primaryKeyMap[primaryKey] = true
				indexKey := string(toIter.Key())
				var bh, ts int64
				var txp string
				prefixLen := len(fmt.Sprintf("tx_to_%s_%s_%s_", chain, address, tickId))
				if len(indexKey) > prefixLen {
					remaining := indexKey[prefixLen:]
					fmt.Sscanf(remaining, "%012d_%012d_%s", &bh, &ts, &txp)
				}
				entries = append(entries, txEntry{primaryKey: primaryKey, blockHeight: bh, timestamp: ts, txPoint: txp})
			}
		}
		toIter.Close()

		// 按 blockHeight 降序、timestamp 降序、txPoint 降序排序
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].blockHeight != entries[j].blockHeight {
				return entries[i].blockHeight > entries[j].blockHeight
			}
			if entries[i].timestamp != entries[j].timestamp {
				return entries[i].timestamp > entries[j].timestamp
			}
			return entries[i].txPoint > entries[j].txPoint
		})

		total = int64(len(entries))

		// 分页
		start := offset
		end := offset + limit
		if start >= len(entries) {
			return []*mrc20.Mrc20Transaction{}, total, nil
		}
		if end > len(entries) {
			end = len(entries)
		}

		// 读取交易详情
		for i := start; i < end; i++ {
			data, closer, err := pd.Database.MrcDb.Get([]byte(entries[i].primaryKey))
			if err != nil {
				continue
			}
			var tx mrc20.Mrc20Transaction
			if err := sonic.Unmarshal(data, &tx); err != nil {
				closer.Close()
				continue
			}
			closer.Close()
			transactions = append(transactions, &tx)
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

// ============== 辅助方法：UTXO 删除 ==============

// DeleteMrc20Utxo 删除 UTXO 记录 (spent 后调用)
// 删除所有相关索引：主记录、mrc20_in、available_utxo
func (pd *PebbleData) DeleteMrc20Utxo(txPoint, address, tickId string) error {
	// 先读取 UTXO 获取 chain 信息
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

	// 删除 mrc20_in 索引
	inKey := []byte(fmt.Sprintf("mrc20_in_%s_%s_%s", address, tickId, txPoint))
	batch.Delete(inKey, pebble.Sync)

	// 删除 available_utxo 索引
	if chain != "" {
		availableKey := []byte(fmt.Sprintf("available_utxo_%s_%s_%s_%s", chain, address, tickId, txPoint))
		batch.Delete(availableKey, pebble.Sync)
	}

	log.Printf("[MRC20] DeleteMrc20Utxo: txPoint=%s, address=%s, tickId=%s", txPoint, address, tickId)
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

		// 地址索引
		if utxo.ToAddress != "" {
			inKey := []byte(fmt.Sprintf("mrc20_in_%s_%s_%s", utxo.ToAddress, utxo.Mrc20Id, utxo.TxPoint))
			batch.Set(inKey, data, pebble.Sync)

			// available_utxo 索引（只有 status=0 的 UTXO）
			if utxo.Status == mrc20.UtxoStatusAvailable {
				availableKey := []byte(fmt.Sprintf("available_utxo_%s_%s_%s_%s", utxo.Chain, utxo.ToAddress, utxo.Mrc20Id, utxo.TxPoint))
				batch.Set(availableKey, data, pebble.Sync)
			}
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

		// 从 CreatedUtxos 中提取 txPoint 作为主键
		txPointForKey := tx.TxId // fallback
		if tx.CreatedUtxos != "" && tx.CreatedUtxos != "[]" {
			var utxos []string
			if err := sonic.UnmarshalString(tx.CreatedUtxos, &utxos); err == nil && len(utxos) > 0 {
				txPointForKey = utxos[0]
			}
		}

		// 主键: tx_{chain}_{txPoint}
		key := []byte(fmt.Sprintf("tx_%s_%s", tx.Chain, txPointForKey))
		batch.Set(key, data, pebble.Sync)

		// 按 tick 查历史索引
		tickKey := []byte(fmt.Sprintf("tx_tick_%s_%s_%012d_%012d_%s", tx.Chain, tx.TickId, tx.BlockHeight, tx.Timestamp, txPointForKey))
		batch.Set(tickKey, key, pebble.Sync)

		// 索引格式必须与 SaveMrc20Transaction 一致
		if tx.FromAddress != "" {
			fromKey := []byte(fmt.Sprintf("tx_from_%s_%s_%s_%012d_%012d_%s", tx.Chain, tx.FromAddress, tx.TickId, tx.BlockHeight, tx.Timestamp, txPointForKey))
			batch.Set(fromKey, key, pebble.Sync)
		}
		if tx.ToAddress != "" {
			toKey := []byte(fmt.Sprintf("tx_to_%s_%s_%s_%012d_%012d_%s", tx.Chain, tx.ToAddress, tx.TickId, tx.BlockHeight, tx.Timestamp, txPointForKey))
			batch.Set(toKey, key, pebble.Sync)
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

	// 2. 写入交易流水
	createdUtxos, _ := json.Marshal([]string{utxo.TxPoint})
	tx := &mrc20.Mrc20Transaction{
		TxId:         utxo.OperationTx,
		TxPoint:      utxo.TxPoint,
		PinId:        utxo.PinId,
		TickId:       utxo.Mrc20Id,
		Tick:         utxo.Tick,
		TxType:       "mint",
		FromAddress:  "",
		ToAddress:    utxo.ToAddress,
		Amount:       utxo.AmtChange,
		Chain:        utxo.Chain,
		BlockHeight:  utxo.BlockHeight,
		Timestamp:    utxo.Timestamp,
		CreatedUtxos: string(createdUtxos),
		Msg:          utxo.Msg,
		Status:       1,
	}
	return pd.SaveMrc20Transaction(tx)
}

// ProcessTransferSuccess 处理 Transfer 成功后的状态更新
// 包括：删除 spent UTXO、更新余额、写入流水
func (pd *PebbleData) ProcessTransferSuccess(
	pinNode *pin.PinInscription,
	spentUtxos []*mrc20.Mrc20Utxo,
	createdUtxos []*mrc20.Mrc20Utxo,
) error {
	// 收集需要更新的余额
	balanceUpdates := make(map[string]*mrc20.Mrc20AccountBalance) // key: chain_address_tickId

	// 1. 删除发送方的 spent UTXO 并更新余额
	for _, utxo := range spentUtxos {
		// 删除 spent UTXO（根据新架构设计）
		err := pd.DeleteMrc20Utxo(utxo.TxPoint, utxo.ToAddress, utxo.Mrc20Id)
		if err != nil {
			log.Printf("[ERROR] DeleteMrc20Utxo failed for %s: %v", utxo.TxPoint, err)
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

	// 4. 写入 UTXO 流水 (每个 created UTXO 一条记录)
	for _, utxo := range createdUtxos {
		// 构建 spent 和 created 列表（用于关联查询）
		spentList := []string{}
		createdList := []string{}
		for _, u := range spentUtxos {
			if u.Mrc20Id == utxo.Mrc20Id {
				spentList = append(spentList, u.TxPoint)
			}
		}
		for _, u := range createdUtxos {
			if u.Mrc20Id == utxo.Mrc20Id {
				createdList = append(createdList, u.TxPoint)
			}
		}
		spentJson, _ := json.Marshal(spentList)
		createdJson, _ := json.Marshal(createdList)

		tx := &mrc20.Mrc20Transaction{
			TxId:         pinNode.GenesisTransaction,
			TxPoint:      utxo.TxPoint,
			PinId:        pinNode.Id,
			TickId:       utxo.Mrc20Id,
			Tick:         utxo.Tick,
			TxType:       utxo.MrcOption, // 使用 UTXO 的 MrcOption (data-transfer 等)
			FromAddress:  utxo.FromAddress,
			ToAddress:    utxo.ToAddress,
			Amount:       utxo.AmtChange,
			Chain:        pinNode.ChainName,
			BlockHeight:  pinNode.GenesisHeight,
			Timestamp:    pinNode.Timestamp,
			SpentUtxos:   string(spentJson),
			CreatedUtxos: string(createdJson),
			Msg:          utxo.Msg,
			Status:       utxo.Status,
		}

		if err := pd.SaveMrc20Transaction(tx); err != nil {
			log.Printf("SaveMrc20Transaction error: %v", err)
			return err
		}
	}

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
	// 获取时间戳
	var timestamp int64
	if len(createdUtxos) > 0 {
		timestamp = createdUtxos[0].Timestamp
	}

	// 收集需要更新的余额
	balanceUpdates := make(map[string]*mrc20.Mrc20AccountBalance)

	// 1. 删除发送方的 spent UTXO 并更新余额
	for _, utxo := range spentUtxos {
		// 删除 spent UTXO
		err := pd.DeleteMrc20Utxo(utxo.TxPoint, utxo.ToAddress, utxo.Mrc20Id)
		if err != nil {
			log.Printf("[ERROR] DeleteMrc20Utxo failed for native transfer %s: %v", utxo.TxPoint, err)
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
		balanceUpdates[key].LastUpdateTx = txId
		balanceUpdates[key].LastUpdateHeight = blockHeight
		balanceUpdates[key].LastUpdateTime = timestamp
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
		balanceUpdates[key].LastUpdateTx = txId
		balanceUpdates[key].LastUpdateHeight = blockHeight
		balanceUpdates[key].LastUpdateTime = timestamp
	}

	// 3. 保存余额
	for _, balance := range balanceUpdates {
		if err := pd.SaveMrc20AccountBalance(balance); err != nil {
			log.Printf("SaveMrc20AccountBalance error: %v", err)
			return err
		}
	}

	// 4. 写入 UTXO 流水 (每个 created UTXO 一条记录)
	for _, utxo := range createdUtxos {
		// 构建 spent 和 created 列表
		spentList := []string{}
		createdList := []string{}
		for _, u := range spentUtxos {
			if u.Mrc20Id == utxo.Mrc20Id {
				spentList = append(spentList, u.TxPoint)
			}
		}
		for _, u := range createdUtxos {
			if u.Mrc20Id == utxo.Mrc20Id {
				createdList = append(createdList, u.TxPoint)
			}
		}
		spentJson, _ := json.Marshal(spentList)
		createdJson, _ := json.Marshal(createdList)

		tx := &mrc20.Mrc20Transaction{
			TxId:         txId,
			TxPoint:      utxo.TxPoint,
			PinId:        "", // Native transfer 没有 PIN
			TickId:       utxo.Mrc20Id,
			Tick:         utxo.Tick,
			TxType:       "native_transfer",
			FromAddress:  utxo.FromAddress,
			ToAddress:    utxo.ToAddress,
			Amount:       utxo.AmtChange,
			Chain:        chainName,
			BlockHeight:  blockHeight,
			Timestamp:    timestamp,
			SpentUtxos:   string(spentJson),
			CreatedUtxos: string(createdJson),
			Msg:          utxo.Msg,
			Status:       utxo.Status,
		}

		if err := pd.SaveMrc20Transaction(tx); err != nil {
			log.Printf("SaveMrc20Transaction error: %v", err)
			return err
		}
	}

	return nil
}
