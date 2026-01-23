package man

import (
	"fmt"
	"log"
	"manindexer/mrc20"
	"manindexer/pin"
	"sort"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"
	"github.com/shopspring/decimal"
)

// SaveMrc20Pin 保存 MRC20 PIN 数据
// 根据新架构设计：
// - UTXO 表只保留 status=0 (Available) 和 status=1/2 (Pending) 的记录
// - status=-1 (Spent) 的 UTXO 应该通过 DeleteMrc20Utxo 删除，不应该通过此方法保存
// 索引：
// - mrc20_utxo_{txPoint}: UTXO 主记录
// - mrc20_in_{ToAddress}_{mrc20Id}_{txPoint}: 地址收入索引
// - available_utxo_{chain}_{address}_{tickId}_{txPoint}: 可用 UTXO 专用索引 (只存储 status=0)
func (pd *PebbleData) SaveMrc20Pin(utxoList []mrc20.Mrc20Utxo) error {
	batch := pd.Database.MrcDb.NewBatch()
	defer batch.Close()

	for _, utxo := range utxoList {
		// 跳过 status=-1 的记录，Spent UTXO 不应该通过此方法保存
		if utxo.Status == mrc20.UtxoStatusSpent {
			log.Printf("[WARN] SaveMrc20Pin: skipping spent UTXO %s, use DeleteMrc20Utxo instead", utxo.TxPoint)
			continue
		}

		// 保存 UTXO 数据
		// Key: mrc20_utxo_{txPoint}
		data, err := sonic.Marshal(utxo)
		if err != nil {
			log.Println("Marshal mrc20 utxo error:", err)
			continue
		}

		key := fmt.Sprintf("mrc20_utxo_%s", utxo.TxPoint)
		err = batch.Set([]byte(key), data, pebble.Sync)
		if err != nil {
			log.Println("Set mrc20 utxo error:", err)
		}

		// 收入索引：所有记录都写入 mrc20_in_{ToAddress}
		// 这个索引用于计算余额（只需扫描 mrc20_in_ 前缀，Status=0 的记录）
		if utxo.ToAddress != "" {
			inKey := fmt.Sprintf("mrc20_in_%s_%s_%s", utxo.ToAddress, utxo.Mrc20Id, utxo.TxPoint)
			err = batch.Set([]byte(inKey), data, pebble.Sync)
			if err != nil {
				log.Println("Set mrc20 income index error:", err)
			}
		}

		// 可用 UTXO 专用索引：只有 status=0 的 UTXO 写入此索引
		// 用于快速查询某地址某 tick 的可用 UTXO
		if utxo.Status == mrc20.UtxoStatusAvailable && utxo.ToAddress != "" {
			availableKey := fmt.Sprintf("available_utxo_%s_%s_%s_%s", utxo.Chain, utxo.ToAddress, utxo.Mrc20Id, utxo.TxPoint)
			err = batch.Set([]byte(availableKey), data, pebble.Sync)
			if err != nil {
				log.Println("Set available utxo index error:", err)
			}
		}
	}

	return batch.Commit(pebble.Sync)
}

// SaveMrc20Tick 保存 MRC20 代币信息
func (pd *PebbleData) SaveMrc20Tick(tickList []mrc20.Mrc20DeployInfo) error {
	batch := pd.Database.MrcDb.NewBatch()
	defer batch.Close()

	for _, tick := range tickList {
		data, err := sonic.Marshal(tick)
		if err != nil {
			log.Println("Marshal mrc20 tick error:", err)
			continue
		}

		// Key: mrc20_tick_{mrc20Id}
		key := fmt.Sprintf("mrc20_tick_%s", tick.Mrc20Id)
		err = batch.Set([]byte(key), data, pebble.Sync)
		if err != nil {
			log.Println("Set mrc20 tick error:", err)
		}

		// 为 tick 名称建立索引
		// Key: mrc20_tick_name_{tickName}
		tickNameKey := fmt.Sprintf("mrc20_tick_name_%s", tick.Tick)
		err = batch.Set([]byte(tickNameKey), []byte(tick.Mrc20Id), pebble.Sync)
		if err != nil {
			log.Println("Set mrc20 tick name index error:", err)
		}
	}

	return batch.Commit(pebble.Sync)
}

// GetMrc20TickList 获取 MRC20 代币列表（分页）
func (pd *PebbleData) GetMrc20TickList(cursor, limit int) ([]mrc20.Mrc20DeployInfo, error) {
	var result []mrc20.Mrc20DeployInfo

	prefix := "mrc20_tick_"
	iter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "~"),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		// 跳过 tick_name 索引
		if strings.Contains(key, "tick_name_") {
			continue
		}

		if count < cursor {
			count++
			continue
		}

		if limit > 0 && len(result) >= limit {
			break
		}

		var info mrc20.Mrc20DeployInfo
		err := sonic.Unmarshal(iter.Value(), &info)
		if err != nil {
			log.Println("Unmarshal mrc20 tick error:", err)
			continue
		}

		result = append(result, info)
		count++
	}

	return result, nil
}

// GetMrc20TickInfo 获取 MRC20 代币信息
func (pd *PebbleData) GetMrc20TickInfo(mrc20Id, tickName string) (mrc20.Mrc20DeployInfo, error) {
	var info mrc20.Mrc20DeployInfo

	var key string
	if mrc20Id != "" {
		key = fmt.Sprintf("mrc20_tick_%s", mrc20Id)
	} else if tickName != "" {
		// 先通过名称找到ID
		nameKey := fmt.Sprintf("mrc20_tick_name_%s", tickName)
		idBytes, closer, err := pd.Database.MrcDb.Get([]byte(nameKey))
		if err != nil {
			return info, err
		}
		defer closer.Close()
		mrc20Id = string(idBytes)
		key = fmt.Sprintf("mrc20_tick_%s", mrc20Id)
	} else {
		return info, fmt.Errorf("mrc20Id and tickName are both empty")
	}

	value, closer, err := pd.Database.MrcDb.Get([]byte(key))
	if err != nil {
		return info, err
	}
	defer closer.Close()

	err = sonic.Unmarshal(value, &info)
	return info, err
}

// UpdateMrc20TickInfo 更新 MRC20 代币信息（铸造数量）
func (pd *PebbleData) UpdateMrc20TickInfo(mrc20Id, txPoint string, totalMinted uint64) error {
	info, err := pd.GetMrc20TickInfo(mrc20Id, "")
	if err != nil {
		return err
	}

	info.TotalMinted = totalMinted

	data, err := sonic.Marshal(info)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("mrc20_tick_%s", mrc20Id)
	return pd.Database.MrcDb.Set([]byte(key), data, pebble.Sync)
}

// UpdateMrc20TickHolder 更新持有者数量和交易数量
func (pd *PebbleData) UpdateMrc20TickHolder(mrc20Id string, txNum int64) error {
	info, err := pd.GetMrc20TickInfo(mrc20Id, "")
	if err != nil {
		return err
	}

	info.TxCount += uint64(txNum)

	// TODO: 实现持有者数量统计逻辑

	data, err := sonic.Marshal(info)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("mrc20_tick_%s", mrc20Id)
	return pd.Database.MrcDb.Set([]byte(key), data, pebble.Sync)
}

// GetMrc20UtxoByOutPutList 根据输出列表获取 MRC20 UTXO
// 返回可用状态(0)和等待跃迁状态(1)的 UTXO
// pending 状态的 UTXO 可以被 native transfer 或普通 transfer 花费
// 花费后跃迁将失败，用户自行承担后果
func (pd *PebbleData) GetMrc20UtxoByOutPutList(outputList []string, isMempool bool) ([]*mrc20.Mrc20Utxo, error) {
	var result []*mrc20.Mrc20Utxo

	for _, output := range outputList {
		key := fmt.Sprintf("mrc20_utxo_%s", output)
		value, closer, err := pd.Database.MrcDb.Get([]byte(key))
		if err != nil {
			if err == pebble.ErrNotFound {
				continue
			}
			return nil, err
		}

		var utxo mrc20.Mrc20Utxo
		err = sonic.Unmarshal(value, &utxo)
		closer.Close()

		if err != nil {
			log.Println("Unmarshal mrc20 utxo error:", err)
			continue
		}

		// 返回可用(0)、等待跃迁(1)和等待转账确认(2)状态的 UTXO
		// 已消耗(-1)的 UTXO 不返回
		if utxo.Status == mrc20.UtxoStatusAvailable ||
			utxo.Status == mrc20.UtxoStatusTeleportPending ||
			utxo.Status == mrc20.UtxoStatusTransferPending {
			result = append(result, &utxo)
		}
	}

	return result, nil
}

// UpdateMrc20Utxo 更新 MRC20 UTXO（用于转账和状态变更）
// 根据新架构设计：
// - status=0 (Available): 保存到 UTXO 表和 available_utxo 索引
// - status=1/2 (Pending): 保存到 UTXO 表，从 available_utxo 索引删除
// - status=-1 (Spent): 从 UTXO 表和所有索引中删除
func (pd *PebbleData) UpdateMrc20Utxo(utxoList []*mrc20.Mrc20Utxo, isMempool bool) error {
	batch := pd.Database.MrcDb.NewBatch()
	defer batch.Close()

	for _, utxo := range utxoList {
		mainKey := fmt.Sprintf("mrc20_utxo_%s", utxo.TxPoint)
		inKey := fmt.Sprintf("mrc20_in_%s_%s_%s", utxo.ToAddress, utxo.Mrc20Id, utxo.TxPoint)
		availableKey := fmt.Sprintf("available_utxo_%s_%s_%s_%s", utxo.Chain, utxo.ToAddress, utxo.Mrc20Id, utxo.TxPoint)

		if utxo.Status == mrc20.UtxoStatusSpent {
			// Spent UTXO: 从所有索引中删除
			// 根据新架构设计，Spent UTXO 不保留在 UTXO 表中，历史由 Transaction 流水表记录
			err := batch.Delete([]byte(mainKey), pebble.Sync)
			if err != nil {
				log.Println("Delete mrc20 utxo error:", err)
			}

			// 删除 mrc20_in 索引
			err = batch.Delete([]byte(inKey), pebble.Sync)
			if err != nil {
				log.Println("Delete mrc20 income index error:", err)
			}

			// 删除 available_utxo 索引
			err = batch.Delete([]byte(availableKey), pebble.Sync)
			if err != nil {
				log.Println("Delete available utxo index error:", err)
			}

			log.Printf("[MRC20] Deleted spent UTXO: %s", utxo.TxPoint)
		} else {
			// 非 Spent UTXO: 保存/更新记录
			data, err := sonic.Marshal(utxo)
			if err != nil {
				log.Println("Marshal mrc20 utxo error:", err)
				continue
			}

			// 保存主记录
			err = batch.Set([]byte(mainKey), data, pebble.Sync)
			if err != nil {
				log.Println("Set mrc20 utxo error:", err)
			}

			// 保存 mrc20_in 索引
			if utxo.ToAddress != "" {
				err = batch.Set([]byte(inKey), data, pebble.Sync)
				if err != nil {
					log.Println("Set mrc20 income index error:", err)
				}
			}

			// 处理 available_utxo 索引
			if utxo.Status == mrc20.UtxoStatusAvailable && utxo.ToAddress != "" {
				// Available UTXO: 写入 available_utxo 索引
				err = batch.Set([]byte(availableKey), data, pebble.Sync)
				if err != nil {
					log.Println("Set available utxo index error:", err)
				}
			} else {
				// Pending UTXO: 从 available_utxo 索引删除
				err = batch.Delete([]byte(availableKey), pebble.Sync)
				if err != nil && err != pebble.ErrNotFound {
					log.Println("Delete available utxo index error:", err)
				}
			}
		}
	}

	return batch.Commit(pebble.Sync)
}

// AddMrc20Shovel 添加 MRC20 铲子（防止重复使用PIN）
func (pd *PebbleData) AddMrc20Shovel(shovelList []string, mintPinId, mrc20Id string) error {
	batch := pd.Database.MrcDb.NewBatch()
	defer batch.Close()

	for _, pinId := range shovelList {
		shovel := mrc20.Mrc20Shovel{
			Id:           pinId,
			Mrc20MintPin: mintPinId,
		}

		data, err := sonic.Marshal(shovel)
		if err != nil {
			log.Println("Marshal mrc20 shovel error:", err)
			continue
		}

		// Key: mrc20_shovel_{mrc20Id}_{pinId}
		key := fmt.Sprintf("mrc20_shovel_%s_%s", mrc20Id, pinId)
		err = batch.Set([]byte(key), data, pebble.Sync)
		if err != nil {
			log.Println("Set mrc20 shovel error:", err)
		}
	}

	return batch.Commit(pebble.Sync)
}

// GetMrc20Shovel 获取已使用的铲子
func (pd *PebbleData) GetMrc20Shovel(pinIds []string, mrc20Id string) (map[string]mrc20.Mrc20Shovel, error) {
	result := make(map[string]mrc20.Mrc20Shovel)

	for _, pinId := range pinIds {
		key := fmt.Sprintf("mrc20_shovel_%s_%s", mrc20Id, pinId)
		value, closer, err := pd.Database.MrcDb.Get([]byte(key))
		if err != nil {
			if err == pebble.ErrNotFound {
				continue
			}
			return nil, err
		}

		var shovel mrc20.Mrc20Shovel
		err = sonic.Unmarshal(value, &shovel)
		closer.Close()

		if err != nil {
			log.Println("Unmarshal mrc20 shovel error:", err)
			continue
		}

		result[pinId] = shovel
	}

	return result, nil
}

// CheckOperationtx 检查交易是否已处理
func (pd *PebbleData) CheckOperationtx(txId string, isMempool bool) (*mrc20.Mrc20Utxo, error) {
	// 查找这个交易ID相关的所有UTXO
	// Key: mrc20_op_tx_{txId}
	key := fmt.Sprintf("mrc20_op_tx_%s", txId)
	value, closer, err := pd.Database.MrcDb.Get([]byte(key))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	var utxo mrc20.Mrc20Utxo
	err = sonic.Unmarshal(value, &utxo)
	if err != nil {
		return nil, err
	}

	return &utxo, nil
}

// GetMrc20ByAddressAndTick 根据地址和代币ID获取余额
// 只扫描 mrc20_in_ 前缀，过滤 Status=0 的记录
func (pd *PebbleData) GetMrc20ByAddressAndTick(address, tickId string) ([]*mrc20.Mrc20Utxo, error) {
	var result []*mrc20.Mrc20Utxo

	// 使用 mrc20_in_ 前缀扫描收入记录
	prefix := fmt.Sprintf("mrc20_in_%s_%s_", address, tickId)
	iter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "~"),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var utxo mrc20.Mrc20Utxo
		err := sonic.Unmarshal(iter.Value(), &utxo)
		if err != nil {
			log.Println("Unmarshal mrc20 utxo error:", err)
			continue
		}

		// 只返回可用的 UTXO (Status=0)
		if utxo.Status == mrc20.UtxoStatusAvailable {
			result = append(result, &utxo)
		}
	}

	return result, nil
}

// GetPinListByOutPutList 根据输出列表获取 PIN 列表
func (pd *PebbleData) GetPinListByOutPutList(outputList []string) ([]*pin.PinInscription, error) {
	var result []*pin.PinInscription

	for _, output := range outputList {
		// output 格式: txid:vout
		// 需要根据 txPoint 查找 PIN
		// 在 pebblestore 中应该有相关的索引
		parts := strings.Split(output, ":")
		if len(parts) != 2 {
			continue
		}

		txid := parts[0]
		vout := parts[1]

		// 构建 pinId: txid + "i" + vout
		pinId := fmt.Sprintf("%si%s", txid, vout)

		// 从 PinsDBs 中查找
		pinBytes, err := pd.Database.GetPinByKey(pinId)
		if err != nil {
			if err != pebble.ErrNotFound {
				log.Println("Get pin by id error:", err, pinId)
			}
			continue
		}

		if pinBytes != nil {
			var pinData pin.PinInscription
			err = sonic.Unmarshal(pinBytes, &pinData)
			if err != nil {
				log.Println("Unmarshal pin error:", err)
				continue
			}
			result = append(result, &pinData)
		}
	}

	return result, nil
}

// GetMrc20Balance 获取地址的 MRC20 余额（用于 API）
func (pd *PebbleData) GetMrc20Balance(address, tickId string) (decimal.Decimal, error) {
	utxoList, err := pd.GetMrc20ByAddressAndTick(address, tickId)
	if err != nil {
		return decimal.Zero, err
	}

	balance := decimal.Zero
	for _, utxo := range utxoList {
		if utxo.Status != -1 {
			balance = balance.Add(utxo.AmtChange)
		}
	}

	return balance, nil
}

// GetMrc20UtxoList 获取地址的所有 MRC20 UTXO 列表（可用余额）
// 只扫描 mrc20_in_ 前缀，过滤 Status != -1 的记录
func (pd *PebbleData) GetMrc20UtxoList(address string, start, limit int) ([]*mrc20.Mrc20Utxo, error) {
	var result []*mrc20.Mrc20Utxo

	prefix := fmt.Sprintf("mrc20_in_%s_", address)
	iter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "~"),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		var utxo mrc20.Mrc20Utxo
		err := sonic.Unmarshal(iter.Value(), &utxo)
		if err != nil {
			log.Println("Unmarshal mrc20 utxo error:", err)
			continue
		}

		// 只返回可用的 UTXO (Status != -1)
		if utxo.Status != mrc20.UtxoStatusSpent {
			if count < start {
				count++
				continue
			}

			if limit > 0 && len(result) >= limit {
				break
			}

			result = append(result, &utxo)
			count++
		}
	}

	return result, nil
}

// GetMrc20TransferHistory 获取 MRC20 转账历史
func (pd *PebbleData) GetMrc20TransferHistory(mrc20Id string, start, limit int) ([]*mrc20.Mrc20Utxo, error) {
	var allUtxos []*mrc20.Mrc20Utxo

	// 扫描所有相关的 UTXO
	prefix := "mrc20_utxo_"
	iter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "~"),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	// 先收集所有匹配的数据
	for iter.First(); iter.Valid(); iter.Next() {
		var utxo mrc20.Mrc20Utxo
		err := sonic.Unmarshal(iter.Value(), &utxo)
		if err != nil {
			log.Println("Unmarshal mrc20 utxo error:", err)
			continue
		}

		if utxo.Mrc20Id != mrc20Id {
			continue
		}

		allUtxos = append(allUtxos, &utxo)
	}

	// 按时间倒序排序（最新的在前面）
	sort.Slice(allUtxos, func(i, j int) bool {
		return allUtxos[i].Timestamp > allUtxos[j].Timestamp
	})

	// 分页
	if start >= len(allUtxos) {
		return []*mrc20.Mrc20Utxo{}, nil
	}

	end := start + limit
	if limit <= 0 || end > len(allUtxos) {
		end = len(allUtxos)
	}

	return allUtxos[start:end], nil
}

// Mrc20HistoryRecord 历史记录条目（包含方向信息）
type Mrc20HistoryRecord struct {
	TxPoint     string `json:"txPoint"`
	MrcOption   string `json:"mrcOption"`
	Direction   string `json:"direction"` // "in" 或 "out"
	AmtChange   string `json:"amtChange"`
	Status      int    `json:"status"`
	Chain       string `json:"chain"`
	BlockHeight int64  `json:"blockHeight"`
	Timestamp   int64  `json:"timestamp"`
	FromAddress string `json:"fromAddress"`
	ToAddress   string `json:"toAddress"`
	OperationTx string `json:"operationTx"`
}

// GetMrc20AddressHistory 获取某地址在某 tick 的收支流水历史
// 收入：ToAddress == 该地址（收到代币）
// 支出：FromAddress == 该地址（转出代币给别人）
// statusFilter: nil 表示返回所有状态，非 nil 表示只返回指定状态的记录
func (pd *PebbleData) GetMrc20AddressHistory(mrc20Id, address string, start, limit int, statusFilter *int) ([]*mrc20.Mrc20Utxo, int, error) {
	records, total, err := pd.GetMrc20AddressHistoryWithDirection(mrc20Id, address, start, limit, statusFilter)
	if err != nil {
		return nil, 0, err
	}

	// 转换为 Mrc20Utxo 格式（向后兼容）
	var result []*mrc20.Mrc20Utxo
	for _, r := range records {
		utxo := &mrc20.Mrc20Utxo{
			TxPoint:     r.TxPoint,
			MrcOption:   r.MrcOption,
			AmtChange:   decimal.RequireFromString(r.AmtChange),
			Status:      r.Status,
			Chain:       r.Chain,
			BlockHeight: r.BlockHeight,
			Timestamp:   r.Timestamp,
			FromAddress: r.FromAddress,
			ToAddress:   r.ToAddress,
			Mrc20Id:     mrc20Id,
		}
		// Direction 通过 Status 传递：out 方向设为 -1
		if r.Direction == "out" {
			utxo.Status = -1
		}
		result = append(result, utxo)
	}

	return result, total, nil
}

// GetMrc20AddressHistoryWithDirection 获取带方向信息的收支流水历史
func (pd *PebbleData) GetMrc20AddressHistoryWithDirection(mrc20Id, address string, start, limit int, statusFilter *int) ([]*Mrc20HistoryRecord, int, error) {
	var allRecords []*Mrc20HistoryRecord
	recordMap := make(map[string]bool) // 用于去重：key = txPoint + direction

	// 扫描所有该 tick 的 UTXO
	prefix := "mrc20_utxo_"
	iter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "~"),
	})
	if err != nil {
		return nil, 0, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var utxo mrc20.Mrc20Utxo
		err := sonic.Unmarshal(iter.Value(), &utxo)
		if err != nil {
			continue
		}

		// 只处理匹配 tickId 的记录
		if utxo.Mrc20Id != mrc20Id {
			continue
		}

		// 从 TxPoint 提取 txid（格式: txid:vout）
		txid := utxo.TxPoint
		if idx := strings.LastIndex(utxo.TxPoint, ":"); idx > 0 {
			txid = utxo.TxPoint[:idx]
		}

		// 只处理属于该地址的 UTXO（ToAddress == 该地址）
		if utxo.ToAddress != address {
			continue
		}

		// 收入记录：收到这笔代币
		keyIn := utxo.TxPoint + "_in"
		if !recordMap[keyIn] {
			// 应用 statusFilter：如果指定了 status，只返回匹配的记录
			if statusFilter != nil && utxo.Status != *statusFilter {
				// 跳过不匹配的收入记录
			} else {
				recordMap[keyIn] = true
				allRecords = append(allRecords, &Mrc20HistoryRecord{
					TxPoint:     utxo.TxPoint,
					MrcOption:   utxo.MrcOption,
					Direction:   "in",
					AmtChange:   utxo.AmtChange.String(),
					Status:      utxo.Status,
					Chain:       utxo.Chain,
					BlockHeight: utxo.BlockHeight,
					Timestamp:   utxo.Timestamp,
					FromAddress: utxo.FromAddress,
					ToAddress:   utxo.ToAddress,
					OperationTx: txid, // In: 显示创建这笔收入的交易（TxPoint的txid）
				})
			}
		}

		// 支出记录：只有当 Status == -1（已消费）时才显示支出
		if utxo.Status == -1 {
			// 应用 statusFilter：如果指定了 status=-1，才显示支出记录
			// 如果 statusFilter 为其他值（如 0, 1, 2），不显示支出记录
			if statusFilter == nil || *statusFilter == -1 {
				keyOut := utxo.TxPoint + "_out"
				if !recordMap[keyOut] {
					recordMap[keyOut] = true
					allRecords = append(allRecords, &Mrc20HistoryRecord{
						TxPoint:     utxo.TxPoint,
						MrcOption:   utxo.MrcOption,
						Direction:   "out",
						AmtChange:   utxo.AmtChange.String(),
						Status:      utxo.Status,
						Chain:       utxo.Chain,
						BlockHeight: utxo.BlockHeight,
						Timestamp:   utxo.Timestamp,
						FromAddress: utxo.FromAddress,
						ToAddress:   utxo.ToAddress,
						OperationTx: utxo.OperationTx, // Out: 显示消费这笔资产的交易
					})
				}
			}
		}
	}

	total := len(allRecords)

	// 按区块高度倒序排序（最新的在前面）
	sort.Slice(allRecords, func(i, j int) bool {
		if allRecords[i].BlockHeight == allRecords[j].BlockHeight {
			// 同一区块，支出在前
			if allRecords[i].Direction != allRecords[j].Direction {
				return allRecords[i].Direction == "out"
			}
		}
		return allRecords[i].BlockHeight > allRecords[j].BlockHeight
	})

	// 分页
	if start >= len(allRecords) {
		return []*Mrc20HistoryRecord{}, total, nil
	}

	end := start + limit
	if limit <= 0 || end > len(allRecords) {
		end = len(allRecords)
	}

	return allRecords[start:end], total, nil
}

// Mrc20Holder 持有者信息
type Mrc20Holder struct {
	Address string          `json:"address"`
	Balance decimal.Decimal `json:"balance"`
}

// GetMrc20Holders 获取 tick 的持有者列表（包括曾经持有过的）
// 使用 mrc20_in_ 前缀，显示所有曾经持有过代币的地址
func (pd *PebbleData) GetMrc20Holders(tickId string, start, limit int, searchAddress string) ([]Mrc20Holder, error) {
	// 遍历所有地址的 UTXO，找出 mrc20Id 匹配的记录
	balanceMap := make(map[string]decimal.Decimal) // 当前可用余额
	addressSet := make(map[string]bool)            // 所有曾经持有过的地址

	// 遍历 mrc20_in_ 前缀的所有数据
	prefix := "mrc20_in_"
	iter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "~"),
	})
	if err != nil {
		log.Printf("[MRC20] GetMrc20Holders: NewIter error: %v", err)
		return nil, err
	}
	defer iter.Close()

	totalCount := 0
	matchCount := 0
	for iter.First(); iter.Valid(); iter.Next() {
		totalCount++
		var utxo mrc20.Mrc20Utxo
		err := sonic.Unmarshal(iter.Value(), &utxo)
		if err != nil {
			continue
		}

		// 只统计匹配 tickId 的记录
		if utxo.Mrc20Id != tickId {
			continue
		}

		if utxo.ToAddress == "" {
			continue
		}

		// 如果有搜索条件，只统计匹配的地址
		if searchAddress != "" && !strings.Contains(utxo.ToAddress, searchAddress) {
			continue
		}

		matchCount++
		// 记录所有曾经持有过的地址
		addressSet[utxo.ToAddress] = true
		// 只有 Status=0 的才计入可用余额
		if utxo.Status == mrc20.UtxoStatusAvailable {
			balanceMap[utxo.ToAddress] = balanceMap[utxo.ToAddress].Add(utxo.AmtChange)
		}
	}
	log.Printf("[MRC20] GetMrc20Holders: tickId=%s, totalUtxos=%d, matchedUtxos=%d, uniqueAddresses=%d", tickId, totalCount, matchCount, len(addressSet))

	// 转换为列表（包括余额为0的曾持有者）
	var holders []Mrc20Holder
	for addr := range addressSet {
		balance := balanceMap[addr] // 如果没有可用余额，默认为0
		if balance.LessThan(decimal.Zero) {
			balance = decimal.Zero // 不显示负数余额
		}
		holders = append(holders, Mrc20Holder{
			Address: addr,
			Balance: balance,
		})
	}

	// 按余额降序排序
	sort.Slice(holders, func(i, j int) bool {
		return holders[i].Balance.GreaterThan(holders[j].Balance)
	})

	// 分页
	if start >= len(holders) {
		return []Mrc20Holder{}, nil
	}

	end := start + limit
	if end > len(holders) {
		end = len(holders)
	}

	return holders[start:end], nil
}

// GetMrc20HoldersCount 获取持有者总数（包括曾经持有过的）
// 使用 mrc20_in_ 前缀，统计所有曾经持有过的地址
func (pd *PebbleData) GetMrc20HoldersCount(tickId string, searchAddress string) (int, error) {
	addressSet := make(map[string]bool)

	prefix := "mrc20_in_"
	iter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "~"),
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var utxo mrc20.Mrc20Utxo
		err := sonic.Unmarshal(iter.Value(), &utxo)
		if err != nil {
			continue
		}

		if utxo.Mrc20Id != tickId {
			continue
		}

		if utxo.ToAddress == "" {
			continue
		}

		// 搜索过滤
		if searchAddress != "" && !strings.Contains(utxo.ToAddress, searchAddress) {
			continue
		}

		// 记录所有曾经持有过的地址
		addressSet[utxo.ToAddress] = true
	}

	return len(addressSet), nil
}

// ================ Teleport 跃迁相关存储方法 ================

// SaveMrc20Arrival 保存 arrival 记录
func (pd *PebbleData) SaveMrc20Arrival(arrival *mrc20.Mrc20Arrival) error {
	data, err := sonic.Marshal(arrival)
	if err != nil {
		return fmt.Errorf("marshal arrival error: %w", err)
	}

	batch := pd.Database.MrcDb.NewBatch()
	defer batch.Close()

	// 主键: arrival_{pinId}
	key := fmt.Sprintf("arrival_%s", arrival.PinId)
	err = batch.Set([]byte(key), data, pebble.Sync)
	if err != nil {
		return fmt.Errorf("save arrival error: %w", err)
	}

	// 索引: arrival_asset_{assetOutpoint} - 用于 teleport 快速查找
	assetKey := fmt.Sprintf("arrival_asset_%s", arrival.AssetOutpoint)
	err = batch.Set([]byte(assetKey), []byte(arrival.PinId), pebble.Sync)
	if err != nil {
		return fmt.Errorf("save arrival asset index error: %w", err)
	}

	// 索引: arrival_pending_{chain}_{tickId}_{pinId} - 用于列出待处理的 arrival
	if arrival.Status == mrc20.ArrivalStatusPending {
		pendingKey := fmt.Sprintf("arrival_pending_%s_%s_%s", arrival.Chain, arrival.TickId, arrival.PinId)
		err = batch.Set([]byte(pendingKey), []byte(arrival.PinId), pebble.Sync)
		if err != nil {
			return fmt.Errorf("save arrival pending index error: %w", err)
		}
	}

	return batch.Commit(pebble.Sync)
}

// GetMrc20ArrivalByPinId 根据 PIN ID 获取 arrival 记录 (coord 查询)
func (pd *PebbleData) GetMrc20ArrivalByPinId(pinId string) (*mrc20.Mrc20Arrival, error) {
	key := fmt.Sprintf("arrival_%s", pinId)
	value, closer, err := pd.Database.MrcDb.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	var arrival mrc20.Mrc20Arrival
	err = sonic.Unmarshal(value, &arrival)
	if err != nil {
		return nil, fmt.Errorf("unmarshal arrival error: %w", err)
	}

	return &arrival, nil
}

// GetMrc20ArrivalByAssetOutpoint 根据 assetOutpoint 获取 arrival 记录
func (pd *PebbleData) GetMrc20ArrivalByAssetOutpoint(assetOutpoint string) (*mrc20.Mrc20Arrival, error) {
	// 先查找索引
	assetKey := fmt.Sprintf("arrival_asset_%s", assetOutpoint)
	pinIdBytes, closer, err := pd.Database.MrcDb.Get([]byte(assetKey))
	if err != nil {
		return nil, err
	}
	closer.Close()

	// 再查找 arrival 数据
	return pd.GetMrc20ArrivalByPinId(string(pinIdBytes))
}

// UpdateMrc20ArrivalStatus 更新 arrival 状态（跃迁完成时调用）
func (pd *PebbleData) UpdateMrc20ArrivalStatus(pinId string, status mrc20.ArrivalStatus, teleportPinId, teleportChain, teleportTxId string, completedAt int64) error {
	arrival, err := pd.GetMrc20ArrivalByPinId(pinId)
	if err != nil {
		return fmt.Errorf("get arrival error: %w", err)
	}

	arrival.Status = status
	arrival.TeleportPinId = teleportPinId
	arrival.TeleportChain = teleportChain
	arrival.TeleportTxId = teleportTxId
	arrival.CompletedAt = completedAt

	data, err := sonic.Marshal(arrival)
	if err != nil {
		return fmt.Errorf("marshal arrival error: %w", err)
	}

	batch := pd.Database.MrcDb.NewBatch()
	defer batch.Close()

	// 更新主数据
	key := fmt.Sprintf("arrival_%s", pinId)
	err = batch.Set([]byte(key), data, pebble.Sync)
	if err != nil {
		return fmt.Errorf("update arrival error: %w", err)
	}

	// 如果状态不再是 pending，删除 pending 索引
	if status != mrc20.ArrivalStatusPending {
		pendingKey := fmt.Sprintf("arrival_pending_%s_%s_%s", arrival.Chain, arrival.TickId, pinId)
		err = batch.Delete([]byte(pendingKey), pebble.Sync)
		if err != nil && err != pebble.ErrNotFound {
			log.Println("delete arrival pending index error:", err)
		}
	}

	return batch.Commit(pebble.Sync)
}

// GetPendingArrivals 获取待处理的 arrival 列表
func (pd *PebbleData) GetPendingArrivals(chain, tickId string, limit int) ([]*mrc20.Mrc20Arrival, error) {
	var result []*mrc20.Mrc20Arrival

	prefix := fmt.Sprintf("arrival_pending_%s_%s_", chain, tickId)
	iter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "~"),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		if limit > 0 && count >= limit {
			break
		}

		pinId := string(iter.Value())
		arrival, err := pd.GetMrc20ArrivalByPinId(pinId)
		if err != nil {
			log.Println("get arrival error:", err)
			continue
		}

		result = append(result, arrival)
		count++
	}

	return result, nil
}

// SaveMrc20Teleport 保存 teleport 记录
func (pd *PebbleData) SaveMrc20Teleport(teleport *mrc20.Mrc20Teleport) error {
	data, err := sonic.Marshal(teleport)
	if err != nil {
		return fmt.Errorf("marshal teleport error: %w", err)
	}

	batch := pd.Database.MrcDb.NewBatch()
	defer batch.Close()

	// 主键: teleport_{pinId}
	key := fmt.Sprintf("teleport_%s", teleport.PinId)
	err = batch.Set([]byte(key), data, pebble.Sync)
	if err != nil {
		return fmt.Errorf("save teleport error: %w", err)
	}

	// 索引: teleport_coord_{coord} - 通过 arrival pinId 查找 teleport
	coordKey := fmt.Sprintf("teleport_coord_%s", teleport.Coord)
	err = batch.Set([]byte(coordKey), []byte(teleport.PinId), pebble.Sync)
	if err != nil {
		return fmt.Errorf("save teleport coord index error: %w", err)
	}

	// 索引: teleport_asset_{assetOutpoint} - 通过源 UTXO 查找 teleport
	if teleport.SpentUtxoPoint != "" {
		assetKey := fmt.Sprintf("teleport_asset_%s", teleport.SpentUtxoPoint)
		err = batch.Set([]byte(assetKey), []byte(teleport.PinId), pebble.Sync)
		if err != nil {
			return fmt.Errorf("save teleport asset index error: %w", err)
		}
	}

	return batch.Commit(pebble.Sync)
}

// GetMrc20TeleportByPinId 根据 PIN ID 获取 teleport 记录
func (pd *PebbleData) GetMrc20TeleportByPinId(pinId string) (*mrc20.Mrc20Teleport, error) {
	key := fmt.Sprintf("teleport_%s", pinId)
	value, closer, err := pd.Database.MrcDb.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	var teleport mrc20.Mrc20Teleport
	err = sonic.Unmarshal(value, &teleport)
	if err != nil {
		return nil, fmt.Errorf("unmarshal teleport error: %w", err)
	}

	return &teleport, nil
}

// GetMrc20TeleportByCoord 根据 coord (arrival pinId) 获取 teleport 记录
func (pd *PebbleData) GetMrc20TeleportByCoord(coord string) (*mrc20.Mrc20Teleport, error) {
	coordKey := fmt.Sprintf("teleport_coord_%s", coord)
	pinIdBytes, closer, err := pd.Database.MrcDb.Get([]byte(coordKey))
	if err != nil {
		return nil, err
	}
	closer.Close()

	return pd.GetMrc20TeleportByPinId(string(pinIdBytes))
}

// CheckTeleportExists 检查某个 arrival 是否已经有对应的 teleport
func (pd *PebbleData) CheckTeleportExists(coord string) bool {
	coordKey := fmt.Sprintf("teleport_coord_%s", coord)
	_, closer, err := pd.Database.MrcDb.Get([]byte(coordKey))
	if err == nil {
		closer.Close()
		return true
	}
	return false
}

// CheckTeleportExistsByAssetOutpoint 检查某个 assetOutpoint 是否已经有对应的 teleport
func (pd *PebbleData) CheckTeleportExistsByAssetOutpoint(assetOutpoint string) bool {
	assetKey := fmt.Sprintf("teleport_asset_%s", assetOutpoint)
	_, closer, err := pd.Database.MrcDb.Get([]byte(assetKey))
	if err == nil {
		closer.Close()
		return true
	}
	return false
}

// GetMrc20UtxoByTxPoint 根据 txPoint 获取单个 UTXO
func (pd *PebbleData) GetMrc20UtxoByTxPoint(txPoint string, checkStatus bool) (*mrc20.Mrc20Utxo, error) {
	key := fmt.Sprintf("mrc20_utxo_%s", txPoint)
	value, closer, err := pd.Database.MrcDb.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	var utxo mrc20.Mrc20Utxo
	err = sonic.Unmarshal(value, &utxo)
	if err != nil {
		return nil, fmt.Errorf("unmarshal mrc20 utxo error: %w", err)
	}

	// 如果检查状态，只返回可用的 UTXO
	if checkStatus && utxo.Status == -1 {
		return nil, fmt.Errorf("utxo already spent")
	}

	return &utxo, nil
}

// ============== PendingTeleport 相关方法 ==============

// SavePendingTeleport 保存等待 arrival 的 teleport transfer
func (pd *PebbleData) SavePendingTeleport(pending *mrc20.PendingTeleport) error {
	data, err := sonic.Marshal(pending)
	if err != nil {
		return fmt.Errorf("marshal pending teleport error: %w", err)
	}

	batch := pd.Database.MrcDb.NewBatch()
	defer batch.Close()

	// 主键: pending_teleport_{pinId}
	key := fmt.Sprintf("pending_teleport_%s", pending.PinId)
	err = batch.Set([]byte(key), data, pebble.Sync)
	if err != nil {
		return fmt.Errorf("save pending teleport error: %w", err)
	}

	// 索引: pending_teleport_coord_{coord} - 通过期望的 arrival pinId 查找
	coordKey := fmt.Sprintf("pending_teleport_coord_%s", pending.Coord)
	err = batch.Set([]byte(coordKey), []byte(pending.PinId), pebble.Sync)
	if err != nil {
		return fmt.Errorf("save pending teleport coord index error: %w", err)
	}

	return batch.Commit(pebble.Sync)
}

// GetPendingTeleportByCoord 根据 coord (期望的 arrival pinId) 获取等待的 teleport
func (pd *PebbleData) GetPendingTeleportByCoord(coord string) (*mrc20.PendingTeleport, error) {
	coordKey := fmt.Sprintf("pending_teleport_coord_%s", coord)
	pinIdBytes, closer, err := pd.Database.MrcDb.Get([]byte(coordKey))
	if err != nil {
		return nil, err
	}
	closer.Close()

	return pd.GetPendingTeleportByPinId(string(pinIdBytes))
}

// GetPendingTeleportByPinId 根据 PIN ID 获取等待的 teleport
func (pd *PebbleData) GetPendingTeleportByPinId(pinId string) (*mrc20.PendingTeleport, error) {
	key := fmt.Sprintf("pending_teleport_%s", pinId)
	value, closer, err := pd.Database.MrcDb.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	var pending mrc20.PendingTeleport
	err = sonic.Unmarshal(value, &pending)
	if err != nil {
		return nil, fmt.Errorf("unmarshal pending teleport error: %w", err)
	}

	return &pending, nil
}

// DeletePendingTeleport 删除已完成的 pending teleport
func (pd *PebbleData) DeletePendingTeleport(pinId, coord string) error {
	batch := pd.Database.MrcDb.NewBatch()
	defer batch.Close()

	key := fmt.Sprintf("pending_teleport_%s", pinId)
	err := batch.Delete([]byte(key), pebble.Sync)
	if err != nil {
		return fmt.Errorf("delete pending teleport error: %w", err)
	}

	coordKey := fmt.Sprintf("pending_teleport_coord_%s", coord)
	err = batch.Delete([]byte(coordKey), pebble.Sync)
	if err != nil {
		return fmt.Errorf("delete pending teleport coord index error: %w", err)
	}

	return batch.Commit(pebble.Sync)
}

// GetAllPendingTeleports 获取所有等待的 teleport（用于定期重试）
func (pd *PebbleData) GetAllPendingTeleports() ([]*mrc20.PendingTeleport, error) {
	var result []*mrc20.PendingTeleport

	iter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
		LowerBound: []byte("pending_teleport_"),
		UpperBound: []byte("pending_teleport_~"),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		// 跳过 coord 索引
		if strings.Contains(key, "_coord_") {
			continue
		}

		var pending mrc20.PendingTeleport
		err := sonic.Unmarshal(iter.Value(), &pending)
		if err != nil {
			continue
		}
		result = append(result, &pending)
	}

	return result, nil
}

// ============== TeleportPendingIn 相关方法 (用于跟踪接收方的 PendingInBalance) ==============

// SaveTeleportPendingIn 保存 teleport 接收方的 pending 余额记录
func (pd *PebbleData) SaveTeleportPendingIn(pendingIn *mrc20.TeleportPendingIn) error {
	data, err := sonic.Marshal(pendingIn)
	if err != nil {
		return fmt.Errorf("marshal teleport pending in error: %w", err)
	}

	batch := pd.Database.MrcDb.NewBatch()
	defer batch.Close()

	// 主键: teleport_pending_in_{coord}
	key := fmt.Sprintf("teleport_pending_in_%s", pendingIn.Coord)
	err = batch.Set([]byte(key), data, pebble.Sync)
	if err != nil {
		return fmt.Errorf("save teleport pending in error: %w", err)
	}

	// 索引: teleport_pending_in_addr_{toAddress}_{coord} - 用于按地址查询
	addrKey := fmt.Sprintf("teleport_pending_in_addr_%s_%s", pendingIn.ToAddress, pendingIn.Coord)
	err = batch.Set([]byte(addrKey), []byte(pendingIn.Coord), pebble.Sync)
	if err != nil {
		return fmt.Errorf("save teleport pending in address index error: %w", err)
	}

	return batch.Commit(pebble.Sync)
}

// GetTeleportPendingInByCoord 根据 coord 获取 pending in 记录
func (pd *PebbleData) GetTeleportPendingInByCoord(coord string) (*mrc20.TeleportPendingIn, error) {
	key := fmt.Sprintf("teleport_pending_in_%s", coord)
	value, closer, err := pd.Database.MrcDb.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	var pendingIn mrc20.TeleportPendingIn
	err = sonic.Unmarshal(value, &pendingIn)
	if err != nil {
		return nil, fmt.Errorf("unmarshal teleport pending in error: %w", err)
	}

	return &pendingIn, nil
}

// GetTeleportPendingInByAddress 获取指定地址的所有 pending in 记录 (用于计算 PendingInBalance)
func (pd *PebbleData) GetTeleportPendingInByAddress(address string) ([]*mrc20.TeleportPendingIn, error) {
	var result []*mrc20.TeleportPendingIn

	prefix := fmt.Sprintf("teleport_pending_in_addr_%s_", address)
	iter, err := pd.Database.MrcDb.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "~"),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		coord := string(iter.Value())
		pendingIn, err := pd.GetTeleportPendingInByCoord(coord)
		if err != nil {
			log.Println("GetTeleportPendingInByAddress: get pending in error:", err)
			continue
		}
		result = append(result, pendingIn)
	}

	return result, nil
}

// DeleteTeleportPendingIn 删除 pending in 记录 (跃迁完成时调用)
func (pd *PebbleData) DeleteTeleportPendingIn(coord, toAddress string) error {
	batch := pd.Database.MrcDb.NewBatch()
	defer batch.Close()

	// 删除主记录
	key := fmt.Sprintf("teleport_pending_in_%s", coord)
	err := batch.Delete([]byte(key), pebble.Sync)
	if err != nil && err != pebble.ErrNotFound {
		return fmt.Errorf("delete teleport pending in error: %w", err)
	}

	// 删除地址索引
	addrKey := fmt.Sprintf("teleport_pending_in_addr_%s_%s", toAddress, coord)
	err = batch.Delete([]byte(addrKey), pebble.Sync)
	if err != nil && err != pebble.ErrNotFound {
		return fmt.Errorf("delete teleport pending in address index error: %w", err)
	}

	return batch.Commit(pebble.Sync)
}
