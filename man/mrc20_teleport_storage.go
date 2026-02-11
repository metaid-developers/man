package man

import (
	"fmt"
	"manindexer/mrc20"
	"time"

	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"
)

// SaveTeleportTransaction 保存 TeleportTransaction
func SaveTeleportTransaction(tx *mrc20.TeleportTransaction) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}

	data, err := sonic.Marshal(tx)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	key := fmt.Sprintf("teleport_tx_v2_%s", tx.ID)
	err = PebbleStore.Database.MrcDb.Set([]byte(key), data, pebble.Sync)
	if err != nil {
		return fmt.Errorf("save failed: %w", err)
	}

	return nil
}

// LoadTeleportTransaction 加载 TeleportTransaction
func LoadTeleportTransaction(teleportID string) (*mrc20.TeleportTransaction, error) {
	key := fmt.Sprintf("teleport_tx_v2_%s", teleportID)
	value, closer, err := PebbleStore.Database.MrcDb.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	var tx mrc20.TeleportTransaction
	if err := sonic.Unmarshal(value, &tx); err != nil {
		return nil, fmt.Errorf("unmarshal failed: %w", err)
	}

	return &tx, nil
}

// DeleteTeleportTransaction 删除 TeleportTransaction
func DeleteTeleportTransaction(teleportID string) error {
	key := fmt.Sprintf("teleport_tx_v2_%s", teleportID)
	return PebbleStore.Database.MrcDb.Delete([]byte(key), pebble.Sync)
}

// ListPendingTeleportTransactions 列出所有待处理的 TeleportTransaction
func ListPendingTeleportTransactions() ([]*mrc20.TeleportTransaction, error) {
	prefix := []byte("teleport_tx_v2_")
	iter, err := PebbleStore.Database.MrcDb.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var result []*mrc20.TeleportTransaction
	for iter.First(); iter.Valid(); iter.Next() {
		var tx mrc20.TeleportTransaction
		if err := sonic.Unmarshal(iter.Value(), &tx); err != nil {
			continue
		}

		// 只返回未完成的
		if !mrc20.IsTerminalState(tx.State) {
			result = append(result, &tx)
		}
	}

	return result, nil
}

// ListAllTeleportTransactions 列出所有 TeleportTransaction（包括已完成）
func ListAllTeleportTransactions(limit int) ([]*mrc20.TeleportTransaction, error) {
	prefix := []byte("teleport_tx_v2_")
	iter, err := PebbleStore.Database.MrcDb.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var result []*mrc20.TeleportTransaction
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		if limit > 0 && count >= limit {
			break
		}

		var tx mrc20.TeleportTransaction
		if err := sonic.Unmarshal(iter.Value(), &tx); err != nil {
			continue
		}

		result = append(result, &tx)
		count++
	}

	return result, nil
}

// GetTeleportTransactionByCoord 通过 Coord 查找 TeleportTransaction
func GetTeleportTransactionByCoord(coord string) (*mrc20.TeleportTransaction, error) {
	// 遍历所有 TeleportTransaction，查找匹配的 Coord
	// 注意：这个查询效率较低，如果需要频繁使用，应该建立索引
	prefix := []byte("teleport_tx_v2_")
	iter, err := PebbleStore.Database.MrcDb.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var tx mrc20.TeleportTransaction
		if err := sonic.Unmarshal(iter.Value(), &tx); err != nil {
			continue
		}

		if tx.Coord == coord {
			return &tx, nil
		}
	}

	return nil, fmt.Errorf("teleport transaction not found for coord: %s", coord)
}

// RetryStuckTeleports 重试所有卡住的 Teleport
// 这个函数在每个区块处理后调用
func RetryStuckTeleports() error {
	pendingTxs, err := ListPendingTeleportTransactions()
	if err != nil {
		return err
	}

	for _, tx := range pendingTxs {
		// 检查是否应该重试
		if !tx.ShouldRetry() {
			continue
		}

		// 检查锁
		if tx.IsLocked() {
			continue
		}

		// 尝试获取锁
		if !tx.AcquireLock(ProcessID, 5*time.Minute) {
			continue
		}

		// 增加重试计数
		tx.RetryCount++
		tx.LastRetryAt = time.Now().Unix()

		// 重新执行状态机
		// 注意：这里需要重新加载 PIN 数据，暂时跳过
		// TODO: 实现完整的重试逻辑

		tx.ReleaseLock(ProcessID)
		SaveTeleportTransaction(tx)
	}

	return nil
}
