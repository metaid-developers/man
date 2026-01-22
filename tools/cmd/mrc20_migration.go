package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"manindexer/mrc20"
	"manindexer/pebblestore"

	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	mongoURI       = flag.String("mongo", "mongodb://localhost:27017", "MongoDB connection URI")
	dbName         = flag.String("db", "metaso", "MongoDB database name")
	pebbleDir      = flag.String("pebble", "./man_base_data_pebble", "PebbleDB directory")
	chainName      = flag.String("chain", "btc", "Chain name (btc/mvc/doge)")
	continueHeight = flag.Int64("height", 0, "Block height to continue indexing from (set mrc20_sync_height)")
	endHeight      = flag.Int64("end-height", 0, "Only migrate data with blockheight < end-height, and auto-reset UTXOs spent after this height")
	dryRun         = flag.Bool("dry-run", false, "Dry run mode, only show statistics")
	batchSize      = flag.Int("batch", 1000, "Batch size for import")
)

type MigrationStats struct {
	UtxoCount      int64
	TickCount      int64
	ShovelCount    int64
	AddressCount   int64
	OperationCount int64
	StartTime      time.Time
	EndTime        time.Time
}

func main() {
	flag.Parse()

	if *continueHeight <= 0 {
		log.Fatal("Please specify --height to set the starting block height for new indexing")
	}

	log.Printf("=== MRC20 Data Migration Tool ===")
	log.Printf("MongoDB: %s/%s", *mongoURI, *dbName)
	log.Printf("PebbleDB: %s", *pebbleDir)
	log.Printf("Chain: %s", *chainName)
	log.Printf("Continue from height: %d", *continueHeight)
	if *endHeight > 0 {
		log.Printf("End height filter: < %d (only migrate data before this height)", *endHeight)
		log.Printf("UTXO status reset: auto (UTXOs spent at height >= %d will be reset to status=0)", *endHeight)
	}
	log.Printf("Dry run: %v", *dryRun)
	log.Printf("=================================\n")

	stats := &MigrationStats{StartTime: time.Now()}

	// 1. Connect to MongoDB
	mongoClient, err := connectMongo()
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer mongoClient.Disconnect(context.Background())

	// 2. Open PebbleDB
	var pebbleDB *pebblestore.Database
	if !*dryRun {
		pebbleDB, err = openPebbleDB()
		if err != nil {
			log.Fatalf("Failed to open PebbleDB: %v", err)
		}
		defer pebbleDB.Close()
	}

	// 3. Export and import data
	log.Println("\n[1/5] Migrating MRC20 Tick (token info)...")
	if err := migrateTickData(mongoClient, pebbleDB, stats); err != nil {
		log.Fatalf("Failed to migrate tick data: %v", err)
	}

	log.Println("\n[2/5] Migrating MRC20 UTXO (balances)...")
	if err := migrateUtxoData(mongoClient, pebbleDB, stats); err != nil {
		log.Fatalf("Failed to migrate UTXO data: %v", err)
	}

	log.Println("\n[3/5] Migrating MRC20 Shovel (used PINs)...")
	if err := migrateShovelData(mongoClient, pebbleDB, stats); err != nil {
		log.Fatalf("Failed to migrate shovel data: %v", err)
	}

	log.Println("\n[4/5] Migrating MRC20 Operation TX...")
	if err := migrateOperationTx(mongoClient, pebbleDB, stats); err != nil {
		log.Fatalf("Failed to migrate operation tx: %v", err)
	}

	log.Println("\n[5/5] Setting MRC20 index height...")
	if !*dryRun && *continueHeight > 0 {
		if err := setIndexHeight(pebbleDB); err != nil {
			log.Fatalf("Failed to set index height: %v", err)
		}
	} else if *continueHeight == 0 {
		log.Printf("⚠ Skipping height setting (continueHeight not specified)")
	}

	stats.EndTime = time.Now()
	printStats(stats)
}

func connectMongo() (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(*mongoURI))
	if err != nil {
		return nil, err
	}

	// Test connection
	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	log.Println("✓ Connected to MongoDB")
	return client, nil
}

func openPebbleDB() (*pebblestore.Database, error) {
	if _, err := os.Stat(*pebbleDir); os.IsNotExist(err) {
		if err := os.MkdirAll(*pebbleDir, 0755); err != nil {
			return nil, err
		}
	}

	db, err := pebblestore.NewDataBase(*pebbleDir, 16)
	if err != nil {
		return nil, err
	}

	log.Println("✓ Opened PebbleDB")
	return db, nil
}

func migrateTickData(mongoClient *mongo.Client, pebbleDB *pebblestore.Database, stats *MigrationStats) error {
	// 实际集合名是 mrc20ticks（不是 mrc20_tick）
	collection := mongoClient.Database(*dbName).Collection("mrc20ticks")

	// 构建查询条件
	// 注意：Tick 数据通常没有 blockheight 字段（为 null），所以不对 tick 应用 end-height 过滤
	// Tick 是 deploy 操作产生的元数据，需要全部迁移才能保证 UTXO 数据的完整性
	filter := bson.M{"chain": *chainName}
	log.Printf("  Note: Tick data migration ignores --end-height filter (tick blockheight is usually null)")

	cursor, err := collection.Find(context.Background(), filter)
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background())

	var batch *pebble.Batch
	if !*dryRun {
		batch = pebbleDB.MrcDb.NewBatch()
	}
	count := int64(0)

	for cursor.Next(context.Background()) {
		var tick map[string]interface{}
		if err := cursor.Decode(&tick); err != nil {
			log.Printf("Warning: failed to decode tick: %v", err)
			continue
		}

		if *dryRun {
			count++
			continue
		}

		// Convert to mrc20.Mrc20DeployInfo
		tickData, err := convertTickData(tick)
		if err != nil {
			log.Printf("Warning: failed to convert tick %v: %v", tick["_id"], err)
			continue
		}

		// Save to PebbleDB
		tickBytes, _ := sonic.Marshal(tickData)
		tickId := tickData.Mrc20Id

		// Save tick by ID
		key := []byte("mrc20_tick_" + tickId)
		batch.Set(key, tickBytes, pebble.Sync)

		// Save tick name index
		nameKey := []byte("mrc20_tick_name_" + tickData.Tick)
		batch.Set(nameKey, []byte(tickId), pebble.Sync)

		count++
		if count%100 == 0 {
			if err := batch.Commit(pebble.Sync); err != nil {
				return err
			}
			batch = pebbleDB.MrcDb.NewBatch()
			fmt.Printf("\r  Processed %d ticks...", count)
		}
	}

	if !*dryRun && batch.Count() > 0 {
		if err := batch.Commit(pebble.Sync); err != nil {
			return err
		}
	}

	stats.TickCount = count
	log.Printf("\n✓ Migrated %d ticks", count)
	return nil
}

// buildOperationTxHeightMap 构建 operationTx -> blockHeight 的映射
// 用于判断已消费的 UTXO 是否需要重置状态
// 注意：需要从 operationTx 产生的输出 UTXO 获取高度，而不是被消费的输入 UTXO
func buildOperationTxHeightMap(mongoClient *mongo.Client) (map[string]int64, error) {
	result := make(map[string]int64)

	// 从 mrc20utxos 集合中获取所有记录
	// 通过 txpoint 的前缀（txid）来确定该交易发生的实际高度
	collection := mongoClient.Database(*dbName).Collection("mrc20utxos")
	filter := bson.M{
		"chain": *chainName,
	}

	// 只获取 txpoint 和 blockheight 字段
	projection := bson.M{"txpoint": 1, "blockheight": 1}
	cursor, err := collection.Find(context.Background(), filter, options.Find().SetProjection(projection))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		var doc struct {
			TxPoint     string `bson:"txpoint"`
			BlockHeight int64  `bson:"blockheight"`
		}
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		if doc.TxPoint != "" && doc.BlockHeight > 0 {
			// 从 txpoint 提取 txid（格式：txid:index）
			parts := strings.Split(doc.TxPoint, ":")
			if len(parts) >= 1 {
				txid := parts[0]
				// 存储 txid 对应的区块高度
				// 这是该交易实际发生时的高度
				result[txid] = doc.BlockHeight
			}
		}
	}

	return result, nil
}

func migrateUtxoData(mongoClient *mongo.Client, pebbleDB *pebblestore.Database, stats *MigrationStats) error {
	// 实际集合名是 mrc20utxos（不是 mrc20_utxo）
	collection := mongoClient.Database(*dbName).Collection("mrc20utxos")

	// 如果指定了 end-height，先构建 operationTx -> blockHeight 映射
	var opTxHeightMap map[string]int64
	if *endHeight > 0 {
		log.Printf("  Building operationTx height map for smart reset...")
		var err error
		opTxHeightMap, err = buildOperationTxHeightMap(mongoClient)
		if err != nil {
			log.Printf("  Warning: failed to build operationTx height map: %v", err)
			opTxHeightMap = make(map[string]int64)
		}
		log.Printf("  Found %d operation transactions", len(opTxHeightMap))
	}

	// 构建查询条件
	filter := bson.M{"chain": *chainName}
	if *endHeight > 0 {
		// 只迁移 blockheight < endHeight 的数据
		filter["blockheight"] = bson.M{"$lt": *endHeight}
		log.Printf("  Filtering UTXO with blockheight < %d", *endHeight)
	}

	// 获取所有 UTXO（包括已花费的）
	// 注意：status=-1 表示已花费，但仍需存储用于历史查询
	// verify=false 的记录是无效操作，仍然需要存储以防止重复处理
	cursor, err := collection.Find(context.Background(), filter)
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background())

	var batch *pebble.Batch
	if !*dryRun {
		batch = pebbleDB.MrcDb.NewBatch()
	}

	// 统计
	count := int64(0)
	spentCount := int64(0)   // status=-1 的数量
	unspentCount := int64(0) // status=0 的数量
	invalidCount := int64(0) // verify=false 的数量
	resetCount := int64(0)   // 重置状态的数量
	addressMap := make(map[string]bool)
	shovelMap := make(map[string]string)  // mrc20id_pinid -> mintPinId
	operationMap := make(map[string]bool) // Track operation tx

	for cursor.Next(context.Background()) {
		var utxo map[string]interface{}
		if err := cursor.Decode(&utxo); err != nil {
			log.Printf("Warning: failed to decode utxo: %v", err)
			continue
		}

		// 统计验证状态
		if verify, ok := utxo["verify"].(bool); ok && !verify {
			invalidCount++
		}

		// 统计花费状态
		if status, ok := utxo["status"]; ok {
			statusInt := toInt(status)
			if statusInt == -1 {
				spentCount++
			} else {
				unspentCount++
			}
		}

		// 提取 shovel 数据（mint 操作使用的 PIN）
		// 只有 mint 操作才有 shovel，并且需要 verify=true
		if mrcoption, ok := utxo["mrcoption"].(string); ok && mrcoption == "mint" {
			if verify, ok := utxo["verify"].(bool); ok && verify {
				if mrc20id, ok := utxo["mrc20id"].(string); ok && mrc20id != "" {
					if pinid, ok := utxo["pinid"].(string); ok && pinid != "" {
						shovelKey := mrc20id + "_" + pinid
						// 存储实际的 mint PIN ID
						shovelMap[shovelKey] = pinid
					}
				}
			}
		}

		// 提取 operation tx
		// 只添加 height < end-height 的 operationTx
		// 这样 height >= end-height 的交易可以被重新索引
		if optx, ok := utxo["operationtx"].(string); ok && optx != "" {
			if *endHeight > 0 {
				// 检查 operationTx 的高度是否 < end-height
				if opHeight, ok := opTxHeightMap[optx]; ok && opHeight < *endHeight {
					operationMap[optx] = true
				}
				// 如果 opHeight >= endHeight，不添加到 operationMap，让索引器重新处理
			} else {
				// 没有 end-height 限制时，添加所有
				operationMap[optx] = true
			}
		}

		// 收集地址统计
		if addr, ok := utxo["toaddress"].(string); ok && addr != "" {
			addressMap[addr] = true
		}

		if *dryRun {
			count++
			continue
		}

		// Convert to mrc20.Mrc20Utxo
		utxoData, err := convertUtxoData(utxo)
		if err != nil {
			log.Printf("Warning: failed to convert utxo: %v", err)
			continue
		}

		// 智能重置已消费 UTXO 的状态
		// 只有当 operationTx 的区块高度 >= end-height 时才重置
		// 这样可以保证：
		// - 在 end-height 之前被消费的 UTXO 保持 status=-1
		// - 在 end-height 之后被消费的 UTXO 重置为 status=0，让索引器重新处理
		if *endHeight > 0 && utxoData.Status == -1 && utxoData.OperationTx != "" {
			if opHeight, ok := opTxHeightMap[utxoData.OperationTx]; ok && opHeight >= *endHeight {
				utxoData.Status = 0
				utxoData.OperationTx = "" // 清除操作交易，让索引器重新设置
				resetCount++
			}
		}

		// Save to PebbleDB
		utxoBytes, _ := sonic.Marshal(utxoData)

		// Save UTXO by TxPoint
		key := []byte("mrc20_utxo_" + utxoData.TxPoint)
		batch.Set(key, utxoBytes, pebble.Sync)

		// 双前缀地址索引:
		// - mrc20_in_{ToAddress}: 收入记录，用于余额计算
		// - mrc20_out_{ToAddress}: 支出记录，仅在 Status=-1 时创建

		// 收入索引：所有记录都写入 mrc20_in_{ToAddress}
		if utxoData.ToAddress != "" {
			inKey := fmt.Sprintf("mrc20_in_%s_%s_%s",
				utxoData.ToAddress, utxoData.Mrc20Id, utxoData.TxPoint)
			batch.Set([]byte(inKey), utxoBytes, pebble.Sync)
		}

		// 支出索引：仅当 Status=-1（已消耗）时写入 mrc20_out_{ToAddress}
		if utxoData.Status == -1 && utxoData.ToAddress != "" {
			outKey := fmt.Sprintf("mrc20_out_%s_%s_%s",
				utxoData.ToAddress, utxoData.Mrc20Id, utxoData.TxPoint)
			batch.Set([]byte(outKey), utxoBytes, pebble.Sync)
		}

		count++
		if count%int64(*batchSize) == 0 {
			if err := batch.Commit(pebble.Sync); err != nil {
				return err
			}
			batch = pebbleDB.MrcDb.NewBatch()
			fmt.Printf("\r  Processed %d UTXOs...", count)
		}
	}

	if !*dryRun && batch.Count() > 0 {
		if err := batch.Commit(pebble.Sync); err != nil {
			return err
		}
	}

	// 保存 shovel 数据（从 UTXO 中提取的 mint PIN）
	// Shovel 需要存储完整结构
	if !*dryRun && len(shovelMap) > 0 {
		shovelBatch := pebbleDB.MrcDb.NewBatch()
		for shovelKey, mintPinId := range shovelMap {
			parts := strings.Split(shovelKey, "_")
			if len(parts) < 2 {
				continue
			}
			pinId := parts[len(parts)-1]
			mrc20Id := strings.Join(parts[:len(parts)-1], "_")

			shovel := mrc20.Mrc20Shovel{
				Id:           pinId,
				Mrc20MintPin: mintPinId,
			}
			data, _ := sonic.Marshal(shovel)

			key := []byte("mrc20_shovel_" + mrc20Id + "_" + pinId)
			shovelBatch.Set(key, data, pebble.Sync)
		}
		if err := shovelBatch.Commit(pebble.Sync); err != nil {
			log.Printf("Warning: failed to save shovel data: %v", err)
		}
	}

	// 保存 operation tx 数据（从 UTXO 中提取）
	if !*dryRun && len(operationMap) > 0 {
		opBatch := pebbleDB.MrcDb.NewBatch()
		for opTx := range operationMap {
			key := []byte("mrc20_op_tx_" + opTx)
			opBatch.Set(key, []byte("1"), pebble.Sync)
		}
		if err := opBatch.Commit(pebble.Sync); err != nil {
			log.Printf("Warning: failed to save operation tx data: %v", err)
		}
	}

	stats.UtxoCount = count
	stats.AddressCount = int64(len(addressMap))
	stats.ShovelCount = int64(len(shovelMap))
	stats.OperationCount = int64(len(operationMap))

	log.Printf("\n✓ Migrated %d UTXOs total:", count)
	log.Printf("  → Unspent (status=0):  %d", unspentCount)
	log.Printf("  → Spent (status=-1):   %d", spentCount)
	if resetCount > 0 {
		log.Printf("  → Reset for re-index:  %d (operationTx height >= %d)", resetCount, *endHeight)
	}
	log.Printf("  → Invalid (verify=false): %d", invalidCount)
	log.Printf("  → Unique addresses:    %d", len(addressMap))
	log.Printf("  → Extracted shovels:   %d", len(shovelMap))
	log.Printf("  → Extracted op txs:    %d", len(operationMap))
	return nil
}

func migrateShovelData(mongoClient *mongo.Client, pebbleDB *pebblestore.Database, stats *MigrationStats) error {
	// Shovel 数据已经在 migrateUtxoData 中从 UTXO 记录提取
	// 这里尝试从独立集合补充数据（如果存在的话）
	collection := mongoClient.Database(*dbName).Collection("mrc20_shovel")
	countDocs, err := collection.CountDocuments(context.Background(), bson.M{})
	if err != nil || countDocs == 0 {
		log.Println("ℹ Collection mrc20_shovel not found - shovel data extracted from UTXOs above")
		// stats.ShovelCount 已在 migrateUtxoData 中设置
		return nil
	}

	cursor, err := collection.Find(context.Background(), bson.M{"chain": *chainName})
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background())

	var batch *pebble.Batch
	if !*dryRun {
		batch = pebbleDB.MrcDb.NewBatch()
	}
	count := int64(0)

	for cursor.Next(context.Background()) {
		var shovel map[string]interface{}
		if err := cursor.Decode(&shovel); err != nil {
			log.Printf("Warning: failed to decode shovel: %v", err)
			continue
		}

		if *dryRun {
			count++
			continue
		}

		mrc20Id := shovel["mrc20_id"].(string)
		pinId := shovel["pin_id"].(string)

		// Save to PebbleDB: mrc20_shovel_{mrc20Id}_{pinId} = "1"
		key := []byte(fmt.Sprintf("mrc20_shovel_%s_%s", mrc20Id, pinId))
		batch.Set(key, []byte("1"), pebble.Sync)

		count++
		if count%int64(*batchSize) == 0 {
			if err := batch.Commit(pebble.Sync); err != nil {
				return err
			}
			batch = pebbleDB.MrcDb.NewBatch()
			fmt.Printf("\r  Processed %d shovels...", count)
		}
	}

	if !*dryRun && batch.Count() > 0 {
		if err := batch.Commit(pebble.Sync); err != nil {
			return err
		}
	}

	stats.ShovelCount = count
	log.Printf("\n✓ Migrated %d shovels", count)
	return nil
}

func migrateOperationTx(mongoClient *mongo.Client, pebbleDB *pebblestore.Database, stats *MigrationStats) error {
	// Operation TX 数据已经在 migrateUtxoData 中从 UTXO 记录提取
	// 这里尝试从独立集合补充数据（如果存在的话）
	collection := mongoClient.Database(*dbName).Collection("mrc20_operation_tx")
	countDocs, err := collection.CountDocuments(context.Background(), bson.M{})
	if err != nil || countDocs == 0 {
		log.Println("ℹ Collection mrc20_operation_tx not found - operation data extracted from UTXOs above")
		// stats.OperationCount 已在 migrateUtxoData 中设置
		return nil
	}

	cursor, err := collection.Find(context.Background(), bson.M{"chain": *chainName})
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background())

	var batch *pebble.Batch
	if !*dryRun {
		batch = pebbleDB.MrcDb.NewBatch()
	}
	count := int64(0)

	for cursor.Next(context.Background()) {
		var opTx map[string]interface{}
		if err := cursor.Decode(&opTx); err != nil {
			log.Printf("Warning: failed to decode operation tx: %v", err)
			continue
		}

		if *dryRun {
			count++
			continue
		}

		txId := opTx["tx_id"].(string)

		// Save to PebbleDB: mrc20_op_tx_{txId} = "1"
		key := []byte("mrc20_op_tx_" + txId)
		batch.Set(key, []byte("1"), pebble.Sync)

		count++
		if count%int64(*batchSize) == 0 {
			if err := batch.Commit(pebble.Sync); err != nil {
				return err
			}
			batch = pebbleDB.MrcDb.NewBatch()
			fmt.Printf("\r  Processed %d operations...", count)
		}
	}

	if !*dryRun && batch.Count() > 0 {
		if err := batch.Commit(pebble.Sync); err != nil {
			return err
		}
	}

	stats.OperationCount = count
	log.Printf("\n✓ Migrated %d operation records", count)
	return nil
}

func setIndexHeight(pebbleDB *pebblestore.Database) error {
	syncKey := fmt.Sprintf("%s_mrc20_sync_height", *chainName)
	heightBytes := []byte(strconv.FormatInt(*continueHeight, 10))

	if err := pebbleDB.MetaDb.Set([]byte(syncKey), heightBytes, pebble.Sync); err != nil {
		return err
	}

	log.Printf("✓ Set %s = %d", syncKey, *continueHeight)
	return nil
}

func convertTickData(raw map[string]interface{}) (*mrc20.Mrc20DeployInfo, error) {
	// MongoDB 字段名是小写的，需要转换为 Go 结构体的驼峰格式
	tick := &mrc20.Mrc20DeployInfo{}

	if v, ok := raw["tick"].(string); ok {
		tick.Tick = v
	}
	if v, ok := raw["tokenname"].(string); ok {
		tick.TokenName = v
	}
	if v, ok := raw["decimals"].(string); ok {
		tick.Decimals = v
	}
	if v, ok := raw["amtpermint"].(string); ok {
		tick.AmtPerMint = v
	}
	if v, ok := raw["mintcount"]; ok {
		tick.MintCount = toUint64(v)
	}
	if v, ok := raw["beginheight"].(string); ok {
		tick.BeginHeight = v
	}
	if v, ok := raw["endheight"].(string); ok {
		tick.EndHeight = v
	}
	if v, ok := raw["metadata"].(string); ok {
		tick.Metadata = v
	}
	if v, ok := raw["deploytype"].(string); ok {
		tick.DeployType = v
	}
	if v, ok := raw["preminecount"]; ok {
		tick.PremineCount = toUint64(v)
	}
	if v, ok := raw["totalminted"]; ok {
		tick.TotalMinted = toUint64(v)
	}
	if v, ok := raw["mrc20id"].(string); ok {
		tick.Mrc20Id = v
	}
	if v, ok := raw["pinnumber"]; ok {
		tick.PinNumber = toInt64(v)
	}
	if v, ok := raw["chain"].(string); ok {
		tick.Chain = v
	}
	if v, ok := raw["holders"]; ok {
		tick.Holders = toUint64(v)
	}
	if v, ok := raw["txcount"]; ok {
		tick.TxCount = toUint64(v)
	}
	if v, ok := raw["metaid"].(string); ok {
		tick.MetaId = v
	}
	if v, ok := raw["address"].(string); ok {
		tick.Address = v
	}
	if v, ok := raw["deploytime"]; ok {
		tick.DeployTime = toInt64(v)
	}
	// Handle pincheck
	if pc, ok := raw["pincheck"].(map[string]interface{}); ok {
		if v, ok := pc["creator"].(string); ok {
			tick.PinCheck.Creator = v
		}
		if v, ok := pc["lvl"].(string); ok {
			tick.PinCheck.Lv = v
		}
		if v, ok := pc["path"].(string); ok {
			tick.PinCheck.Path = v
		}
		if v, ok := pc["count"].(string); ok {
			tick.PinCheck.Count = v
		}
	}
	// Handle paycheck
	if pc, ok := raw["paycheck"].(map[string]interface{}); ok {
		if v, ok := pc["payto"].(string); ok {
			tick.PayCheck.PayTo = v
		}
		if v, ok := pc["payamount"].(string); ok {
			tick.PayCheck.PayAmount = v
		}
	}

	return tick, nil
}

func convertUtxoData(raw map[string]interface{}) (*mrc20.Mrc20Utxo, error) {
	// MongoDB 字段名是小写的，需要转换为 Go 结构体的驼峰格式
	utxo := &mrc20.Mrc20Utxo{}

	if v, ok := raw["tick"].(string); ok {
		utxo.Tick = v
	}
	if v, ok := raw["mrc20id"].(string); ok {
		utxo.Mrc20Id = v
	}
	if v, ok := raw["txpoint"].(string); ok {
		utxo.TxPoint = v
	}
	if v, ok := raw["pointvalue"]; ok {
		utxo.PointValue = toUint64(v)
	}
	if v, ok := raw["pinid"].(string); ok {
		utxo.PinId = v
	}
	if v, ok := raw["pincontent"].(string); ok {
		utxo.PinContent = v
	}
	if v, ok := raw["verify"].(bool); ok {
		utxo.Verify = v
	}
	if v, ok := raw["blockheight"]; ok {
		utxo.BlockHeight = toInt64(v)
	}
	if v, ok := raw["mrcoption"].(string); ok {
		utxo.MrcOption = v
	}
	if v, ok := raw["fromaddress"].(string); ok {
		utxo.FromAddress = v
	}
	if v, ok := raw["toaddress"].(string); ok {
		utxo.ToAddress = v
	}
	if v, ok := raw["msg"].(string); ok {
		utxo.Msg = v
	}
	if v, ok := raw["amtchange"]; ok {
		utxo.AmtChange = toDecimal(v)
	}
	if v, ok := raw["status"]; ok {
		utxo.Status = toInt(v)
	}
	if v, ok := raw["chain"].(string); ok {
		utxo.Chain = v
	}
	if v, ok := raw["index"]; ok {
		utxo.Index = toInt(v)
	}
	if v, ok := raw["timestamp"]; ok {
		utxo.Timestamp = toInt64(v)
	}
	if v, ok := raw["operationtx"].(string); ok {
		utxo.OperationTx = v
	}

	// Direction 字段已删除，方向由前缀决定：
	// - mrc20_in_: 收入记录
	// - mrc20_out_: 支出记录 (Status=-1)

	return utxo, nil
}

// 类型转换辅助函数
func toUint64(v interface{}) uint64 {
	switch val := v.(type) {
	case int:
		return uint64(val)
	case int32:
		return uint64(val)
	case int64:
		return uint64(val)
	case float64:
		return uint64(val)
	case uint64:
		return val
	default:
		return 0
	}
}

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case float64:
		return int64(val)
	default:
		return 0
	}
}

func toInt(v interface{}) int {
	switch val := v.(type) {
	case int:
		return val
	case int32:
		return int(val)
	case int64:
		return int(val)
	case float64:
		return int(val)
	default:
		return 0
	}
}

func toDecimal(v interface{}) decimal.Decimal {
	switch val := v.(type) {
	case string:
		d, _ := decimal.NewFromString(val)
		return d
	case float64:
		return decimal.NewFromFloat(val)
	case int64:
		return decimal.NewFromInt(val)
	case int32:
		return decimal.NewFromInt(int64(val))
	case int:
		return decimal.NewFromInt(int64(val))
	case primitive.Decimal128:
		// Handle MongoDB Decimal128 type
		d, _ := decimal.NewFromString(val.String())
		return d
	default:
		log.Printf("toDecimal: unknown type %T for value %v", v, v)
		return decimal.Zero
	}
}

func printStats(stats *MigrationStats) {
	duration := stats.EndTime.Sub(stats.StartTime)

	log.Println("\n=== Migration Statistics ===")
	log.Printf("Ticks:      %d", stats.TickCount)
	log.Printf("UTXOs:      %d", stats.UtxoCount)
	log.Printf("Addresses:  %d", stats.AddressCount)
	log.Printf("Shovels:    %d", stats.ShovelCount)
	log.Printf("Operations: %d", stats.OperationCount)
	log.Printf("Duration:   %v", duration)
	log.Println("============================")

	if *dryRun {
		log.Println("\n✓ Dry run completed. No data was written.")
		log.Println("  Run without --dry-run to perform actual migration.")
	} else {
		log.Println("\n✓ Migration completed successfully!")
		log.Printf("  MRC20 will continue indexing from block %d", *continueHeight)
	}
}
