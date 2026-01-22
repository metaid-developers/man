package main

import (
"context"
"encoding/json"
"flag"
"fmt"
"log"
"time"

"go.mongodb.org/mongo-driver/bson"
"go.mongodb.org/mongo-driver/mongo"
"go.mongodb.org/mongo-driver/mongo/options"
)

var (
mongoURI = flag.String("mongo", "mongodb://localhost:27017", "MongoDB connection URI")
dbName   = flag.String("db", "man_btc", "MongoDB database name")
chain    = flag.String("chain", "btc", "Chain name")
)

func main() {
flag.Parse()

ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
defer cancel()

client, err := mongo.Connect(ctx, options.Client().ApplyURI(*mongoURI))
if err != nil {
log.Fatalf("Failed to connect: %v", err)
}
defer client.Disconnect(ctx)

db := client.Database(*dbName)

// 1. 检查 mrc20ticks 集合
fmt.Println("=== mrc20ticks Sample ===")
var tickSample bson.M
err = db.Collection("mrc20ticks").FindOne(ctx, bson.M{"chain": *chain}).Decode(&tickSample)
if err != nil {
fmt.Printf("Error: %v\n", err)
} else {
printJSON(tickSample)
}

// 2. 检查 mrc20utxos 集合 - mint 操作
fmt.Println("\n=== mrc20utxos Sample (mint) ===")
var mintSample bson.M
err = db.Collection("mrc20utxos").FindOne(ctx, bson.M{"chain": *chain, "mrcoption": "mint"}).Decode(&mintSample)
if err != nil {
fmt.Printf("Error: %v\n", err)
} else {
printJSON(mintSample)
}

// 3. 检查 mrc20utxos 集合 - transfer 操作
fmt.Println("\n=== mrc20utxos Sample (transfer) ===")
var transferSample bson.M
err = db.Collection("mrc20utxos").FindOne(ctx, bson.M{"chain": *chain, "mrcoption": "transfer"}).Decode(&transferSample)
if err != nil {
fmt.Printf("Error: %v\n", err)
} else {
printJSON(transferSample)
}

// 4. 检查 mrc20utxos 集合 - deploy 操作
fmt.Println("\n=== mrc20utxos Sample (deploy) ===")
var deploySample bson.M
err = db.Collection("mrc20utxos").FindOne(ctx, bson.M{"chain": *chain, "mrcoption": "deploy"}).Decode(&deploySample)
if err != nil {
fmt.Printf("Error: %v\n", err)
} else {
printJSON(deploySample)
}

// 5. 统计各类型操作数量
fmt.Println("\n=== Operation Statistics ===")
pipeline := []bson.M{
{"$match": bson.M{"chain": *chain}},
{"$group": bson.M{"_id": "$mrcoption", "count": bson.M{"$sum": 1}}},
}
cursor, err := db.Collection("mrc20utxos").Aggregate(ctx, pipeline)
if err != nil {
fmt.Printf("Error: %v\n", err)
} else {
var results []bson.M
cursor.All(ctx, &results)
for _, r := range results {
fmt.Printf("  %s: %v\n", r["_id"], r["count"])
}
}

// 6. 检查 Status 字段的分布
fmt.Println("\n=== Status Distribution ===")
pipeline = []bson.M{
{"$match": bson.M{"chain": *chain}},
{"$group": bson.M{"_id": "$status", "count": bson.M{"$sum": 1}}},
}
cursor, err = db.Collection("mrc20utxos").Aggregate(ctx, pipeline)
if err != nil {
fmt.Printf("Error: %v\n", err)
} else {
var results []bson.M
cursor.All(ctx, &results)
for _, r := range results {
fmt.Printf("  status=%v: %v\n", r["_id"], r["count"])
}
}

// 7. 检查 Verify 字段的分布
fmt.Println("\n=== Verify Distribution ===")
pipeline = []bson.M{
{"$match": bson.M{"chain": *chain}},
{"$group": bson.M{"_id": "$verify", "count": bson.M{"$sum": 1}}},
}
cursor, err = db.Collection("mrc20utxos").Aggregate(ctx, pipeline)
if err != nil {
fmt.Printf("Error: %v\n", err)
} else {
var results []bson.M
cursor.All(ctx, &results)
for _, r := range results {
fmt.Printf("  verify=%v: %v\n", r["_id"], r["count"])
}
}

// 8. 检查最高和最低 blockheight
fmt.Println("\n=== Block Height Range ===")
var minHeight, maxHeight bson.M
opts := options.FindOne().SetSort(bson.M{"blockheight": 1})
db.Collection("mrc20utxos").FindOne(ctx, bson.M{"chain": *chain}, opts).Decode(&minHeight)
opts = options.FindOne().SetSort(bson.M{"blockheight": -1})
db.Collection("mrc20utxos").FindOne(ctx, bson.M{"chain": *chain}, opts).Decode(&maxHeight)
if minHeight != nil && maxHeight != nil {
fmt.Printf("  Min blockheight: %v\n", minHeight["blockheight"])
fmt.Printf("  Max blockheight: %v\n", maxHeight["blockheight"])
}

// 9. 检查是否有 pinid 为空的 mint 记录
fmt.Println("\n=== Mint records without PinId ===")
count, _ := db.Collection("mrc20utxos").CountDocuments(ctx, bson.M{
"chain":     *chain,
"mrcoption": "mint",
"$or": []bson.M{
{"pinid": ""},
{"pinid": nil},
{"pinid": bson.M{"$exists": false}},
},
})
fmt.Printf("  Count: %d\n", count)

// 10. 检查是否有 operationtx 为空的记录
fmt.Println("\n=== Records without OperationTx ===")
count, _ = db.Collection("mrc20utxos").CountDocuments(ctx, bson.M{
"chain": *chain,
"$or": []bson.M{
{"operationtx": ""},
{"operationtx": nil},
{"operationtx": bson.M{"$exists": false}},
},
})
fmt.Printf("  Count: %d\n", count)
}

func printJSON(v interface{}) {
b, _ := json.MarshalIndent(v, "", "  ")
fmt.Println(string(b))
}
