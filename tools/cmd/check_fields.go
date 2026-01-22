package main

import (
"context"
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
col := db.Collection("mrc20utxos")

// 1. 统计唯一地址数量
fmt.Println("=== 唯一地址统计 ===")
pipeline := []bson.M{
{"$match": bson.M{"chain": *chain}},
{"$group": bson.M{"_id": "$toaddress"}},
{"$count": "total"},
}
cursor, _ := col.Aggregate(ctx, pipeline)
var result []bson.M
cursor.All(ctx, &result)
if len(result) > 0 {
fmt.Printf("唯一 toaddress 数量: %v\n", result[0]["total"])
}

// 2. 检查 toaddress 为空的记录
fmt.Println("\n=== toaddress 为空的记录 ===")
emptyAddr, _ := col.CountDocuments(ctx, bson.M{
"chain": *chain,
"$or": []bson.M{
{"toaddress": ""},
{"toaddress": nil},
{"toaddress": bson.M{"$exists": false}},
},
})
fmt.Printf("toaddress 为空: %d\n", emptyAddr)

// 3. 统计唯一 operationtx 数量
fmt.Println("\n=== Operation TX 统计 ===")
pipeline = []bson.M{
{"$match": bson.M{"chain": *chain, "operationtx": bson.M{"$ne": ""}}},
{"$group": bson.M{"_id": "$operationtx"}},
{"$count": "total"},
}
cursor, _ = col.Aggregate(ctx, pipeline)
result = nil
cursor.All(ctx, &result)
if len(result) > 0 {
fmt.Printf("唯一 operationtx 数量 (非空): %v\n", result[0]["total"])
}

// 4. 检查 operationtx 为空的记录
emptyOpTx, _ := col.CountDocuments(ctx, bson.M{
"chain": *chain,
"$or": []bson.M{
{"operationtx": ""},
{"operationtx": nil},
{"operationtx": bson.M{"$exists": false}},
},
})
fmt.Printf("operationtx 为空: %d\n", emptyOpTx)

// 5. 按操作类型统计 operationtx 为空的记录
fmt.Println("\n=== operationtx 为空的记录按操作类型分布 ===")
pipeline = []bson.M{
{"$match": bson.M{
"chain": *chain,
"$or": []bson.M{
{"operationtx": ""},
{"operationtx": nil},
},
}},
{"$group": bson.M{"_id": "$mrcoption", "count": bson.M{"$sum": 1}}},
}
cursor, _ = col.Aggregate(ctx, pipeline)
result = nil
cursor.All(ctx, &result)
for _, r := range result {
fmt.Printf("  %s: %v\n", r["_id"], r["count"])
}

// 6. 总数
total, _ := col.CountDocuments(ctx, bson.M{"chain": *chain})
fmt.Printf("\n总 UTXO 数: %d\n", total)
}
