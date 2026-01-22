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
	dbName   = flag.String("db", "metaso", "MongoDB database name")
)

func main() {
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(*mongoURI))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Disconnect(ctx)

	db := client.Database(*dbName)

	// 1. 列出所有集合
	collections, err := db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		log.Fatalf("Failed to list collections: %v", err)
	}
	fmt.Println("=== Collections ===")
	for _, c := range collections {
		count, _ := db.Collection(c).CountDocuments(ctx, bson.M{})
		fmt.Printf("  %s: %d documents\n", c, count)
	}

	// 2. 查找可能的 MRC20 相关集合
	fmt.Println("\n=== Looking for MRC20 collections ===")
	mrc20Keywords := []string{"mrc20", "mrc", "token", "tick", "utxo"}
	for _, c := range collections {
		for _, kw := range mrc20Keywords {
			if contains(c, kw) {
				fmt.Printf("  Found: %s\n", c)
				// 显示一条样例数据
				var sample bson.M
				err := db.Collection(c).FindOne(ctx, bson.M{}).Decode(&sample)
				if err == nil {
					fmt.Printf("    Sample fields: ")
					for k := range sample {
						fmt.Printf("%s, ", k)
					}
					fmt.Println()
				}
				break
			}
		}
	}

	// 3. 检查标准 MRC20 集合
	fmt.Println("\n=== Checking standard MRC20 collections ===")
	standardCollections := []string{"mrc20_tick", "mrc20_utxo", "mrc20_shovel", "mrc20_operation_tx"}
	for _, c := range standardCollections {
		count, err := db.Collection(c).CountDocuments(ctx, bson.M{})
		if err != nil {
			fmt.Printf("  %s: ERROR - %v\n", c, err)
		} else {
			fmt.Printf("  %s: %d documents\n", c, count)
			// 如果有数据，查看 chain 字段的分布
			if count > 0 {
				pipeline := []bson.M{
					{"$group": bson.M{"_id": "$chain", "count": bson.M{"$sum": 1}}},
				}
				cursor, err := db.Collection(c).Aggregate(ctx, pipeline)
				if err == nil {
					var results []bson.M
					cursor.All(ctx, &results)
					fmt.Printf("    Chain distribution: %v\n", results)
				}
			}
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsLower(s, substr))
}

func containsLower(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
