package api

import (
	"fmt"
	"log"
	"manindexer/api/respond"
	"manindexer/common"
	"manindexer/man"
	"manindexer/mrc20"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"
	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"
)

func mrc20JsonApi(r *gin.Engine) {
	mrc20Group := r.Group("/api/mrc20")
	mrc20Group.Use(CorsMiddleware())
	mrc20Group.GET("/tick/all", allTick)
	mrc20Group.GET("/tick/info/:id", getTickInfoById)
	mrc20Group.GET("/tick/info", getTickInfo)
	mrc20Group.GET("/tick/address", getHistoryByAddress)
	mrc20Group.GET("/tick/history", getHistoryById)
	mrc20Group.GET("/address/balance/:address", getBalanceByAddress)
	mrc20Group.GET("/address/history/:tickId/:address", getAddressHistoryByTickAndAddress)
	mrc20Group.GET("/tx/history", getHistoryByTx)
	mrc20Group.GET("/tick/AddressBalance", getAddressBalance)

	// 管理接口
	adminGroup := mrc20Group.Group("/admin")
	adminGroup.GET("/index-height/:chain", getIndexHeight)
	adminGroup.GET("/index-height/:chain/set", setIndexHeight)
}

func allTick(ctx *gin.Context) {
	cursor, err := strconv.ParseInt(ctx.Query("cursor"), 10, 64)
	if err != nil {
		cursor = 0
	}
	size, err := strconv.ParseInt(ctx.Query("size"), 10, 64)
	if err != nil {
		size = 20
	}
	// PebbleStore 方法不支持 order/completed/orderType 参数，返回全量数据
	list, err := man.PebbleStore.GetMrc20TickList(int(cursor), int(size))
	if err != nil || list == nil || len(list) == 0 {
		ctx.JSON(http.StatusOK, respond.ErrNoDataFound)
		return
	}
	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{"list": list, "total": len(list)}))
}
func getTickInfoById(ctx *gin.Context) {
	info, err := man.PebbleStore.GetMrc20TickInfo(ctx.Param("id"), "")
	if err != nil {
		ctx.JSON(http.StatusOK, respond.ErrNoResultFound)
		return
	}
	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", info))
}
func getTickInfo(ctx *gin.Context) {
	info, err := man.PebbleStore.GetMrc20TickInfo(ctx.Query("id"), ctx.Query("tick"))
	if err != nil {
		ctx.JSON(http.StatusOK, respond.ErrNoResultFound)
		return
	}
	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", info))
}
func getHistoryByAddress(ctx *gin.Context) {
	cursor, err := strconv.ParseInt(ctx.Query("cursor"), 10, 64)
	if err != nil {
		cursor = 0
	}
	size, err := strconv.ParseInt(ctx.Query("size"), 10, 64)
	if err != nil {
		size = 20
	}
	tickId := ctx.Query("tickId")
	address := ctx.Query("address")

	// 状态参数，默认为空返回所有状态
	statusStr := ctx.Query("status")
	var statusFilter *int
	if statusStr != "" {
		status, err := strconv.Atoi(statusStr)
		if err == nil {
			statusFilter = &status
		}
	}

	list, total, err := man.PebbleStore.GetMrc20AddressHistory(tickId, address, int(cursor), int(size), statusFilter)
	if err != nil || list == nil || len(list) == 0 {
		ctx.JSON(http.StatusOK, respond.ErrNoDataFound)
		return
	}
	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{"list": list, "total": total}))
}
func getHistoryById(ctx *gin.Context) {
	cursor, err := strconv.ParseInt(ctx.Query("cursor"), 10, 64)
	if err != nil {
		cursor = 0
	}
	size, err := strconv.ParseInt(ctx.Query("size"), 10, 64)
	if err != nil {
		size = 20
	}
	tickId := ctx.Query("tickId")
	chain := ctx.Query("chain")
	if chain == "" {
		chain = "btc" // 默认 BTC
	}

	// 新架构：使用 Transaction 流水表
	// 查询该 tick 的所有交易（不限地址）
	list, total, err := man.PebbleStore.GetMrc20TransactionHistory("", tickId, chain, int(size), int(cursor))
	if err != nil || list == nil || len(list) == 0 {
		ctx.JSON(http.StatusOK, respond.ErrNoDataFound)
		return
	}
	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{"list": list, "total": total}))
}

func getBalanceByAddress(ctx *gin.Context) {
	address := ctx.Param("address")
	cursor, err := strconv.ParseInt(ctx.Query("cursor"), 10, 64)
	if err != nil {
		cursor = 0
	}
	size, err := strconv.ParseInt(ctx.Query("size"), 10, 64)
	if err != nil {
		size = 20
	}

	chain := ctx.Query("chain")
	if chain == "" {
		chain = "btc" // 默认 BTC
	}

	// 新架构：使用 AccountBalance 表
	balanceMap := make(map[string]*mrc20.Mrc20Balance)
	var nameList []string

	// 获取该地址在指定链的所有余额
	// 使用前缀扫描 mrc20_balance_{chain}_{address}_
	prefix := []byte(fmt.Sprintf("mrc20_balance_%s_%s_", chain, address))
	iter, err := man.PebbleStore.Database.MrcDb.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		ctx.JSON(http.StatusOK, respond.ErrNoDataFound)
		return
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var accountBalance mrc20.Mrc20AccountBalance
		if err := sonic.Unmarshal(iter.Value(), &accountBalance); err != nil {
			continue
		}

		// 转换为 API 响应格式
		balance := &mrc20.Mrc20Balance{
			Id:                accountBalance.TickId,
			Name:              accountBalance.Tick,
			Chain:             accountBalance.Chain,
			Balance:           accountBalance.Balance,
			PendingOutBalance: accountBalance.PendingOut,
			PendingInBalance:  accountBalance.PendingIn,
		}

		balanceMap[accountBalance.Tick] = balance
		nameList = append(nameList, accountBalance.Tick)
	}

	// 查询该地址的 teleport pending in（teleport 接收方的待转入余额）
	// 注意：这部分暂时保留，因为 teleport pending in 可能还未写入 AccountBalance
	teleportPendingIns, _ := man.PebbleStore.GetTeleportPendingInByAddress(address)
	for _, pendingIn := range teleportPendingIns {
		if balance, ok := balanceMap[pendingIn.Tick]; ok {
			balance.PendingInBalance = balance.PendingInBalance.Add(pendingIn.Amount)
		} else {
			balanceMap[pendingIn.Tick] = &mrc20.Mrc20Balance{
				Id:               pendingIn.TickId,
				Name:             pendingIn.Tick,
				Chain:            pendingIn.Chain,
				PendingInBalance: pendingIn.Amount,
			}
			nameList = append(nameList, pendingIn.Tick)
		}
	}

	if len(nameList) == 0 {
		ctx.JSON(http.StatusOK, respond.ErrNoDataFound)
		return
	}

	// 排序
	sort.Strings(nameList)

	// 分页
	total := int64(len(nameList))
	start := int(cursor)
	end := int(cursor + size)
	if start >= len(nameList) {
		ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{"list": []mrc20.Mrc20Balance{}, "total": total}))
		return
	}
	if end > len(nameList) {
		end = len(nameList)
	}
	nameList = nameList[start:end]

	var result []mrc20.Mrc20Balance
	for _, name := range nameList {
		if balance, ok := balanceMap[name]; ok {
			// 只返回有余额的（任意类型余额大于0）
			if balance.Balance.GreaterThan(decimal.Zero) ||
				balance.PendingInBalance.GreaterThan(decimal.Zero) ||
				balance.PendingOutBalance.GreaterThan(decimal.Zero) {
				result = append(result, *balance)
			}
		}
	}

	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{"list": result, "total": total}))
}
func getHistoryByTx(ctx *gin.Context) {
	txId := ctx.Query("txId")
	if txId == "" {
		ctx.JSON(http.StatusOK, respond.ErrParameterError)
		return
	}

	// 通过 txId 查找 UTXO
	utxo, err := man.PebbleStore.CheckOperationtx(txId, false)
	if err != nil || utxo == nil {
		ctx.JSON(http.StatusOK, respond.ErrNoDataFound)
		return
	}

	// 返回找到的 UTXO（已包含 chain 字段）
	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{"list": []*mrc20.Mrc20Utxo{utxo}, "total": 1}))
}
func getAddressHistoryByTickAndAddress(ctx *gin.Context) {
	tickId := ctx.Param("tickId")
	address := ctx.Param("address")
	if tickId == "" || address == "" {
		ctx.JSON(http.StatusOK, respond.ErrParameterError)
		return
	}

	cursor, err := strconv.ParseInt(ctx.Query("cursor"), 10, 64)
	if err != nil {
		cursor = 0
	}
	size, err := strconv.ParseInt(ctx.Query("size"), 10, 64)
	if err != nil {
		size = 20
	}

	chain := ctx.Query("chain")
	if chain == "" {
		chain = "btc" // 默认 BTC
	}

	// 新架构：使用 Transaction 流水表
	list, total, err := man.PebbleStore.GetMrc20TransactionHistory(address, tickId, chain, int(size), int(cursor))
	if err != nil || list == nil || len(list) == 0 {
		ctx.JSON(http.StatusOK, respond.ErrNoDataFound)
		return
	}
	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{"list": list, "total": total}))
}

func getAddressBalance(ctx *gin.Context) {
	address := ctx.Query("address")
	tickId := ctx.Query("tickId")
	if address == "" || tickId == "" {
		ctx.JSON(http.StatusOK, respond.ErrParameterError)
		return
	}

	chain := ctx.Query("chain")
	if chain == "" {
		chain = "btc" // 默认 BTC
	}

	// 新架构：使用 AccountBalance 表
	accountBalance, err := man.PebbleStore.GetMrc20AccountBalance(chain, address, tickId)
	if err != nil {
		// 余额不存在，返回 0
		ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
			"balance":    "0",
			"pendingIn":  "0",
			"pendingOut": "0",
		}))
		return
	}

	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
		"balance":    accountBalance.Balance.String(),
		"pendingIn":  accountBalance.PendingIn.String(),
		"pendingOut": accountBalance.PendingOut.String(),
		"utxoCount":  accountBalance.UtxoCount,
	}))
}

// getIndexHeight 获取指定链的 MRC20 索引高度
func getIndexHeight(ctx *gin.Context) {
	chainName := strings.ToLower(ctx.Param("chain"))

	// 验证链名称
	if !isValidChain(chainName) {
		ctx.JSON(http.StatusOK, respond.ErrParameterError)
		return
	}

	// 获取当前索引高度
	currentHeight := man.PebbleStore.GetMrc20IndexHeight(chainName)

	// 获取配置中的 mrc20Height
	var configHeight int64
	switch chainName {
	case "btc", "bitcoin":
		configHeight = common.Config.Btc.Mrc20Height
	case "doge", "dogecoin":
		configHeight = common.Config.Doge.Mrc20Height
	case "mvc":
		configHeight = common.Config.Mvc.Mrc20Height
	default:
		configHeight = 0
	}

	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
		"chain":         chainName,
		"currentHeight": currentHeight,
		"configHeight":  configHeight,
	}))
}

// setIndexHeight 设置指定链的 MRC20 索引高度
func setIndexHeight(ctx *gin.Context) {
	chainName := strings.ToLower(ctx.Param("chain"))

	// 验证链名称
	if !isValidChain(chainName) {
		ctx.JSON(http.StatusOK, respond.ErrParameterError)
		return
	}

	// 从查询参数获取数据
	heightStr := ctx.Query("height")
	token := ctx.Query("token")
	reason := ctx.Query("reason")

	// 验证 height 参数
	if heightStr == "" {
		ctx.JSON(http.StatusOK, respond.ErrParameterError)
		return
	}

	height, err := strconv.ParseInt(heightStr, 10, 64)
	if err != nil {
		ctx.JSON(http.StatusOK, respond.ErrParameterError)
		return
	}

	// 简单的 token 验证（可以根据需要加强）
	if common.Config.AdminToken != "" && token != common.Config.AdminToken {
		ctx.JSON(http.StatusUnauthorized, &respond.ApiResponse{
			Code: 401,
			Msg:  "Unauthorized: invalid admin token",
			Data: nil,
		})
		return
	}

	// 获取当前高度
	currentHeight := man.PebbleStore.GetMrc20IndexHeight(chainName)

	// 验证新高度是否合理
	if height < 0 {
		ctx.JSON(http.StatusOK, &respond.ApiResponse{
			Code: -1,
			Msg:  "Height cannot be negative",
			Data: nil,
		})
		return
	}

	// 记录日志
	log.Printf("[ADMIN] MRC20 index height change for %s: %d -> %d, reason: %s",
		chainName, currentHeight, height, reason)

	// 保存新的索引高度
	err = man.PebbleStore.SaveMrc20IndexHeight(chainName, height)
	if err != nil {
		log.Printf("Failed to save MRC20 index height for %s: %v", chainName, err)
		ctx.JSON(http.StatusOK, &respond.ApiResponse{
			Code: -1,
			Msg:  fmt.Sprintf("Failed to save index height: %v", err),
			Data: nil,
		})
		return
	}

	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
		"chain":     chainName,
		"oldHeight": currentHeight,
		"newHeight": height,
		"reason":    reason,
		"message":   "MRC20 index height updated successfully",
	}))
}

// isValidChain 验证链名称是否有效
func isValidChain(chainName string) bool {
	validChains := []string{"btc", "bitcoin", "doge", "dogecoin", "mvc"}
	for _, valid := range validChains {
		if chainName == valid {
			return true
		}
	}
	return false
}
