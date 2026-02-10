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
	adminGroup.GET("/reindex-block/:chain/:height", reindexBlock)
	adminGroup.GET("/reindex-range/:chain/:start/:end", reindexBlockRange)
	adminGroup.GET("/reindex-from/:chain/:height", reindexFromHeight)
	adminGroup.GET("/recalculate-balance/:chain/:address/:tickId", recalculateBalance)
	adminGroup.GET("/verify-balance/:chain/:address/:tickId", verifyBalance)
	adminGroup.GET("/fix-pending/:chain", fixPendingUtxos)

	// 快照管理接口
	adminGroup.POST("/snapshot/create", createSnapshot)
	adminGroup.GET("/snapshot/list", listSnapshots)
	adminGroup.GET("/snapshot/info/:id", getSnapshotInfo)
	adminGroup.POST("/snapshot/restore/:id", restoreSnapshot)
	adminGroup.DELETE("/snapshot/:id", deleteSnapshot)

	// 调试接口
	debugGroup := mrc20Group.Group("/debug")
	debugGroup.GET("/pending-in/:address", debugPendingIn)
	debugGroup.GET("/utxo-status/:address/:tickId", debugUtxoStatus)
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

	// 验证参数，默认为空返回所有验证状态
	verifyStr := ctx.Query("verify")
	var verifyFilter *bool
	if verifyStr != "" {
		if verifyStr == "true" || verifyStr == "1" {
			verify := true
			verifyFilter = &verify
		} else if verifyStr == "false" || verifyStr == "0" {
			verify := false
			verifyFilter = &verify
		}
	}

	list, total, err := man.PebbleStore.GetMrc20AddressHistory(tickId, address, int(cursor), int(size), statusFilter, verifyFilter)
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

	// 新架构：使用 Transaction 流水表（跨链统一查询）
	// 查询该 tick 的所有交易（不限地址）
	list, total, err := man.PebbleStore.GetMrc20TransactionHistory("", tickId, int(size), int(cursor))
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

	// 可选的 chain 参数，如果不传则查询所有链
	chainFilter := ctx.Query("chain")

	// 新架构：使用 AccountBalance 表
	balanceMap := make(map[string]*mrc20.Mrc20Balance)
	var nameList []string

	// 获取该地址的所有余额（跨链查询）
	// 遍历所有可能的链
	chains := []string{"btc", "doge", "mvc"}
	if chainFilter != "" {
		// 如果指定了链，只查该链
		chains = []string{chainFilter}
	}

	for _, chain := range chains {
		prefix := []byte(fmt.Sprintf("balance_%s_%s_", chain, address))
		iter, err := man.PebbleStore.Database.MrcDb.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: append(prefix, 0xff),
		})
		if err != nil {
			continue
		}

		for iter.First(); iter.Valid(); iter.Next() {
			var accountBalance mrc20.Mrc20AccountBalance
			if err := sonic.Unmarshal(iter.Value(), &accountBalance); err != nil {
				continue
			}

			// 使用 tickId + chain 作为唯一键，支持同一 tick 在不同链上的余额
			key := fmt.Sprintf("%s_%s", accountBalance.TickId, accountBalance.Chain)

			// 转换为 API 响应格式
			// 注意：PendingInBalance 不使用 accountBalance.PendingIn，因为可能不准确
			// 后面会从 TeleportPendingIn + TransferPendingIn + UTXO 表实时计算
			balance := &mrc20.Mrc20Balance{
				Id:                accountBalance.TickId,
				Name:              accountBalance.Tick,
				Chain:             accountBalance.Chain,
				Balance:           accountBalance.Balance,
				PendingOutBalance: accountBalance.PendingOut,
				PendingInBalance:  decimal.Zero, // 初始化为 0，后面实时计算
			}

			balanceMap[key] = balance
			nameList = append(nameList, key)
		}
		iter.Close()
	}

	// 【修复】从 TeleportPendingIn 表实时计算 pendingIn（teleport 接收方的待转入余额）
	// 不使用 AccountBalance.PendingIn，因为可能不准确
	teleportPendingIns, _ := man.PebbleStore.GetTeleportPendingInByAddress(address)
	for _, pendingIn := range teleportPendingIns {
		key := fmt.Sprintf("%s_%s", pendingIn.TickId, pendingIn.Chain)
		if balance, ok := balanceMap[key]; ok {
			balance.PendingInBalance = balance.PendingInBalance.Add(pendingIn.Amount)
		} else {
			balanceMap[key] = &mrc20.Mrc20Balance{
				Id:               pendingIn.TickId,
				Name:             pendingIn.Tick,
				Chain:            pendingIn.Chain,
				PendingInBalance: pendingIn.Amount,
			}
			nameList = append(nameList, key)
		}
	}

	// 查询该地址的 transfer pending in（普通转账接收方的待转入余额）
	// 注意：TransferPendingIn 表可能数据不全，后面会从 UTXO 表实时计算补充
	transferPendingIns, _ := man.PebbleStore.GetTransferPendingInByAddress(address)
	for _, pendingIn := range transferPendingIns {
		key := fmt.Sprintf("%s_%s", pendingIn.TickId, pendingIn.Chain)
		if balance, ok := balanceMap[key]; ok {
			balance.PendingInBalance = balance.PendingInBalance.Add(pendingIn.Amount)
		} else {
			balanceMap[key] = &mrc20.Mrc20Balance{
				Id:               pendingIn.TickId,
				Name:             pendingIn.Tick,
				Chain:            pendingIn.Chain,
				PendingInBalance: pendingIn.Amount,
			}
			nameList = append(nameList, key)
		}
	}

	// 实时计算 pendingOut：扫描该地址的所有 UTXO
	// - pendingOut: Status=Pending 的 UTXO（被花费的待确认）
	// 【注意】不再从 UTXO 扫描 mempool pendingIn，因为 TransferPendingIn 表已经包含了
	for _, chain := range chains {
		// 直接扫描该地址的所有 UTXO（所有 tick）
		// 前缀：mrc20_in_{address}_
		prefix := []byte(fmt.Sprintf("mrc20_in_%s_", address))
		iter, err := man.PebbleStore.Database.MrcDb.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: append(prefix, 0xff),
		})
		if err != nil {
			continue
		}

		// 按 tick 分组统计 pendingOut
		tickPendingOut := make(map[string]decimal.Decimal)
		tickInfo := make(map[string]struct{ tickName, tickChain string })

		for iter.First(); iter.Valid(); iter.Next() {
			var utxo mrc20.Mrc20Utxo
			if err := sonic.Unmarshal(iter.Value(), &utxo); err != nil {
				continue
			}

			// 只处理 ToAddress=address 的 UTXO
			if utxo.ToAddress != address {
				continue
			}

			// 只处理当前链的 UTXO
			if utxo.Chain != chain {
				continue
			}

			tickId := utxo.Mrc20Id
			if tickId == "" {
				continue
			}

			// 记录 tick 信息
			if _, ok := tickInfo[tickId]; !ok {
				tickInfo[tickId] = struct{ tickName, tickChain string }{utxo.Tick, utxo.Chain}
			}

			// 计算 pendingOut：被花费的待确认 UTXO（Status=Pending）
			if utxo.Status == mrc20.UtxoStatusTeleportPending || utxo.Status == mrc20.UtxoStatusTransferPending {
				amtAbs := utxo.AmtChange
				if amtAbs.LessThan(decimal.Zero) {
					amtAbs = amtAbs.Neg()
				}
				tickPendingOut[tickId] = tickPendingOut[tickId].Add(amtAbs)
			}
		}
		iter.Close()

		// 更新 balanceMap 的 pendingOut，并修正 balance（扣除被花费的 pending UTXO）
		for tickId, info := range tickInfo {
			key := fmt.Sprintf("%s_%s", tickId, chain)
			balance, ok := balanceMap[key]
			if !ok {
				// 如果 balanceMap 中没有这个 tick，创建一个新的
				balance = &mrc20.Mrc20Balance{
					Id:    tickId,
					Name:  info.tickName,
					Chain: chain,
				}
				balanceMap[key] = balance
				nameList = append(nameList, key)
			}

			pendingOut := tickPendingOut[tickId]
			balance.PendingOutBalance = pendingOut

			// 【修复】balance 应该是"当前可用的已确认余额"
			// 如果 UTXO 已经被花费（Pending 状态），这部分不应该算在 balance 里
			// balance = 存储的已确认余额 - 正在被花费的金额
			if pendingOut.GreaterThan(decimal.Zero) {
				balance.Balance = balance.Balance.Sub(pendingOut)
				if balance.Balance.LessThan(decimal.Zero) {
					balance.Balance = decimal.Zero
				}
			}
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

	// 获取 index 参数（可选）
	indexStr := ctx.Query("index")
	var targetIndex *int
	if indexStr != "" {
		if idx, err := strconv.Atoi(indexStr); err == nil {
			targetIndex = &idx
		}
	}

	// 通过 txId 查找 UTXO
	if targetIndex != nil {
		// 如果指定了 index，查找特定的 txPoint (txId:index)
		txPoint := fmt.Sprintf("%s:%d", txId, *targetIndex)
		utxo, err := man.PebbleStore.CheckOperationtxByTxPoint(txPoint, false)
		if err != nil || utxo == nil {
			ctx.JSON(http.StatusOK, respond.ErrNoDataFound)
			return
		}
		ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{"list": []*mrc20.Mrc20Utxo{utxo}, "total": 1}))
	} else {
		// 如果没有指定 index，返回该交易的所有 UTXO
		utxos, err := man.PebbleStore.CheckOperationtxAll(txId, false)
		if err != nil || len(utxos) == 0 {
			ctx.JSON(http.StatusOK, respond.ErrNoDataFound)
			return
		}
		ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{"list": utxos, "total": len(utxos)}))
	}
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

	// 新架构：使用 Transaction 流水表（跨链统一查询）
	list, total, err := man.PebbleStore.GetMrc20TransactionHistory(address, tickId, int(size), int(cursor))
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
		// 余额不存在，说明没有已确认余额
		// 从 TeleportPendingIn + TransferPendingIn 表计算 pendingIn
		pendingIn := decimal.Zero
		pendingOut := decimal.Zero

		// 【修复】只从 TeleportPendingIn 和 TransferPendingIn 表累加
		teleportPendingIns, _ := man.PebbleStore.GetTeleportPendingInByAddress(address)
		for _, p := range teleportPendingIns {
			if p.TickId == tickId && p.Chain == chain {
				pendingIn = pendingIn.Add(p.Amount)
			}
		}
		transferPendingIns, _ := man.PebbleStore.GetTransferPendingInByAddress(address)
		for _, p := range transferPendingIns {
			if p.TickId == tickId && p.Chain == chain {
				pendingIn = pendingIn.Add(p.Amount)
			}
		}

		// 【修复】balance 是已确认余额，没有 AccountBalance 表示没有已确认余额，返回 0
		ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
			"balance":    "0", // 没有已确认余额
			"pendingIn":  pendingIn.String(),
			"pendingOut": pendingOut.String(),
		}))
		return
	}

	// 实时计算 pendingOut：扫描该地址的所有 UTXO
	// - pendingOut: Status=Pending 的 UTXO（被花费的待确认）
	pendingOut := decimal.Zero
	prefix := []byte(fmt.Sprintf("mrc20_in_%s_%s_", address, tickId))
	iter, err := man.PebbleStore.Database.MrcDb.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err == nil {
		for iter.First(); iter.Valid(); iter.Next() {
			var utxo mrc20.Mrc20Utxo
			if err := sonic.Unmarshal(iter.Value(), &utxo); err != nil {
				continue
			}

			// 只处理 ToAddress=address 的 UTXO
			if utxo.ToAddress != address {
				continue
			}

			// 计算 pendingOut：被花费的待确认 UTXO（Status=Pending）
			if utxo.Status == mrc20.UtxoStatusTeleportPending || utxo.Status == mrc20.UtxoStatusTransferPending {
				amtAbs := utxo.AmtChange
				if amtAbs.LessThan(decimal.Zero) {
					amtAbs = amtAbs.Neg()
				}
				pendingOut = pendingOut.Add(amtAbs)
			}
		}
		iter.Close()
	}

	// 【修复】计算 pendingIn：从 TeleportPendingIn + TransferPendingIn 表实时计算
	// 不从 UTXO 扫描，因为 TransferPendingIn 表已经包含了 mempool UTXO 的信息
	// 不使用 accountBalance.PendingIn，因为可能不准确
	pendingIn := decimal.Zero

	// 累加 teleport pending in（teleport 接收方的待转入余额）
	teleportPendingIns, _ := man.PebbleStore.GetTeleportPendingInByAddress(address)
	for _, p := range teleportPendingIns {
		if p.TickId == tickId && p.Chain == chain {
			pendingIn = pendingIn.Add(p.Amount)
		}
	}

	// 累加 transfer pending in（普通转账接收方的待转入余额）
	transferPendingIns, _ := man.PebbleStore.GetTransferPendingInByAddress(address)
	for _, p := range transferPendingIns {
		if p.TickId == tickId && p.Chain == chain {
			pendingIn = pendingIn.Add(p.Amount)
		}
	}

	// 【修复】balance 应该是"当前可用的已确认余额"
	// 如果 UTXO 已经被花费（Pending 状态），这部分不应该算在 balance 里
	// balance = 存储的已确认余额 - 正在被花费的金额
	confirmedBalance := accountBalance.Balance.Sub(pendingOut)
	if confirmedBalance.LessThan(decimal.Zero) {
		confirmedBalance = decimal.Zero
	}

	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
		"balance":    confirmedBalance.String(),
		"pendingIn":  pendingIn.String(),
		"pendingOut": pendingOut.String(),
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

// ============== 调试接口 ==============

// debugPendingIn 查看指定地址的所有 TransferPendingIn 记录
func debugPendingIn(ctx *gin.Context) {
	address := ctx.Param("address")
	if address == "" {
		ctx.JSON(http.StatusOK, respond.ErrParameterError)
		return
	}

	// 查询 transfer pending in
	transferPendingIns, err := man.PebbleStore.GetTransferPendingInByAddress(address)
	if err != nil {
		ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
			"error":   err.Error(),
			"records": []interface{}{},
			"count":   0,
		}))
		return
	}

	// 也查询 teleport pending in
	teleportPendingIns, _ := man.PebbleStore.GetTeleportPendingInByAddress(address)

	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
		"transferPendingIn": transferPendingIns,
		"teleportPendingIn": teleportPendingIns,
		"transferCount":     len(transferPendingIns),
		"teleportCount":     len(teleportPendingIns),
	}))
}

// debugUtxoStatus 查看指定地址和 tick 的 UTXO 状态
func debugUtxoStatus(ctx *gin.Context) {
	address := ctx.Param("address")
	tickId := ctx.Param("tickId")
	if address == "" || tickId == "" {
		ctx.JSON(http.StatusOK, respond.ErrParameterError)
		return
	}

	type UtxoInfo struct {
		TxPoint     string `json:"txPoint"`
		Amount      string `json:"amount"`
		Status      int    `json:"status"`
		StatusName  string `json:"statusName"`
		FromAddress string `json:"fromAddress"`
		ToAddress   string `json:"toAddress"`
		BlockHeight int64  `json:"blockHeight"`
	}

	var utxos []UtxoInfo

	prefix := []byte(fmt.Sprintf("mrc20_in_%s_%s_", address, tickId))
	iter, err := man.PebbleStore.Database.MrcDb.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
			"error": err.Error(),
			"utxos": []interface{}{},
		}))
		return
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var utxo mrc20.Mrc20Utxo
		if err := sonic.Unmarshal(iter.Value(), &utxo); err != nil {
			continue
		}

		statusName := "Unknown"
		switch utxo.Status {
		case 0:
			statusName = "Available"
		case 1:
			statusName = "TeleportPending"
		case 2:
			statusName = "TransferPending"
		case -1:
			statusName = "Spent"
		}

		utxos = append(utxos, UtxoInfo{
			TxPoint:     utxo.TxPoint,
			Amount:      utxo.AmtChange.String(),
			Status:      utxo.Status,
			StatusName:  statusName,
			FromAddress: utxo.FromAddress,
			ToAddress:   utxo.ToAddress,
			BlockHeight: utxo.BlockHeight,
		})
	}

	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
		"address": address,
		"tickId":  tickId,
		"utxos":   utxos,
		"count":   len(utxos),
	}))
}

// ============== 区块重跑管理接口 ==============

// reindexBlock 重跑单个区块
// GET /api/mrc20/admin/reindex-block/:chain/:height?token=xxx
func reindexBlock(ctx *gin.Context) {
	chainName := strings.ToLower(ctx.Param("chain"))
	heightStr := ctx.Param("height")
	token := ctx.Query("token")

	// 验证链名
	if !isValidChain(chainName) {
		ctx.JSON(http.StatusOK, respond.ErrParameterError)
		return
	}

	// 验证 token
	if common.Config.AdminToken != "" && token != common.Config.AdminToken {
		ctx.JSON(http.StatusUnauthorized, &respond.ApiResponse{
			Code: 401,
			Msg:  "Unauthorized: invalid admin token",
			Data: nil,
		})
		return
	}

	height, err := strconv.ParseInt(heightStr, 10, 64)
	if err != nil || height <= 0 {
		ctx.JSON(http.StatusOK, &respond.ApiResponse{
			Code: -1,
			Msg:  "Invalid height",
			Data: nil,
		})
		return
	}

	log.Printf("[ADMIN] ReindexBlock request: chain=%s, height=%d", chainName, height)

	// 执行重跑
	err = man.PebbleStore.ReindexBlock(chainName, height)
	if err != nil {
		log.Printf("[ADMIN] ReindexBlock failed: %v", err)
		ctx.JSON(http.StatusOK, &respond.ApiResponse{
			Code: -1,
			Msg:  fmt.Sprintf("ReindexBlock failed: %v", err),
			Data: nil,
		})
		return
	}

	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
		"chain":   chainName,
		"height":  height,
		"message": "Block reindexed successfully",
	}))
}

// reindexBlockRange 重跑区块范围
// GET /api/mrc20/admin/reindex-range/:chain/:start/:end?token=xxx
func reindexBlockRange(ctx *gin.Context) {
	chainName := strings.ToLower(ctx.Param("chain"))
	startStr := ctx.Param("start")
	endStr := ctx.Param("end")
	token := ctx.Query("token")

	// 验证链名
	if !isValidChain(chainName) {
		ctx.JSON(http.StatusOK, respond.ErrParameterError)
		return
	}

	// 验证 token
	if common.Config.AdminToken != "" && token != common.Config.AdminToken {
		ctx.JSON(http.StatusUnauthorized, &respond.ApiResponse{
			Code: 401,
			Msg:  "Unauthorized: invalid admin token",
			Data: nil,
		})
		return
	}

	startHeight, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || startHeight <= 0 {
		ctx.JSON(http.StatusOK, &respond.ApiResponse{
			Code: -1,
			Msg:  "Invalid start height",
			Data: nil,
		})
		return
	}

	endHeight, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil || endHeight <= 0 || endHeight < startHeight {
		ctx.JSON(http.StatusOK, &respond.ApiResponse{
			Code: -1,
			Msg:  "Invalid end height (must be >= start height)",
			Data: nil,
		})
		return
	}

	// 限制最大范围，避免长时间阻塞
	maxRange := int64(100)
	if endHeight-startHeight > maxRange {
		ctx.JSON(http.StatusOK, &respond.ApiResponse{
			Code: -1,
			Msg:  fmt.Sprintf("Range too large, max allowed: %d blocks", maxRange),
			Data: nil,
		})
		return
	}

	log.Printf("[ADMIN] ReindexBlockRange request: chain=%s, start=%d, end=%d", chainName, startHeight, endHeight)

	// 执行重跑
	err = man.PebbleStore.ReindexBlockRange(chainName, startHeight, endHeight)
	if err != nil {
		log.Printf("[ADMIN] ReindexBlockRange failed: %v", err)
		ctx.JSON(http.StatusOK, &respond.ApiResponse{
			Code: -1,
			Msg:  fmt.Sprintf("ReindexBlockRange failed: %v", err),
			Data: nil,
		})
		return
	}

	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
		"chain":       chainName,
		"startHeight": startHeight,
		"endHeight":   endHeight,
		"blockCount":  endHeight - startHeight + 1,
		"message":     "Block range reindexed successfully",
	}))
}

// reindexFromHeight 从指定高度重跑（真正幂等）
// GET /api/mrc20/admin/reindex-from/:chain/:height?token=xxx
// 该接口会：
// 1. 删除所有 BlockHeight >= height 的 UTXO
// 2. 恢复所有 SpentAtHeight >= height 的 UTXO
// 3. 设置索引高度为 height - 1
// 调用后需要重启服务让主循环重新索引
func reindexFromHeight(ctx *gin.Context) {
	chainName := strings.ToLower(ctx.Param("chain"))
	heightStr := ctx.Param("height")
	token := ctx.Query("token")

	// 验证链名
	if !isValidChain(chainName) {
		ctx.JSON(http.StatusOK, respond.ErrParameterError)
		return
	}

	// 验证 token
	if common.Config.AdminToken != "" && token != common.Config.AdminToken {
		ctx.JSON(http.StatusUnauthorized, &respond.ApiResponse{
			Code: 401,
			Msg:  "Unauthorized: invalid admin token",
			Data: nil,
		})
		return
	}

	height, err := strconv.ParseInt(heightStr, 10, 64)
	if err != nil || height <= 0 {
		ctx.JSON(http.StatusOK, &respond.ApiResponse{
			Code: -1,
			Msg:  "Invalid height",
			Data: nil,
		})
		return
	}

	log.Printf("[ADMIN] ReindexFromHeight request: chain=%s, height=%d", chainName, height)

	// 执行重跑
	stats, err := man.PebbleStore.ReindexFromHeight(chainName, height)
	if err != nil {
		log.Printf("[ADMIN] ReindexFromHeight failed: %v", err)
		ctx.JSON(http.StatusOK, &respond.ApiResponse{
			Code: -1,
			Msg:  fmt.Sprintf("ReindexFromHeight failed: %v", err),
			Data: nil,
		})
		return
	}

	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
		"chain":                    chainName,
		"fromHeight":               height,
		"newIndexHeight":           height - 1,
		"deleted":                  stats["deleted"],
		"restored":                 stats["restored"],
		"pendingFixed":             stats["pendingFixed"],
		"balanceCleared":           stats["balanceCleared"],
		"pendingTeleportCleared":   stats["pendingTeleportCleared"],
		"arrivalCleared":           stats["arrivalCleared"],
		"teleportPendingInCleared": stats["teleportPendingInCleared"],
		"transferPendingInCleared": stats["transferPendingInCleared"],
		"message":                  "Reindex prepared. Restart service to reindex from the specified height.",
	}))
}

// recalculateBalance 重算指定地址余额
// GET /api/mrc20/admin/recalculate-balance/:chain/:address/:tickId?token=xxx
func recalculateBalance(ctx *gin.Context) {
	chainName := strings.ToLower(ctx.Param("chain"))
	address := ctx.Param("address")
	tickId := ctx.Param("tickId")
	token := ctx.Query("token")

	// 验证链名
	if !isValidChain(chainName) {
		ctx.JSON(http.StatusOK, respond.ErrParameterError)
		return
	}

	// 验证 token
	if common.Config.AdminToken != "" && token != common.Config.AdminToken {
		ctx.JSON(http.StatusUnauthorized, &respond.ApiResponse{
			Code: 401,
			Msg:  "Unauthorized: invalid admin token",
			Data: nil,
		})
		return
	}

	if address == "" || tickId == "" {
		ctx.JSON(http.StatusOK, respond.ErrParameterError)
		return
	}

	log.Printf("[ADMIN] RecalculateBalance request: chain=%s, address=%s, tickId=%s", chainName, address, tickId)

	// 获取重算前的余额
	oldBalance, _ := man.PebbleStore.GetMrc20AccountBalance(chainName, address, tickId)
	var oldBalanceStr string
	if oldBalance != nil {
		oldBalanceStr = oldBalance.Balance.String()
	} else {
		oldBalanceStr = "0"
	}

	// 执行重算
	err := man.PebbleStore.RecalculateBalance(chainName, address, tickId)
	if err != nil {
		log.Printf("[ADMIN] RecalculateBalance failed: %v", err)
		ctx.JSON(http.StatusOK, &respond.ApiResponse{
			Code: -1,
			Msg:  fmt.Sprintf("RecalculateBalance failed: %v", err),
			Data: nil,
		})
		return
	}

	// 获取重算后的余额
	newBalance, _ := man.PebbleStore.GetMrc20AccountBalance(chainName, address, tickId)
	var newBalanceStr string
	if newBalance != nil {
		newBalanceStr = newBalance.Balance.String()
	} else {
		newBalanceStr = "0"
	}

	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
		"chain":      chainName,
		"address":    address,
		"tickId":     tickId,
		"oldBalance": oldBalanceStr,
		"newBalance": newBalanceStr,
		"message":    "Balance recalculated successfully",
	}))
}

// verifyBalance 验证余额是否正确（缓存与 UTXO 一致）
// GET /api/mrc20/admin/verify-balance/:chain/:address/:tickId?token=xxx
func verifyBalance(ctx *gin.Context) {
	chainName := strings.ToLower(ctx.Param("chain"))
	address := ctx.Param("address")
	tickId := ctx.Param("tickId")
	token := ctx.Query("token")

	// 验证链名
	if !isValidChain(chainName) {
		ctx.JSON(http.StatusOK, respond.ErrParameterError)
		return
	}

	// 验证 token（验证操作不需要严格认证，可以放宽）
	if common.Config.AdminToken != "" && token != common.Config.AdminToken {
		// 验证不需要严格认证，但记录日志
		log.Printf("[ADMIN] VerifyBalance without token: chain=%s, address=%s, tickId=%s", chainName, address, tickId)
	}

	if address == "" || tickId == "" {
		ctx.JSON(http.StatusOK, respond.ErrParameterError)
		return
	}

	// 执行验证
	isValid, err := man.PebbleStore.VerifyBalance(chainName, address, tickId)
	if err != nil {
		ctx.JSON(http.StatusOK, &respond.ApiResponse{
			Code: -1,
			Msg:  fmt.Sprintf("VerifyBalance failed: %v", err),
			Data: nil,
		})
		return
	}

	// 获取当前缓存余额
	cachedBalance, _ := man.PebbleStore.GetMrc20AccountBalance(chainName, address, tickId)
	var cachedBalanceStr string
	if cachedBalance != nil {
		cachedBalanceStr = cachedBalance.Balance.String()
	} else {
		cachedBalanceStr = "0"
	}

	status := "VALID"
	if !isValid {
		status = "MISMATCH"
	}

	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
		"chain":         chainName,
		"address":       address,
		"tickId":        tickId,
		"cachedBalance": cachedBalanceStr,
		"status":        status,
		"isValid":       isValid,
	}))
}

// fixPendingUtxos 修复 pending 状态的 UTXO
// GET /api/mrc20/admin/fix-pending/:chain?token=xxx
func fixPendingUtxos(ctx *gin.Context) {
	chainName := strings.ToLower(ctx.Param("chain"))
	token := ctx.Query("token")

	// 验证链名
	if !isValidChain(chainName) {
		ctx.JSON(http.StatusOK, respond.ErrParameterError)
		return
	}

	// 验证 token
	if common.Config.AdminToken != "" && token != common.Config.AdminToken {
		ctx.JSON(http.StatusUnauthorized, &respond.ApiResponse{
			Code: 401,
			Msg:  "Unauthorized: invalid admin token",
			Data: nil,
		})
		return
	}

	log.Printf("[ADMIN] FixPendingUtxos request: chain=%s", chainName)

	// 执行修复
	fixedCount, err := man.PebbleStore.FixPendingUtxoStatus(chainName)
	if err != nil {
		log.Printf("[ADMIN] FixPendingUtxos failed: %v", err)
		ctx.JSON(http.StatusOK, &respond.ApiResponse{
			Code: -1,
			Msg:  fmt.Sprintf("FixPendingUtxos failed: %v", err),
			Data: nil,
		})
		return
	}

	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
		"chain":      chainName,
		"fixedCount": fixedCount,
		"message":    fmt.Sprintf("Fixed %d pending UTXOs", fixedCount),
	}))
}

// ============== 快照管理接口 ==============

// createSnapshot 创建快照
// POST /api/mrc20/admin/snapshot/create?token=xxx
// Body: {"description": "snapshot description"}
func createSnapshot(ctx *gin.Context) {
	token := ctx.Query("token")

	// 验证 token
	if common.Config.AdminToken != "" && token != common.Config.AdminToken {
		ctx.JSON(http.StatusUnauthorized, &respond.ApiResponse{
			Code: 401,
			Msg:  "Unauthorized: invalid admin token",
			Data: nil,
		})
		return
	}

	// 获取描述
	var req struct {
		Description string `json:"description"`
	}
	if err := ctx.ShouldBindJSON(&req); err != nil {
		req.Description = ctx.Query("description")
	}
	if req.Description == "" {
		req.Description = "Manual snapshot"
	}

	snapshotDir := man.GetSnapshotDir(common.Config.Pebble.Dir)
	log.Printf("[ADMIN] CreateSnapshot request: dir=%s, description=%s", snapshotDir, req.Description)

	// 执行快照创建
	metadata, err := man.PebbleStore.CreateSnapshot(snapshotDir, req.Description)
	if err != nil {
		log.Printf("[ADMIN] CreateSnapshot failed: %v", err)
		ctx.JSON(http.StatusOK, &respond.ApiResponse{
			Code: -1,
			Msg:  fmt.Sprintf("CreateSnapshot failed: %v", err),
			Data: nil,
		})
		return
	}

	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
		"snapshotId":   metadata.ID,
		"createdAt":    metadata.CreatedAt,
		"description":  metadata.Description,
		"chainHeights": metadata.ChainHeights,
		"recordCounts": metadata.RecordCounts,
		"fileSize":     metadata.FileSize,
		"message":      "Snapshot created successfully",
	}))
}

// listSnapshots 列出所有快照
// GET /api/mrc20/admin/snapshot/list?token=xxx
func listSnapshots(ctx *gin.Context) {
	token := ctx.Query("token")

	// 验证 token
	if common.Config.AdminToken != "" && token != common.Config.AdminToken {
		ctx.JSON(http.StatusUnauthorized, &respond.ApiResponse{
			Code: 401,
			Msg:  "Unauthorized: invalid admin token",
			Data: nil,
		})
		return
	}

	snapshotDir := man.GetSnapshotDir(common.Config.Pebble.Dir)
	snapshots, err := man.ListSnapshots(snapshotDir)
	if err != nil {
		log.Printf("[ADMIN] ListSnapshots failed: %v", err)
		ctx.JSON(http.StatusOK, &respond.ApiResponse{
			Code: -1,
			Msg:  fmt.Sprintf("ListSnapshots failed: %v", err),
			Data: nil,
		})
		return
	}

	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
		"snapshots": snapshots,
		"count":     len(snapshots),
	}))
}

// getSnapshotInfo 获取快照信息
// GET /api/mrc20/admin/snapshot/info/:id?token=xxx
func getSnapshotInfo(ctx *gin.Context) {
	snapshotID := ctx.Param("id")
	token := ctx.Query("token")

	// 验证 token
	if common.Config.AdminToken != "" && token != common.Config.AdminToken {
		ctx.JSON(http.StatusUnauthorized, &respond.ApiResponse{
			Code: 401,
			Msg:  "Unauthorized: invalid admin token",
			Data: nil,
		})
		return
	}

	snapshotDir := man.GetSnapshotDir(common.Config.Pebble.Dir)
	metadata, err := man.GetSnapshotInfo(snapshotDir, snapshotID)
	if err != nil {
		ctx.JSON(http.StatusOK, &respond.ApiResponse{
			Code: -1,
			Msg:  fmt.Sprintf("Snapshot not found: %v", err),
			Data: nil,
		})
		return
	}

	// 验证快照完整性
	verifyErr := man.VerifySnapshot(snapshotDir, snapshotID)

	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
		"snapshotId":   metadata.ID,
		"createdAt":    metadata.CreatedAt,
		"description":  metadata.Description,
		"chainHeights": metadata.ChainHeights,
		"recordCounts": metadata.RecordCounts,
		"fileSize":     metadata.FileSize,
		"valid":        verifyErr == nil,
		"verifyError": func() string {
			if verifyErr != nil {
				return verifyErr.Error()
			} else {
				return ""
			}
		}(),
	}))
}

// restoreSnapshot 从快照恢复
// POST /api/mrc20/admin/snapshot/restore/:id?token=xxx
// 警告：此操作会清空现有 MRC20 数据！
func restoreSnapshot(ctx *gin.Context) {
	snapshotID := ctx.Param("id")
	token := ctx.Query("token")

	// 验证 token
	if common.Config.AdminToken != "" && token != common.Config.AdminToken {
		ctx.JSON(http.StatusUnauthorized, &respond.ApiResponse{
			Code: 401,
			Msg:  "Unauthorized: invalid admin token",
			Data: nil,
		})
		return
	}

	snapshotDir := man.GetSnapshotDir(common.Config.Pebble.Dir)
	snapshotPath := snapshotDir + "/" + snapshotID

	log.Printf("[ADMIN] RestoreSnapshot request: id=%s", snapshotID)

	// 先验证快照
	if err := man.VerifySnapshot(snapshotDir, snapshotID); err != nil {
		ctx.JSON(http.StatusOK, &respond.ApiResponse{
			Code: -1,
			Msg:  fmt.Sprintf("Snapshot verification failed: %v", err),
			Data: nil,
		})
		return
	}

	// 执行恢复
	metadata, err := man.PebbleStore.RestoreSnapshot(snapshotPath)
	if err != nil {
		log.Printf("[ADMIN] RestoreSnapshot failed: %v", err)
		ctx.JSON(http.StatusOK, &respond.ApiResponse{
			Code: -1,
			Msg:  fmt.Sprintf("RestoreSnapshot failed: %v", err),
			Data: nil,
		})
		return
	}

	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
		"snapshotId":   metadata.ID,
		"createdAt":    metadata.CreatedAt,
		"chainHeights": metadata.ChainHeights,
		"message":      "Snapshot restored successfully. Please restart the indexer to continue from the snapshot height.",
	}))
}

// deleteSnapshot 删除快照
// DELETE /api/mrc20/admin/snapshot/:id?token=xxx
func deleteSnapshot(ctx *gin.Context) {
	snapshotID := ctx.Param("id")
	token := ctx.Query("token")

	// 验证 token
	if common.Config.AdminToken != "" && token != common.Config.AdminToken {
		ctx.JSON(http.StatusUnauthorized, &respond.ApiResponse{
			Code: 401,
			Msg:  "Unauthorized: invalid admin token",
			Data: nil,
		})
		return
	}

	snapshotDir := man.GetSnapshotDir(common.Config.Pebble.Dir)

	log.Printf("[ADMIN] DeleteSnapshot request: id=%s", snapshotID)

	if err := man.DeleteSnapshot(snapshotDir, snapshotID); err != nil {
		ctx.JSON(http.StatusOK, &respond.ApiResponse{
			Code: -1,
			Msg:  fmt.Sprintf("DeleteSnapshot failed: %v", err),
			Data: nil,
		})
		return
	}

	ctx.JSON(http.StatusOK, respond.ApiSuccess(1, "ok", gin.H{
		"snapshotId": snapshotID,
		"message":    "Snapshot deleted successfully",
	}))
}
