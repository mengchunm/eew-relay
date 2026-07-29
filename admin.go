package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const adminSessionCookie = "eew_admin_session"

var adminProcessStartedAt = time.Now()

type adminAuth struct {
	username     string
	password     string
	sessionTTL   time.Duration
	signingKey   [32]byte
	loginLimiter *clientRateLimiter
	testLimiter  *clientRateLimiter
}

type adminSessionClaims struct {
	Username  string `json:"u"`
	ExpiresAt int64  `json:"e"`
}

type adminAuditSummary struct {
	ID       string    `json:"id"`
	Modified time.Time `json:"modified_at"`
	deliveryAuditSummary
}

type adminBatchSubscriptionsRequest struct {
	Subscriptions []Subscription `json:"subscriptions"`
	AllowUpdates  bool           `json:"allow_updates"`
}

type adminBatchDeleteRequest struct {
	BarkIDs []string `json:"bark_ids"`
}

type adminTestRequest struct {
	BarkID string `json:"bark_id"`
	Kind   string `json:"kind"`
}

func newAdminAuth(cfg ServerConfig) *adminAuth {
	username := strings.TrimSpace(cfg.AdminUsername)
	password := cfg.AdminPassword
	ttl := time.Duration(cfg.AdminSessionHours) * time.Hour
	if ttl <= 0 {
		ttl = 8 * time.Hour
	}
	if ttl > 24*time.Hour {
		ttl = 24 * time.Hour
	}
	auth := &adminAuth{
		username:     username,
		password:     password,
		sessionTTL:   ttl,
		loginLimiter: newClientRateLimiter(10, 5),
		testLimiter:  newClientRateLimiter(12, 4),
	}
	auth.signingKey = sha256.Sum256([]byte("eew-admin-session-v1\x00" + username + "\x00" + password))
	return auth
}

func (a *adminAuth) enabled() bool {
	return a != nil && a.username != "" && a.password != ""
}

func (a *adminAuth) credentialsValid(username, password string) bool {
	if !a.enabled() || len(username) != len(a.username) || len(password) != len(a.password) {
		return false
	}
	userOK := subtle.ConstantTimeCompare([]byte(username), []byte(a.username))
	passwordOK := subtle.ConstantTimeCompare([]byte(password), []byte(a.password))
	return userOK&passwordOK == 1
}

func (a *adminAuth) issueSession(now time.Time) (string, error) {
	claims := adminSessionClaims{Username: a.username, ExpiresAt: now.Add(a.sessionTTL).Unix()}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, a.signingKey[:])
	_, _ = mac.Write([]byte(encoded))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return encoded + "." + signature, nil
}

func (a *adminAuth) validSession(token string, now time.Time) bool {
	if !a.enabled() {
		return false
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return false
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, a.signingKey[:])
	_, _ = mac.Write([]byte(parts[0]))
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return false
	}
	var claims adminSessionClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return false
	}
	return claims.Username == a.username && claims.ExpiresAt > now.Unix()
}

func (a *adminAuth) authenticated(r *http.Request) bool {
	if username, password, ok := r.BasicAuth(); ok && a.credentialsValid(username, password) {
		return true
	}
	cookie, err := r.Cookie(adminSessionCookie)
	return err == nil && a.validSession(cookie.Value, time.Now())
}

func (a *adminAuth) require(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !a.enabled() {
			writeJSON(w, http.StatusServiceUnavailable, APIResponse{Success: false, Message: "管理员后台尚未配置"})
			return
		}
		if !a.authenticated(r) {
			writeJSON(w, http.StatusUnauthorized, APIResponse{Success: false, Message: "管理员登录已失效，请重新登录"})
			return
		}
		next(w, r)
	}
}

func setAdminSessionCookie(w http.ResponseWriter, r *http.Request, value string, maxAge int) {
	http.SetCookie(w, &http.Cookie{
		Name:     adminSessionCookie,
		Value:    value,
		Path:     "/",
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   requestUsesHTTPS(r),
		SameSite: http.SameSiteStrictMode,
	})
}

func registerAdminRoutes(mux *http.ServeMux, cfg Config, store *Store, alertCache *AlertCache, notifier *Notifier, health *RuntimeHealth) {
	auth := newAdminAuth(cfg.Server)
	mux.HandleFunc("GET /admin", func(w http.ResponseWriter, _ *http.Request) {
		data, err := publicFS.ReadFile("public/admin.html")
		if err != nil {
			http.Error(w, "admin page not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(data)
	})
	mux.HandleFunc("GET /admin/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/admin", http.StatusTemporaryRedirect)
	})

	mux.HandleFunc("GET /api/admin/session", func(w http.ResponseWriter, r *http.Request) {
		authenticated := auth.authenticated(r)
		data := map[string]any{"enabled": auth.enabled(), "authenticated": authenticated}
		if authenticated {
			data["username"] = auth.username
		}
		writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "ok", Data: data})
	})
	mux.HandleFunc("POST /api/admin/login", func(w http.ResponseWriter, r *http.Request) {
		if !auth.enabled() {
			writeJSON(w, http.StatusServiceUnavailable, APIResponse{Success: false, Message: "管理员后台尚未配置"})
			return
		}
		if !auth.loginLimiter.Allow(clientIP(r), time.Now()) {
			w.Header().Set("Retry-After", "60")
			writeJSON(w, http.StatusTooManyRequests, APIResponse{Success: false, Message: "登录尝试过于频繁，请稍后再试"})
			return
		}
		var credentials struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&credentials); err != nil || !auth.credentialsValid(strings.TrimSpace(credentials.Username), credentials.Password) {
			writeJSON(w, http.StatusUnauthorized, APIResponse{Success: false, Message: "账号或密码错误"})
			return
		}
		token, err := auth.issueSession(time.Now())
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: "创建登录会话失败"})
			return
		}
		setAdminSessionCookie(w, r, token, int(auth.sessionTTL.Seconds()))
		log.Printf("admin login user=%s ip=%s", auth.username, clientIP(r))
		writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "登录成功", Data: map[string]any{"username": auth.username}})
	})
	mux.HandleFunc("POST /api/admin/logout", func(w http.ResponseWriter, r *http.Request) {
		setAdminSessionCookie(w, r, "", -1)
		writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "已退出登录"})
	})

	mux.HandleFunc("GET /api/admin/overview", auth.require(func(w http.ResponseWriter, r *http.Request) {
		data := buildAdminOverview(r.Context(), cfg, store, notifier, health)
		writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "ok", Data: data})
	}))
	mux.HandleFunc("GET /api/admin/subscriptions", auth.require(func(w http.ResponseWriter, r *http.Request) {
		serveAdminSubscriptions(w, r, store)
	}))
	mux.HandleFunc("POST /api/admin/subscriptions/batch", auth.require(func(w http.ResponseWriter, r *http.Request) {
		serveAdminBatchSubscriptions(w, r, cfg, store)
	}))
	mux.HandleFunc("POST /api/admin/subscriptions/delete", auth.require(func(w http.ResponseWriter, r *http.Request) {
		serveAdminDeleteSubscriptions(w, r, store)
	}))
	mux.HandleFunc("GET /api/admin/audits", auth.require(func(w http.ResponseWriter, r *http.Request) {
		serveAdminAudits(w, r, cfg)
	}))
	mux.HandleFunc("GET /api/admin/audit", auth.require(func(w http.ResponseWriter, r *http.Request) {
		serveAdminAuditDetail(w, r, cfg)
	}))
	mux.HandleFunc("POST /api/admin/test", auth.require(func(w http.ResponseWriter, r *http.Request) {
		if !auth.testLimiter.Allow(clientIP(r), time.Now()) {
			w.Header().Set("Retry-After", "60")
			writeJSON(w, http.StatusTooManyRequests, APIResponse{Success: false, Message: "测试操作过于频繁，请稍后再试"})
			return
		}
		serveAdminTest(w, r, cfg, store, alertCache, notifier)
	}))
}

func parseAdminPage(r *http.Request, defaultLimit, maxLimit int) (int, int) {
	offset, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("offset")))
	limit, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("limit")))
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	return offset, limit
}

func serveAdminSubscriptions(w http.ResponseWriter, r *http.Request, store *Store) {
	query := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
	offset, limit := parseAdminPage(r, 50, 200)
	subscriptions := store.List()
	sort.Slice(subscriptions, func(i, j int) bool {
		if subscriptions[i].UpdatedAt != subscriptions[j].UpdatedAt {
			return subscriptions[i].UpdatedAt > subscriptions[j].UpdatedAt
		}
		return subscriptions[i].BarkID < subscriptions[j].BarkID
	})
	filtered := subscriptions[:0]
	for _, sub := range subscriptions {
		if query != "" && !adminSubscriptionMatches(sub, query) {
			continue
		}
		filtered = append(filtered, sub)
	}
	total := len(filtered)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "ok", Data: map[string]any{
		"items": filtered[offset:end], "total": total, "offset": offset, "limit": limit,
	}})
}

func adminSubscriptionMatches(sub Subscription, query string) bool {
	if strings.Contains(strings.ToLower(sub.BarkID), query) || strings.Contains(strings.ToLower(sub.BarkServer), query) || strings.Contains(strings.ToLower(sub.LocationName), query) {
		return true
	}
	for _, location := range sub.Locations {
		if strings.Contains(strings.ToLower(location.Name), query) {
			return true
		}
	}
	return false
}

func serveAdminBatchSubscriptions(w http.ResponseWriter, r *http.Request, cfg Config, store *Store) {
	var request adminBatchSubscriptionsRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "批量订阅请求格式错误"})
		return
	}
	if len(request.Subscriptions) == 0 || len(request.Subscriptions) > 100 {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "每次需提交 1 至 100 个订阅"})
		return
	}
	prepared := make([]Subscription, 0, len(request.Subscriptions))
	seen := make(map[string]bool, len(request.Subscriptions))
	for index, sub := range request.Subscriptions {
		var err error
		sub.BarkID, sub.BarkServer, err = normalizeBarkInput(sub.BarkID, sub.BarkServer, cfg)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: fmt.Sprintf("第 %d 个订阅：%v", index+1, err)})
			return
		}
		if seen[sub.BarkID] {
			writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: fmt.Sprintf("第 %d 个订阅：Bark Key 在本批次中重复", index+1)})
			return
		}
		seen[sub.BarkID] = true
		if !isAllowedBarkServer(sub.BarkServer, cfg) {
			writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: fmt.Sprintf("第 %d 个订阅：Bark 服务器不受支持", index+1)})
			return
		}
		_, exists := store.Get(sub.BarkID)
		if exists && !request.AllowUpdates {
			writeJSON(w, http.StatusConflict, APIResponse{Success: false, Message: fmt.Sprintf("第 %d 个订阅已存在；为避免覆盖，本次未写入任何订阅", index+1), Data: map[string]any{"bark_id": sub.BarkID}})
			return
		}
		if !exists && isSelfHostedBarkServer(sub.BarkServer, cfg) {
			existsInBark, err := selfHostedBarkKeyExists(cfg, sub.BarkID)
			if err != nil {
				log.Printf("admin verify bark key failed key=%s: %v", maskKey(sub.BarkID), err)
				writeJSON(w, http.StatusServiceUnavailable, APIResponse{Success: false, Message: fmt.Sprintf("第 %d 个订阅：暂时无法验证 Bark Key", index+1)})
				return
			}
			if !existsInBark {
				writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: fmt.Sprintf("第 %d 个订阅：自建 Bark 中未找到该 Key", index+1)})
				return
			}
		}
		normalizeSubscription(&sub)
		if err := validateSubscription(sub); err != nil {
			writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: fmt.Sprintf("第 %d 个订阅：%v", index+1, err)})
			return
		}
		if err := canonicalizeSubscriptionLocations(r.Context(), store, &sub, true); err != nil {
			writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: fmt.Sprintf("第 %d 个订阅：%v", index+1, err)})
			return
		}
		if err := validateSubscription(sub); err != nil {
			writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: fmt.Sprintf("第 %d 个订阅：%v", index+1, err)})
			return
		}
		prepared = append(prepared, sub)
	}

	previous := make(map[string]Subscription, len(prepared))
	applied := make([]Subscription, 0, len(prepared))
	created, updated := 0, 0
	for _, sub := range prepared {
		if old, ok := store.Get(sub.BarkID); ok {
			previous[sub.BarkID] = old
		}
		wasCreated, err := store.UpsertWithLimit(sub, 0)
		if err != nil {
			rollbackAdminSubscriptions(store, applied, previous)
			log.Printf("admin batch subscription rollback count=%d: %v", len(applied), err)
			writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: "保存订阅失败，已尝试回滚本批次"})
			return
		}
		applied = append(applied, sub)
		if wasCreated {
			created++
		} else {
			updated++
		}
	}
	log.Printf("admin batch subscriptions created=%d updated=%d ip=%s", created, updated, clientIP(r))
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "批量订阅已保存", Data: map[string]any{"created": created, "updated": updated, "total": len(prepared)}})
}

func rollbackAdminSubscriptions(store *Store, attempted []Subscription, previous map[string]Subscription) {
	for index := len(attempted) - 1; index >= 0; index-- {
		sub := attempted[index]
		if old, ok := previous[sub.BarkID]; ok {
			_ = store.Upsert(old)
		} else {
			_ = store.Delete(sub.BarkID)
		}
	}
}

func serveAdminDeleteSubscriptions(w http.ResponseWriter, r *http.Request, store *Store) {
	var request adminBatchDeleteRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || len(request.BarkIDs) == 0 || len(request.BarkIDs) > 100 {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "每次需提交 1 至 100 个 Bark Key"})
		return
	}
	keys := make([]string, 0, len(request.BarkIDs))
	previous := make(map[string]Subscription, len(request.BarkIDs))
	seen := make(map[string]bool, len(request.BarkIDs))
	for index, value := range request.BarkIDs {
		key, err := normalizeBarkIDInput(value)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: fmt.Sprintf("第 %d 个 Bark Key 无效", index+1)})
			return
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		keys = append(keys, key)
		if old, ok := store.Get(key); ok {
			previous[key] = old
		}
	}
	deleted := 0
	deletedKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		if _, ok := previous[key]; !ok {
			continue
		}
		if err := store.Delete(key); err != nil {
			for _, deletedKey := range deletedKeys {
				_ = store.Upsert(previous[deletedKey])
			}
			writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: "删除订阅失败，已尝试恢复本批次"})
			return
		}
		deleted++
		deletedKeys = append(deletedKeys, key)
	}
	log.Printf("admin delete subscriptions deleted=%d requested=%d ip=%s", deleted, len(keys), clientIP(r))
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "订阅已删除", Data: map[string]any{"deleted": deleted, "not_found": len(keys) - deleted}})
}

func buildAdminOverview(ctx context.Context, cfg Config, store *Store, notifier *Notifier, health *RuntimeHealth) map[string]any {
	now := time.Now()
	staleAfter := time.Duration(cfg.Wolfx.HealthStaleSeconds) * time.Second
	if staleAfter <= 0 {
		staleAfter = 180 * time.Second
	}
	runtimeSnapshot := health.Snapshot()
	dataSourceHealthy := health.DataSourceHealthy(now, staleAfter)
	storageType := "json"
	storageHealthy := true
	storageError := ""
	store.mu.RLock()
	backend := store.backend
	store.mu.RUnlock()
	if backend != nil {
		storageType = "postgresql"
	}
	if postgres, ok := backend.(*postgresSubscriptionBackend); ok {
		checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := postgres.db.PingContext(checkCtx)
		cancel()
		if err != nil {
			storageHealthy = false
			storageError = err.Error()
		}
	}
	queueStatus := "disabled"
	queueConnected := false
	queueLastError := ""
	if notifier != nil && notifier.queue != nil && notifier.queue.nc != nil {
		queueStatus = notifier.queue.nc.Status().String()
		queueConnected = notifier.queue.nc.IsConnected()
		if err := notifier.queue.nc.LastError(); err != nil {
			queueLastError = err.Error()
		}
	}
	_, auditTotal, auditErr := listDeliveryAuditSummaries(cfg, 1)
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	paused, pausedMessage, pausedReason := subscriptionPauseState(cfg, store.Count())
	checks := []map[string]any{
		{"name": "地震数据源", "healthy": dataSourceHealthy, "detail": dataSourceCheckDetail(runtimeSnapshot, dataSourceHealthy)},
		{"name": "订阅存储", "healthy": storageHealthy, "detail": storageType, "error": storageError},
		{"name": "推送队列", "healthy": notifier == nil || notifier.queue == nil || queueConnected, "detail": queueStatus, "error": queueLastError},
		{"name": "投递审计", "healthy": auditErr == nil, "detail": fmt.Sprintf("%d 条记录，保留 %d 天", auditTotal, cfg.Server.AuditRetentionDays), "error": errorString(auditErr)},
	}
	return map[string]any{
		"checked_at":                 now.UTC().Format(time.RFC3339),
		"started_at":                 adminProcessStartedAt.UTC().Format(time.RFC3339),
		"uptime_seconds":             int64(now.Sub(adminProcessStartedAt).Seconds()),
		"subscriptions":              store.Count(),
		"subscription_paused":        paused,
		"subscription_pause_reason":  pausedReason,
		"subscription_pause_message": pausedMessage,
		"runtime":                    runtimeSnapshot,
		"data_source_healthy":        dataSourceHealthy,
		"storage":                    map[string]any{"type": storageType, "healthy": storageHealthy, "error": storageError},
		"queue":                      map[string]any{"enabled": notifier != nil && notifier.queue != nil, "connected": queueConnected, "status": queueStatus, "last_error": queueLastError},
		"relay":                      map[string]any{"enabled": notifier != nil && notifier.relay != nil},
		"audit":                      map[string]any{"records": auditTotal, "retention_days": cfg.Server.AuditRetentionDays, "error": errorString(auditErr)},
		"process":                    map[string]any{"go_version": runtime.Version(), "goroutines": runtime.NumGoroutine(), "memory_alloc_bytes": memory.Alloc, "memory_sys_bytes": memory.Sys},
		"default_bark_server":        normalizeBarkServer("", cfg),
		"self_hosted_bark_server":    strings.TrimRight(strings.TrimSpace(cfg.Bark.SelfHostedServer), "/"),
		"checks":                     checks,
	}
}

func dataSourceCheckDetail(snapshot runtimeHealthSnapshot, healthy bool) string {
	if healthy {
		return "连接正常，最近消息 " + snapshot.LastMessageAt
	}
	if !snapshot.WebSocketConnected {
		return "连接已断开"
	}
	return "连接存在，但数据已超时"
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func auditDirectory(cfg Config) string {
	if path := strings.TrimSpace(cfg.Server.AuditPath); path != "" {
		return path
	}
	return filepath.Join(filepath.Dir(cfg.Server.DataPath), "audit")
}

func listDeliveryAuditSummaries(cfg Config, limit int) ([]adminAuditSummary, int, error) {
	directory := auditDirectory(cfg)
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return []adminAuditSummary{}, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	items := make([]adminAuditSummary, 0)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".summary.json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			continue
		}
		var summary deliveryAuditSummary
		if len(data) > 2<<20 || json.Unmarshal(data, &summary) != nil {
			continue
		}
		info, _ := entry.Info()
		modified := time.Time{}
		if info != nil {
			modified = info.ModTime()
		}
		summary.DetailPath = ""
		items = append(items, adminAuditSummary{
			ID:                   strings.TrimSuffix(entry.Name(), ".summary.json"),
			Modified:             modified,
			deliveryAuditSummary: summary,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		if !items[i].Modified.Equal(items[j].Modified) {
			return items[i].Modified.After(items[j].Modified)
		}
		return items[i].ID > items[j].ID
	})
	total := len(items)
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items, total, nil
}

func serveAdminAudits(w http.ResponseWriter, r *http.Request, cfg Config) {
	_, limit := parseAdminPage(r, 100, 500)
	items, total, err := listDeliveryAuditSummaries(cfg, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: "读取投递审计失败"})
		return
	}
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "ok", Data: map[string]any{"items": items, "total": total}})
}

func validAuditID(value string) bool {
	return value != "" && len(value) <= 240 && sanitizeFilePart(value) == value
}

func serveAdminAuditDetail(w http.ResponseWriter, r *http.Request, cfg Config) {
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	if !validAuditID(id) {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "投递审计 ID 无效"})
		return
	}
	directory := auditDirectory(cfg)
	summaryData, err := os.ReadFile(filepath.Join(directory, id+".summary.json"))
	if errors.Is(err, os.ErrNotExist) {
		writeJSON(w, http.StatusNotFound, APIResponse{Success: false, Message: "投递审计不存在"})
		return
	}
	var summary deliveryAuditSummary
	if err != nil || len(summaryData) > 2<<20 || json.Unmarshal(summaryData, &summary) != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: "投递审计摘要损坏"})
		return
	}
	summary.DetailPath = ""
	file, err := os.Open(filepath.Join(directory, id+".jsonl.gz"))
	compressed := err == nil
	if errors.Is(err, os.ErrNotExist) {
		file, err = os.Open(filepath.Join(directory, id+".jsonl"))
		compressed = false
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: "投递审计明细不存在"})
		return
	}
	defer file.Close()
	var reader io.Reader = file
	if compressed {
		gz, err := gzip.NewReader(file)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: "投递审计明细损坏"})
			return
		}
		defer gz.Close()
		reader = gz
	}
	offset, limit := parseAdminPage(r, 0, 500)
	if limit == 0 {
		limit = 200
	}
	statusFilter := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("status")))
	rawQuery := strings.TrimSpace(r.URL.Query().Get("q"))
	query := strings.ToLower(rawQuery)
	queryKeyHash := ""
	if validateBarkID(rawQuery) == nil {
		queryKeyHash = hashKey(rawQuery)
	}
	records := make([]deliveryAuditRecord, 0, limit)
	total := 0
	scanner := bufio.NewScanner(io.LimitReader(reader, 64<<20))
	scanner.Buffer(make([]byte, 64<<10), 2<<20)
	for scanner.Scan() {
		var record deliveryAuditRecord
		if json.Unmarshal(scanner.Bytes(), &record) != nil {
			continue
		}
		if statusFilter != "" && record.Status != statusFilter {
			continue
		}
		if query != "" && record.BarkHash != queryKeyHash && !strings.Contains(strings.ToLower(record.BarkMasked+" "+record.BarkHash+" "+record.LocationName+" "+record.Error+" "+record.Reason), query) {
			continue
		}
		if total >= offset && len(records) < limit {
			records = append(records, record)
		}
		total++
	}
	if err := scanner.Err(); err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: "读取投递审计明细失败"})
		return
	}
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "ok", Data: map[string]any{
		"id": id, "summary": summary, "records": records, "total": total, "offset": offset, "limit": limit,
	}})
}

func serveAdminTest(w http.ResponseWriter, r *http.Request, cfg Config, store *Store, alertCache *AlertCache, notifier *Notifier) {
	var request adminTestRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "测试请求格式错误"})
		return
	}
	barkID, _, err := normalizeBarkInput(request.BarkID, "", cfg)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: err.Error()})
		return
	}
	sub, ok := store.Get(barkID)
	if !ok {
		writeJSON(w, http.StatusNotFound, APIResponse{Success: false, Message: "未找到该订阅"})
		return
	}
	kind := strings.ToLower(strings.TrimSpace(request.Kind))
	if kind == "" || kind == "connectivity" {
		params := map[string]string{}
		addBarkIconParam(cfg, params, "passive")
		retries, err := notifier.SendWithRetry(r.Context(), normalizeBarkServer(sub.BarkServer, cfg), sub.BarkID, "EEW 管理员测试", "系统连通性", "推送网关、Bark Key 与设备通知链路工作正常。", params, PushOptions{Level: "passive"})
		if err != nil {
			log.Printf("admin connectivity test failed bark=%s retries=%d: %v", maskKey(sub.BarkID), retries, err)
			writeJSON(w, http.StatusBadGateway, APIResponse{Success: false, Message: "测试推送失败", Data: map[string]any{"retries": retries, "error": err.Error()}})
			return
		}
		log.Printf("admin connectivity test sent bark=%s retries=%d", maskKey(sub.BarkID), retries)
		writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "连通性测试通知已发送", Data: map[string]any{"retries": retries}})
		return
	}
	if kind != "small" && kind != "medium" && kind != "large" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "测试类型无效"})
		return
	}
	event := simulatedEvent([]Subscription{sub}, kind)
	selectedSub, decision := nearestSubscriptionForEvent(cfg, sub, event)
	if notifyLevelForEvent(event, selectedSub, decision) == "" {
		writeJSON(w, http.StatusUnprocessableEntity, APIResponse{Success: false, Message: "当前订阅规则不会触发该模拟测试"})
		return
	}
	pushed, skipped := dispatchOne(r.Context(), cfg, notifier, alertCache, event, sub)
	if pushed == 0 {
		writeJSON(w, http.StatusBadGateway, APIResponse{Success: false, Message: "模拟预警发送失败或被跳过", Data: map[string]any{"skipped": skipped}})
		return
	}
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "模拟地震预警已发送", Data: map[string]any{"event_id": event.EventID, "pushed": pushed, "skipped": skipped}})
}
