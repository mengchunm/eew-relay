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
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
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

type adminServiceStatus struct {
	ID            string              `json:"id"`
	Name          string              `json:"name"`
	Status        string              `json:"status"`
	Healthy       bool                `json:"healthy"`
	Enabled       bool                `json:"enabled"`
	Detail        string              `json:"detail,omitempty"`
	Error         string              `json:"error,omitempty"`
	LatencyMS     int64               `json:"latency_ms,omitempty"`
	LastSuccessAt string              `json:"last_success_at,omitempty"`
	Metrics       map[string]any      `json:"metrics,omitempty"`
	Workers       []queueWorkerHealth `json:"workers,omitempty"`
}

type adminServiceHealthSnapshot struct {
	CheckedAt         string               `json:"checked_at"`
	DurationMS        int64                `json:"duration_ms"`
	Status            string               `json:"status"`
	Healthy           bool                 `json:"healthy"`
	EnabledServices   int                  `json:"enabled_services"`
	HealthyServices   int                  `json:"healthy_services"`
	DegradedServices  int                  `json:"degraded_services"`
	UnhealthyServices int                  `json:"unhealthy_services"`
	Services          []adminServiceStatus `json:"services"`
}

type adminServiceMonitor struct {
	cfg         Config
	store       *Store
	notifier    *Notifier
	health      *RuntimeHealth
	httpClient  *http.Client
	probeMu     sync.Mutex
	cached      adminServiceHealthSnapshot
	cachedAt    time.Time
	mu          sync.Mutex
	lastSuccess map[string]time.Time
}

type adminAuditSummary struct {
	ID       string    `json:"id"`
	Modified time.Time `json:"modified_at"`
	deliveryAuditSummary
}

type adminAuditEventGroup struct {
	ID            string              `json:"id"`
	Title         string              `json:"title"`
	EventID       string              `json:"event_id"`
	EventIDs      []string            `json:"event_ids"`
	Type          string              `json:"type"`
	Source        string              `json:"source"`
	Sources       []string            `json:"sources"`
	OriginTime    string              `json:"origin_time"`
	Hypocenter    string              `json:"hypocenter,omitempty"`
	Latitude      float64             `json:"latitude,omitempty"`
	Longitude     float64             `json:"longitude,omitempty"`
	Magnitude     float64             `json:"magnitude,omitempty"`
	DepthKM       float64             `json:"depth_km,omitempty"`
	MaxIntensity  string              `json:"max_intensity,omitempty"`
	Final         bool                `json:"final,omitempty"`
	Cancel        bool                `json:"cancel,omitempty"`
	FirstReceived string              `json:"first_received_at"`
	LastReceived  string              `json:"last_received_at"`
	ReportCount   int                 `json:"report_count"`
	TotalPushed   int                 `json:"total_pushed"`
	TotalFiltered int                 `json:"total_filtered"`
	TotalSkipped  int                 `json:"total_skipped"`
	TotalFailed   int                 `json:"total_failed"`
	Latest        adminAuditSummary   `json:"latest"`
	Reports       []adminAuditSummary `json:"reports"`
}

type auditHistoryCandidate struct {
	Record     HistoryRecord
	OriginTime time.Time
}

type auditHistoryLookup struct {
	ByID       map[string]HistoryRecord
	Candidates []auditHistoryCandidate
}

type adminBatchSubscriptionsRequest struct {
	Subscriptions []Subscription `json:"subscriptions"`
	AllowUpdates  bool           `json:"allow_updates"`
}

type adminBatchDeleteRequest struct {
	BarkIDs []string `json:"bark_ids"`
}

type adminSubscriptionFilter struct {
	Query       string `json:"q"`
	Server      string `json:"server"`
	NotifyLevel string `json:"notify_level"`
	CreatedFrom string `json:"created_from"`
	CreatedTo   string `json:"created_to"`
}

type adminSubscriptionLivenessRequest struct {
	BarkIDs []string `json:"bark_ids"`
	adminSubscriptionFilter
}

type adminTestRequest struct {
	BarkID string `json:"bark_id"`
	Kind   string `json:"kind"`
}

type adminSubscriptionLivenessIssue struct {
	BarkID     string `json:"bark_id"`
	BarkServer string `json:"bark_server"`
	Status     string `json:"status"`
	Message    string `json:"message"`
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
	serviceMonitor := newAdminServiceMonitor(cfg, store, notifier, health)
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
	mux.HandleFunc("GET /api/admin/services", auth.require(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		data := serviceMonitor.Snapshot(r.Context())
		writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "ok", Data: data})
	}))
	mux.HandleFunc("GET /api/admin/subscriptions", auth.require(func(w http.ResponseWriter, r *http.Request) {
		serveAdminSubscriptions(w, r, cfg, store)
	}))
	mux.HandleFunc("POST /api/admin/subscriptions/batch", auth.require(func(w http.ResponseWriter, r *http.Request) {
		serveAdminBatchSubscriptions(w, r, cfg, store)
	}))
	mux.HandleFunc("POST /api/admin/subscriptions/delete", auth.require(func(w http.ResponseWriter, r *http.Request) {
		serveAdminDeleteSubscriptions(w, r, store)
	}))
	mux.HandleFunc("POST /api/admin/subscriptions/liveness", auth.require(func(w http.ResponseWriter, r *http.Request) {
		serveAdminSubscriptionLiveness(w, r, cfg, store)
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

func serveAdminSubscriptions(w http.ResponseWriter, r *http.Request, cfg Config, store *Store) {
	filter := adminSubscriptionFilter{
		Query:       r.URL.Query().Get("q"),
		Server:      r.URL.Query().Get("server"),
		NotifyLevel: r.URL.Query().Get("notify_level"),
		CreatedFrom: r.URL.Query().Get("created_from"),
		CreatedTo:   r.URL.Query().Get("created_to"),
	}
	sortBy, sortOrder := normalizeAdminSubscriptionSort(r.URL.Query().Get("sort_by"), r.URL.Query().Get("sort_order"))
	offset, limit := parseAdminPage(r, 50, 200)
	filtered := filterAdminSubscriptions(cfg, store.List(), filter)
	sortAdminSubscriptions(filtered, sortBy, sortOrder)
	total := len(filtered)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "ok", Data: map[string]any{
		"items": filtered[offset:end], "total": total, "offset": offset, "limit": limit, "sort_by": sortBy, "sort_order": sortOrder,
	}})
}

func filterAdminSubscriptions(cfg Config, subscriptions []Subscription, filter adminSubscriptionFilter) []Subscription {
	query := strings.ToLower(strings.TrimSpace(filter.Query))
	serverFilter := strings.ToLower(strings.TrimSpace(filter.Server))
	levelFilter := ""
	if requestedLevel := strings.TrimSpace(filter.NotifyLevel); validNotifyLevel(requestedLevel) {
		levelFilter = normalizeNotifyLevel(requestedLevel)
	}
	createdFrom, createdFromOK := parseAdminFilterDate(filter.CreatedFrom, false)
	createdTo, createdToOK := parseAdminFilterDate(filter.CreatedTo, true)
	filtered := make([]Subscription, 0, len(subscriptions))
	for _, sub := range subscriptions {
		if query != "" && !adminSubscriptionMatches(sub, query) {
			continue
		}
		switch serverFilter {
		case "official":
			if !isOfficialBarkServer(sub.BarkServer) {
				continue
			}
		case "self_hosted":
			if !isSelfHostedBarkServer(sub.BarkServer, cfg) {
				continue
			}
		}
		if levelFilter != "" && !adminSubscriptionHasLevel(sub, levelFilter) {
			continue
		}
		createdAt := time.UnixMilli(sub.CreatedAt)
		if createdFromOK && createdAt.Before(createdFrom) {
			continue
		}
		if createdToOK && !createdAt.Before(createdTo) {
			continue
		}
		filtered = append(filtered, sub)
	}
	return filtered
}

func normalizeAdminSubscriptionSort(sortBy, sortOrder string) (string, string) {
	sortBy = strings.ToLower(strings.TrimSpace(sortBy))
	switch sortBy {
	case "bark_id", "server", "location", "notify_level", "updated_at":
	default:
		sortBy = "updated_at"
	}
	sortOrder = strings.ToLower(strings.TrimSpace(sortOrder))
	if sortOrder != "asc" && sortOrder != "desc" {
		if sortBy == "updated_at" {
			sortOrder = "desc"
		} else {
			sortOrder = "asc"
		}
	}
	return sortBy, sortOrder
}

func sortAdminSubscriptions(subscriptions []Subscription, sortBy, sortOrder string) {
	sort.SliceStable(subscriptions, func(i, j int) bool {
		comparison := compareAdminSubscriptions(subscriptions[i], subscriptions[j], sortBy)
		if comparison == 0 {
			comparison = strings.Compare(strings.ToLower(subscriptions[i].BarkID), strings.ToLower(subscriptions[j].BarkID))
		}
		if sortOrder == "desc" {
			return comparison > 0
		}
		return comparison < 0
	})
}

func compareAdminSubscriptions(left, right Subscription, sortBy string) int {
	switch sortBy {
	case "bark_id":
		return strings.Compare(strings.ToLower(left.BarkID), strings.ToLower(right.BarkID))
	case "server":
		return strings.Compare(strings.ToLower(left.BarkServer), strings.ToLower(right.BarkServer))
	case "location":
		return strings.Compare(adminSubscriptionLocationSortKey(left), adminSubscriptionLocationSortKey(right))
	case "notify_level":
		return strings.Compare(adminSubscriptionNotifySortKey(left), adminSubscriptionNotifySortKey(right))
	default:
		if left.UpdatedAt < right.UpdatedAt {
			return -1
		}
		if left.UpdatedAt > right.UpdatedAt {
			return 1
		}
		return 0
	}
}

func adminSubscriptionLocationSortKey(sub Subscription) string {
	if name := strings.TrimSpace(sub.LocationName); name != "" {
		return strings.ToLower(name)
	}
	if len(sub.Locations) > 0 {
		return strings.ToLower(strings.TrimSpace(sub.Locations[0].Name))
	}
	return ""
}

func adminSubscriptionNotifySortKey(sub Subscription) string {
	bands := normalizeNotificationBands(sub.NotifyBands, sub.NotifyRules)
	parts := make([]string, 0, len(bands))
	for _, band := range bands {
		parts = append(parts, fmt.Sprintf("%03d:%03d:%s", band.Min, band.Max, band.Level))
	}
	return strings.Join(parts, "|")
}

func parseAdminFilterDate(value string, exclusiveEnd bool) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false
	}
	parsed, err := time.ParseInLocation("2006-01-02", value, beijingTZ)
	if err != nil {
		return time.Time{}, false
	}
	if exclusiveEnd {
		parsed = parsed.AddDate(0, 0, 1)
	}
	return parsed, true
}

func adminSubscriptionHasLevel(sub Subscription, level string) bool {
	for _, band := range normalizeNotificationBands(sub.NotifyBands, sub.NotifyRules) {
		if band.Level == level {
			return true
		}
	}
	return false
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

func serveAdminSubscriptionLiveness(w http.ResponseWriter, r *http.Request, cfg Config, store *Store) {
	startedAt := time.Now()
	var request adminSubscriptionLivenessRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "测活范围格式错误"})
		return
	}
	if len(request.BarkIDs) > 10000 {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "每次最多测活 10000 个选中订阅"})
		return
	}
	subscriptions, scope, notFound := adminLivenessSubscriptions(cfg, store.List(), request)
	selfHostedTotal := 0
	for _, sub := range subscriptions {
		if isSelfHostedBarkServer(sub.BarkServer, cfg) {
			selfHostedTotal++
		}
	}
	deviceKeys := map[string]struct{}{}
	if selfHostedTotal > 0 {
		var err error
		deviceKeys, err = loadSelfHostedBarkKeys(cfg)
		if err != nil {
			log.Printf("admin subscription liveness failed to read Bark devices: %v", err)
			writeJSON(w, http.StatusServiceUnavailable, APIResponse{Success: false, Message: "无法读取自建 Bark 设备库，未执行测活"})
			return
		}
	}

	selfHostedAlive := 0
	selfHostedMissing := 0
	officialNotChecked := 0
	invalid := 0
	issues := make([]adminSubscriptionLivenessIssue, 0)
	issueTotal := 0
	appendIssue := func(issue adminSubscriptionLivenessIssue) {
		issueTotal++
		if len(issues) < 500 {
			issues = append(issues, issue)
		}
	}
	for _, sub := range subscriptions {
		if err := validateSubscription(sub); err != nil {
			invalid++
			appendIssue(adminSubscriptionLivenessIssue{BarkID: sub.BarkID, BarkServer: sub.BarkServer, Status: "invalid_subscription", Message: err.Error()})
			continue
		}
		if isSelfHostedBarkServer(sub.BarkServer, cfg) {
			if _, ok := deviceKeys[sub.BarkID]; ok {
				selfHostedAlive++
			} else {
				selfHostedMissing++
				appendIssue(adminSubscriptionLivenessIssue{BarkID: sub.BarkID, BarkServer: sub.BarkServer, Status: "missing_device", Message: "自建 Bark 设备库中不存在该 Key"})
			}
			continue
		}
		officialNotChecked++
	}
	healthy := selfHostedMissing == 0 && invalid == 0
	log.Printf("admin subscription liveness scope=%s total=%d not_found=%d self_hosted=%d alive=%d missing=%d official_unchecked=%d invalid=%d duration=%s ip=%s",
		scope, len(subscriptions), notFound, selfHostedTotal, selfHostedAlive, selfHostedMissing, officialNotChecked, invalid, time.Since(startedAt), clientIP(r))
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "无推送测活完成", Data: map[string]any{
		"healthy":                    healthy,
		"scope":                      scope,
		"checked_at":                 time.Now().UTC().Format(time.RFC3339),
		"duration_ms":                time.Since(startedAt).Milliseconds(),
		"total_subscriptions":        len(subscriptions),
		"self_hosted_total":          selfHostedTotal,
		"self_hosted_alive":          selfHostedAlive,
		"self_hosted_missing":        selfHostedMissing,
		"official_not_checked":       officialNotChecked,
		"invalid_subscriptions":      invalid,
		"device_keys":                len(deviceKeys),
		"issue_total":                issueTotal,
		"issues":                     issues,
		"issues_truncated":           issueTotal > len(issues),
		"notification_sent":          false,
		"selected_not_found":         notFound,
		"official_check_explanation": "官方 Bark 不提供无推送 Key 校验接口，因此仅统计、不发送测试消息。",
	}})
}

func adminLivenessSubscriptions(cfg Config, subscriptions []Subscription, request adminSubscriptionLivenessRequest) ([]Subscription, string, int) {
	if len(request.BarkIDs) == 0 {
		return filterAdminSubscriptions(cfg, subscriptions, request.adminSubscriptionFilter), "filtered", 0
	}
	byKey := make(map[string]Subscription, len(subscriptions))
	for _, sub := range subscriptions {
		byKey[sub.BarkID] = sub
	}
	selected := make([]Subscription, 0, len(request.BarkIDs))
	seen := make(map[string]bool, len(request.BarkIDs))
	notFound := 0
	for _, rawKey := range request.BarkIDs {
		key := strings.TrimSpace(rawKey)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		if sub, ok := byKey[key]; ok {
			selected = append(selected, sub)
		} else {
			notFound++
		}
	}
	return selected, "selected", notFound
}

func loadSelfHostedBarkKeys(cfg Config) (map[string]struct{}, error) {
	if dsn := strings.TrimSpace(cfg.Bark.DeviceDBDSN); dsn != "" {
		return selfHostedBarkKeysMySQL(dsn)
	}
	path := strings.TrimSpace(cfg.Bark.DeviceDBPath)
	if path == "" {
		return nil, errors.New("bark device db path is empty")
	}
	db, cleanup, err := openBarkDeviceDB(path)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	defer db.Close()
	keys := make(map[string]struct{})
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("device"))
		if bucket == nil {
			return errors.New("device bucket not found")
		}
		return bucket.ForEach(func(key, _ []byte) error {
			if len(key) > 0 {
				keys[string(key)] = struct{}{}
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
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
	auditReports, auditTotal, auditErr := listDeliveryAuditSummaries(cfg, 0)
	auditEvents := 0
	if auditErr == nil {
		auditEvents = len(groupDeliveryAuditSummaries(cfg, auditReports))
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	paused, pausedMessage, pausedReason := subscriptionPauseState(cfg, store.Count())
	checks := []map[string]any{
		{"name": "地震数据源", "healthy": dataSourceHealthy, "detail": dataSourceCheckDetail(runtimeSnapshot, dataSourceHealthy)},
		{"name": "订阅存储", "healthy": storageHealthy, "detail": storageType, "error": storageError},
		{"name": "推送队列", "healthy": notifier == nil || notifier.queue == nil || queueConnected, "detail": queueStatus, "error": queueLastError},
		{"name": "投递审计", "healthy": auditErr == nil, "detail": fmt.Sprintf("%d 次地震，%d 个报次，保留 %d 天", auditEvents, auditTotal, cfg.Server.AuditRetentionDays), "error": errorString(auditErr)},
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
		"audit":                      map[string]any{"events": auditEvents, "records": auditTotal, "retention_days": cfg.Server.AuditRetentionDays, "error": errorString(auditErr)},
		"process":                    map[string]any{"go_version": runtime.Version(), "goroutines": runtime.NumGoroutine(), "memory_alloc_bytes": memory.Alloc, "memory_sys_bytes": memory.Sys},
		"default_bark_server":        normalizeBarkServer("", cfg),
		"self_hosted_bark_server":    strings.TrimRight(strings.TrimSpace(cfg.Bark.SelfHostedServer), "/"),
		"checks":                     checks,
	}
}

func newAdminServiceMonitor(cfg Config, store *Store, notifier *Notifier, health *RuntimeHealth) *adminServiceMonitor {
	return &adminServiceMonitor{
		cfg:      cfg,
		store:    store,
		notifier: notifier,
		health:   health,
		httpClient: &http.Client{
			Timeout: 2500 * time.Millisecond,
			CheckRedirect: func(_ *http.Request, via []*http.Request) error {
				if len(via) >= 3 {
					return errors.New("too many health probe redirects")
				}
				return nil
			},
		},
		lastSuccess: make(map[string]time.Time),
	}
}

func (m *adminServiceMonitor) Snapshot(ctx context.Context) adminServiceHealthSnapshot {
	m.probeMu.Lock()
	defer m.probeMu.Unlock()
	if !m.cachedAt.IsZero() && time.Since(m.cachedAt) < 3*time.Second {
		return m.cached
	}
	started := time.Now()
	probes := []func(context.Context) adminServiceStatus{
		m.probeApplication,
		m.probeEarthquakeSource,
		m.probeSubscriptionStore,
		m.probePushQueue,
		m.probePushWorkers,
		func(ctx context.Context) adminServiceStatus {
			return m.probeBark(ctx, "official_bark", "官方 Bark", m.cfg.Bark.Server, m.cfg.Bark.Server)
		},
		func(ctx context.Context) adminServiceStatus {
			probeURL := m.cfg.Bark.SelfHostedInternalServer
			if strings.TrimSpace(probeURL) == "" {
				probeURL = m.cfg.Bark.SelfHostedServer
			}
			return m.probeBark(ctx, "self_hosted_bark", "自建 Bark", m.cfg.Bark.SelfHostedServer, probeURL)
		},
		m.probeBarkDeviceStore,
		m.probeAuditStorage,
		m.probeRelay,
	}

	results := make(chan adminServiceStatus, len(probes))
	var wait sync.WaitGroup
	for _, probe := range probes {
		wait.Add(1)
		go func(probe func(context.Context) adminServiceStatus) {
			defer wait.Done()
			results <- probe(ctx)
		}(probe)
	}
	wait.Wait()
	close(results)
	byID := make(map[string]adminServiceStatus, len(probes))
	for result := range results {
		byID[result.ID] = result
	}

	order := []string{"application", "earthquake_source", "subscription_store", "push_queue", "push_workers", "official_bark", "self_hosted_bark", "bark_device_store", "audit_storage", "fanout_relay"}
	now := time.Now()
	snapshot := adminServiceHealthSnapshot{CheckedAt: now.UTC().Format(time.RFC3339), Status: "healthy", Healthy: true}
	for _, id := range order {
		result, ok := byID[id]
		if !ok {
			continue
		}
		result = m.withLastSuccess(result, now)
		snapshot.Services = append(snapshot.Services, result)
		if !result.Enabled {
			continue
		}
		snapshot.EnabledServices++
		switch result.Status {
		case "healthy":
			snapshot.HealthyServices++
		case "degraded":
			snapshot.DegradedServices++
		default:
			snapshot.UnhealthyServices++
		}
	}
	if snapshot.UnhealthyServices > 0 {
		snapshot.Status = "unhealthy"
		snapshot.Healthy = false
	} else if snapshot.DegradedServices > 0 {
		snapshot.Status = "degraded"
		snapshot.Healthy = false
	}
	snapshot.DurationMS = time.Since(started).Milliseconds()
	m.cached = snapshot
	m.cachedAt = time.Now()
	return snapshot
}

func (m *adminServiceMonitor) withLastSuccess(result adminServiceStatus, now time.Time) adminServiceStatus {
	m.mu.Lock()
	if result.Enabled && result.Status == "healthy" {
		m.lastSuccess[result.ID] = now
	}
	lastSuccess := m.lastSuccess[result.ID]
	m.mu.Unlock()
	if !lastSuccess.IsZero() {
		result.LastSuccessAt = lastSuccess.UTC().Format(time.RFC3339)
	}
	result.Healthy = result.Status == "healthy"
	return result
}

func healthyAdminService(id, name, detail string) adminServiceStatus {
	return adminServiceStatus{ID: id, Name: name, Status: "healthy", Healthy: true, Enabled: true, Detail: detail}
}

func disabledAdminService(id, name, detail string) adminServiceStatus {
	return adminServiceStatus{ID: id, Name: name, Status: "disabled", Enabled: false, Detail: detail}
}

func failedAdminService(id, name, detail string, err error) adminServiceStatus {
	result := adminServiceStatus{ID: id, Name: name, Status: "unhealthy", Enabled: true, Detail: detail}
	if err != nil {
		result.Error = truncateAdminServiceError(err.Error())
	}
	return result
}

func truncateAdminServiceError(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 300 {
		return value[:300] + "…"
	}
	return value
}

func (m *adminServiceMonitor) probeApplication(context.Context) adminServiceStatus {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	uptime := time.Since(adminProcessStartedAt)
	result := healthyAdminService("application", "EEW 应用", fmt.Sprintf("已运行 %s", compactAdminDuration(uptime)))
	result.Metrics = map[string]any{
		"go_version":         runtime.Version(),
		"goroutines":         runtime.NumGoroutine(),
		"memory_alloc_bytes": memory.Alloc,
		"memory_sys_bytes":   memory.Sys,
		"uptime_seconds":     int64(uptime.Seconds()),
	}
	return result
}

func (m *adminServiceMonitor) probeEarthquakeSource(context.Context) adminServiceStatus {
	now := time.Now()
	snapshot := m.health.Snapshot()
	staleAfter := time.Duration(m.cfg.Wolfx.HealthStaleSeconds) * time.Second
	if staleAfter <= 0 {
		staleAfter = 180 * time.Second
	}
	healthy := m.health.DataSourceHealthy(now, staleAfter)
	result := healthyAdminService("earthquake_source", "Wolfx 地震数据源", dataSourceCheckDetail(snapshot, healthy))
	if !healthy {
		result.Status = "unhealthy"
		result.Healthy = false
		result.Error = truncateAdminServiceError(snapshot.LastError)
	}
	result.Metrics = map[string]any{
		"connected":           snapshot.WebSocketConnected,
		"last_message_at":     snapshot.LastMessageAt,
		"last_processed_at":   snapshot.LastProcessedAt,
		"queue_depth":         snapshot.QueueDepth,
		"queue_dropped":       snapshot.QueueDropped,
		"queue_coalesced":     snapshot.QueueCoalesced,
		"stale_after_seconds": int64(staleAfter.Seconds()),
	}
	return result
}

func (m *adminServiceMonitor) probeSubscriptionStore(ctx context.Context) adminServiceStatus {
	started := time.Now()
	result := healthyAdminService("subscription_store", "订阅数据库", "JSON 文件存储")
	result.Metrics = map[string]any{"subscriptions": m.store.Count(), "type": "json"}
	m.store.mu.RLock()
	backend := m.store.backend
	m.store.mu.RUnlock()
	if backend == nil {
		result.LatencyMS = time.Since(started).Milliseconds()
		return result
	}
	postgres, ok := backend.(*postgresSubscriptionBackend)
	if !ok {
		result.Detail = "自定义持久化后端"
		result.Metrics["type"] = "custom"
		return result
	}
	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	err := postgres.db.PingContext(checkCtx)
	cancel()
	result.LatencyMS = time.Since(started).Milliseconds()
	result.Detail = "PostgreSQL 连接正常"
	result.Metrics["type"] = "postgresql"
	stats := postgres.db.Stats()
	result.Metrics["total_connections"] = stats.OpenConnections
	result.Metrics["acquired_connections"] = stats.InUse
	result.Metrics["idle_connections"] = stats.Idle
	if err != nil {
		return failedAdminService("subscription_store", "订阅数据库", "PostgreSQL 连接失败", err)
	}
	return result
}

func (m *adminServiceMonitor) probePushQueue(context.Context) adminServiceStatus {
	if m.notifier == nil || m.notifier.queue == nil || m.notifier.queue.nc == nil {
		return disabledAdminService("push_queue", "NATS 推送队列", "未启用队列，应用直接投递")
	}
	queue := m.notifier.queue
	started := time.Now()
	if !queue.nc.IsConnected() {
		err := queue.nc.LastError()
		if err == nil {
			err = errors.New(queue.nc.Status().String())
		}
		return failedAdminService("push_queue", "NATS 推送队列", "连接已断开", err)
	}
	info, err := queue.js.StreamInfo(queue.cfg.Stream)
	latency := time.Since(started).Milliseconds()
	if err != nil {
		result := failedAdminService("push_queue", "NATS 推送队列", "已连接，但无法读取 JetStream", err)
		result.LatencyMS = latency
		return result
	}
	result := healthyAdminService("push_queue", "NATS 推送队列", "JetStream 连接正常")
	result.LatencyMS = latency
	result.Metrics = map[string]any{
		"status":        queue.nc.Status().String(),
		"stream":        info.Config.Name,
		"messages":      info.State.Msgs,
		"bytes":         info.State.Bytes,
		"consumers":     info.State.Consumers,
		"async_pending": queue.js.PublishAsyncPending(),
	}
	if info.State.Msgs > 0 {
		result.Detail = fmt.Sprintf("队列处理中：%d 条待消费", info.State.Msgs)
		if !info.State.FirstTime.IsZero() {
			result.Metrics["oldest_message_at"] = info.State.FirstTime.UTC().Format(time.RFC3339)
			if time.Since(info.State.FirstTime) > 30*time.Second {
				result.Status = "degraded"
				result.Healthy = false
				result.Detail = fmt.Sprintf("队列积压：%d 条，最旧已等待 %s", info.State.Msgs, compactAdminDuration(time.Since(info.State.FirstTime)))
			}
		}
	}
	return result
}

func (m *adminServiceMonitor) probePushWorkers(context.Context) adminServiceStatus {
	if m.notifier == nil || m.notifier.queue == nil {
		return disabledAdminService("push_workers", "推送 Worker", "未启用 NATS Worker")
	}
	now := time.Now()
	workers, expected, staleAfter := m.notifier.queue.workerHealth(now)
	active := 0
	for _, worker := range workers {
		if worker.Active {
			active++
		}
	}
	result := healthyAdminService("push_workers", "推送 Worker", fmt.Sprintf("%d / %d 个 Worker 在线", active, expected))
	result.Workers = workers
	result.Metrics = map[string]any{"active": active, "expected": expected, "stale_after_seconds": int64(staleAfter.Seconds())}
	if active < expected {
		result.Status = "degraded"
		result.Healthy = false
		if active == 0 && time.Since(adminProcessStartedAt) > staleAfter {
			result.Status = "unhealthy"
		}
		result.Error = fmt.Sprintf("缺少 %d 个有效 Worker 心跳", expected-active)
	}
	return result
}

func (m *adminServiceMonitor) probeBark(ctx context.Context, id, name, displayURL, probeBaseURL string) adminServiceStatus {
	displayURL = strings.TrimRight(strings.TrimSpace(displayURL), "/")
	probeBaseURL = strings.TrimRight(strings.TrimSpace(probeBaseURL), "/")
	if displayURL == "" || probeBaseURL == "" {
		return disabledAdminService(id, name, "未配置")
	}
	started := time.Now()
	probeCtx, cancel := context.WithTimeout(ctx, 2500*time.Millisecond)
	defer cancel()
	request, err := http.NewRequestWithContext(probeCtx, http.MethodGet, probeBaseURL+"/ping", nil)
	if err != nil {
		return failedAdminService(id, name, barkServiceHost(displayURL), err)
	}
	request.Header.Set("Accept", "text/plain, application/json")
	request.Header.Set("User-Agent", "eew-bark-health-monitor/1.0")
	response, err := m.httpClient.Do(request)
	latency := time.Since(started).Milliseconds()
	if err != nil {
		result := failedAdminService(id, name, barkServiceHost(displayURL), err)
		result.LatencyMS = latency
		return result
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		result := failedAdminService(id, name, barkServiceHost(displayURL), fmt.Errorf("HTTP %d", response.StatusCode))
		result.LatencyMS = latency
		return result
	}
	result := healthyAdminService(id, name, barkServiceHost(displayURL)+" /ping 正常")
	result.LatencyMS = latency
	result.Metrics = map[string]any{"endpoint": barkServiceHost(displayURL), "http_status": response.StatusCode, "notification_sent": false}
	return result
}

func barkServiceHost(value string) string {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err == nil && parsed.Host != "" {
		return parsed.Host
	}
	return "已配置服务"
}

func (m *adminServiceMonitor) probeBarkDeviceStore(ctx context.Context) adminServiceStatus {
	if strings.TrimSpace(m.cfg.Bark.SelfHostedServer) == "" {
		return disabledAdminService("bark_device_store", "Bark 设备数据库", "未配置自建 Bark")
	}
	started := time.Now()
	if dsn := strings.TrimSpace(m.cfg.Bark.DeviceDBDSN); dsn != "" {
		db, err := barkMySQLPool(dsn)
		if err != nil {
			return failedAdminService("bark_device_store", "Bark 设备数据库", "MySQL 初始化失败", err)
		}
		checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err = db.PingContext(checkCtx)
		cancel()
		latency := time.Since(started).Milliseconds()
		if err != nil {
			result := failedAdminService("bark_device_store", "Bark 设备数据库", "MySQL 连接失败", err)
			result.LatencyMS = latency
			return result
		}
		stats := db.Stats()
		result := healthyAdminService("bark_device_store", "Bark 设备数据库", "MySQL 连接正常")
		result.LatencyMS = latency
		result.Metrics = map[string]any{"type": "mysql", "open_connections": stats.OpenConnections, "in_use": stats.InUse, "idle": stats.Idle, "wait_count": stats.WaitCount}
		return result
	}
	path := strings.TrimSpace(m.cfg.Bark.DeviceDBPath)
	if path == "" {
		return failedAdminService("bark_device_store", "Bark 设备数据库", "设备数据库未配置", errors.New("device database path is empty"))
	}
	info, err := os.Stat(path)
	if err != nil {
		return failedAdminService("bark_device_store", "Bark 设备数据库", "bbolt 文件不可读", err)
	}
	result := healthyAdminService("bark_device_store", "Bark 设备数据库", "bbolt 文件可读")
	result.LatencyMS = time.Since(started).Milliseconds()
	result.Metrics = map[string]any{"type": "bbolt", "size_bytes": info.Size(), "modified_at": info.ModTime().UTC().Format(time.RFC3339)}
	return result
}

func (m *adminServiceMonitor) probeAuditStorage(context.Context) adminServiceStatus {
	started := time.Now()
	directory := auditDirectory(m.cfg)
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		result := healthyAdminService("audit_storage", "投递审计存储", "审计目录尚无记录")
		result.LatencyMS = time.Since(started).Milliseconds()
		result.Metrics = map[string]any{"files": 0, "retention_days": m.cfg.Server.AuditRetentionDays}
		return result
	}
	if err != nil {
		return failedAdminService("audit_storage", "投递审计存储", "审计目录不可读", err)
	}
	files := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			files++
		}
	}
	result := healthyAdminService("audit_storage", "投递审计存储", fmt.Sprintf("目录可读，%d 个文件", files))
	result.LatencyMS = time.Since(started).Milliseconds()
	result.Metrics = map[string]any{"files": files, "retention_days": m.cfg.Server.AuditRetentionDays}
	return result
}

func (m *adminServiceMonitor) probeRelay(context.Context) adminServiceStatus {
	if m.notifier == nil || m.notifier.relay == nil {
		return disabledAdminService("fanout_relay", "推送中继", "未启用")
	}
	result := healthyAdminService("fanout_relay", "推送中继", "已配置，健康由实际投递结果验证")
	result.Metrics = map[string]any{"share_percent": m.notifier.relay.cfg.SharePercent}
	return result
}

func compactAdminDuration(value time.Duration) string {
	if value < 0 {
		value = 0
	}
	days := int(value / (24 * time.Hour))
	hours := int(value/time.Hour) % 24
	minutes := int(value/time.Minute) % 60
	if value < time.Minute {
		return fmt.Sprintf("%d 秒", int(value/time.Second))
	}
	if days > 0 {
		return fmt.Sprintf("%d 天 %d 小时", days, hours)
	}
	if hours > 0 {
		return fmt.Sprintf("%d 小时 %d 分钟", hours, minutes)
	}
	return fmt.Sprintf("%d 分钟", minutes)
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

func groupDeliveryAuditSummaries(cfg Config, reports []adminAuditSummary) []adminAuditEventGroup {
	historyIndex := auditHistoryIndex(cfg)
	exactGroups := make(map[string][]adminAuditSummary)
	for _, report := range reports {
		enrichAuditSummary(&report.deliveryAuditSummary, historyIndex)
		key := auditExactEventKey(report)
		exactGroups[key] = append(exactGroups[key], report)
	}
	exactKeys := make([]string, 0, len(exactGroups))
	for key := range exactGroups {
		exactKeys = append(exactKeys, key)
	}
	sort.Strings(exactKeys)
	clusters := make([][]adminAuditSummary, 0, len(exactKeys))
	for _, key := range exactKeys {
		clusters = append(clusters, exactGroups[key])
	}
	parents := make([]int, len(clusters))
	rootReports := make([][]adminAuditSummary, len(clusters))
	for index := range parents {
		parents[index] = index
		rootReports[index] = append([]adminAuditSummary(nil), clusters[index]...)
	}
	var find func(int) int
	find = func(index int) int {
		if parents[index] != index {
			parents[index] = find(parents[index])
		}
		return parents[index]
	}
	for left := 0; left < len(clusters); left++ {
		for right := left + 1; right < len(clusters); right++ {
			leftRoot, rightRoot := find(left), find(right)
			if leftRoot == rightRoot || auditReportGroupsSourceConflict(rootReports[leftRoot], rootReports[rightRoot]) {
				continue
			}
			if auditReportGroupsSameEarthquake(rootReports[leftRoot], rootReports[rightRoot]) {
				parents[rightRoot] = leftRoot
				rootReports[leftRoot] = append(rootReports[leftRoot], rootReports[rightRoot]...)
				rootReports[rightRoot] = nil
			}
		}
	}
	merged := make(map[int][]adminAuditSummary)
	for index, cluster := range clusters {
		root := find(index)
		merged[root] = append(merged[root], cluster...)
	}
	result := make([]adminAuditEventGroup, 0, len(merged))
	for _, groupReports := range merged {
		sort.Slice(groupReports, func(i, j int) bool {
			if !groupReports[i].Modified.Equal(groupReports[j].Modified) {
				return groupReports[i].Modified.After(groupReports[j].Modified)
			}
			return groupReports[i].ReportNum > groupReports[j].ReportNum
		})
		latest := groupReports[0]
		sources := auditGroupSources(groupReports)
		eventIDs := auditGroupEventIDs(groupReports)
		identity := auditGroupIdentity(groupReports)
		group := adminAuditEventGroup{
			ID:            hashKey(identity)[:16],
			EventID:       latest.EventID,
			EventIDs:      eventIDs,
			Type:          latest.Type,
			Source:        strings.Join(sources, " / "),
			Sources:       sources,
			OriginTime:    latest.OriginTime,
			Hypocenter:    latest.Hypocenter,
			Latitude:      latest.Latitude,
			Longitude:     latest.Longitude,
			Magnitude:     latest.Magnitude,
			DepthKM:       latest.DepthKM,
			MaxIntensity:  latest.MaxIntensity,
			Final:         latest.Final,
			Cancel:        latest.Cancel,
			FirstReceived: latest.ReceivedAt,
			LastReceived:  latest.ReceivedAt,
			ReportCount:   len(groupReports),
			Latest:        latest,
			Reports:       groupReports,
		}
		for _, report := range groupReports {
			group.TotalPushed += report.Pushed
			group.TotalFiltered += report.Filtered
			group.TotalSkipped += report.Skipped
			group.TotalFailed += report.Failed
			if group.Hypocenter == "" && report.Hypocenter != "" {
				group.Hypocenter = report.Hypocenter
			}
			if group.Latitude == 0 && group.Longitude == 0 && (report.Latitude != 0 || report.Longitude != 0) {
				group.Latitude, group.Longitude = report.Latitude, report.Longitude
			}
			if group.Magnitude == 0 && report.Magnitude != 0 {
				group.Magnitude = report.Magnitude
			}
			if group.DepthKM == 0 && report.DepthKM != 0 {
				group.DepthKM = report.DepthKM
			}
			if group.MaxIntensity == "" && report.MaxIntensity != "" {
				group.MaxIntensity = report.MaxIntensity
			}
			if group.OriginTime == "" && report.OriginTime != "" {
				group.OriginTime = report.OriginTime
			}
			if report.ReceivedAt < group.FirstReceived || group.FirstReceived == "" {
				group.FirstReceived = report.ReceivedAt
			}
			if report.ReceivedAt > group.LastReceived {
				group.LastReceived = report.ReceivedAt
			}
			group.Final = group.Final || report.Final
			group.Cancel = group.Cancel || report.Cancel
		}
		if group.Hypocenter != "" {
			group.Title = group.Hypocenter
		} else if group.Source != "" {
			group.Title = group.Source + "地震预警"
		} else {
			group.Title = "地震预警"
		}
		result = append(result, group)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Latest.Modified.After(result[j].Latest.Modified)
	})
	return result
}

func auditExactEventKey(report adminAuditSummary) string {
	key := strings.ToLower(strings.TrimSpace(report.Type)) + "|" + strings.ToLower(strings.TrimSpace(report.EventID))
	if strings.Trim(key, "|") == "" {
		return report.ID
	}
	return key
}

func auditReportGroupsSameEarthquake(left, right []adminAuditSummary) bool {
	for _, leftReport := range left {
		for _, rightReport := range right {
			if auditReportsSameEarthquake(leftReport, rightReport) {
				return true
			}
		}
	}
	return false
}

func auditReportGroupsSourceConflict(left, right []adminAuditSummary) bool {
	types := make(map[string]bool)
	for _, report := range left {
		types[strings.ToLower(strings.TrimSpace(report.Type))] = true
	}
	for _, report := range right {
		if types[strings.ToLower(strings.TrimSpace(report.Type))] {
			return true
		}
	}
	return false
}

func auditReportsSameEarthquake(left, right adminAuditSummary) bool {
	leftType := strings.ToLower(strings.TrimSpace(left.Type))
	rightType := strings.ToLower(strings.TrimSpace(right.Type))
	if leftType == rightType {
		return false
	}
	leftTime := parseTimeInZone(left.OriginTime, 8*60*60)
	rightTime := parseTimeInZone(right.OriginTime, 8*60*60)
	if leftTime.IsZero() || rightTime.IsZero() {
		return false
	}
	delta := leftTime.Sub(rightTime)
	if delta < 0 {
		delta = -delta
	}
	if delta > 15*time.Second {
		return false
	}
	if left.EventID != "" && strings.EqualFold(left.EventID, right.EventID) {
		return true
	}
	if left.Magnitude > 0 && right.Magnitude > 0 && math.Abs(left.Magnitude-right.Magnitude) > 1.0 {
		return false
	}
	leftHasCoordinates := validSubscriptionCoordinate(left.Latitude, left.Longitude)
	rightHasCoordinates := validSubscriptionCoordinate(right.Latitude, right.Longitude)
	if leftHasCoordinates && rightHasCoordinates {
		return haversineKM(left.Latitude, left.Longitude, right.Latitude, right.Longitude) <= 120
	}
	leftHypocenter := normalizeAuditHypocenter(left.Hypocenter)
	rightHypocenter := normalizeAuditHypocenter(right.Hypocenter)
	if leftHypocenter != "" && rightHypocenter != "" {
		if strings.Contains(leftHypocenter, rightHypocenter) || strings.Contains(rightHypocenter, leftHypocenter) {
			return true
		}
		return delta <= 3*time.Second
	}
	return delta <= 12*time.Second
}

func normalizeAuditHypocenter(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	replacer := strings.NewReplacer(" ", "", "　", "", "地震", "", "附近", "", "地区", "", "地方", "")
	return replacer.Replace(value)
}

func auditGroupIdentity(reports []adminAuditSummary) string {
	keys := make([]string, 0, len(reports))
	seen := make(map[string]bool)
	for _, report := range reports {
		key := auditExactEventKey(report)
		if !seen[key] {
			seen[key] = true
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return strings.Join(keys, "+")
}

func auditGroupEventIDs(reports []adminAuditSummary) []string {
	values := make([]string, 0)
	seen := make(map[string]bool)
	for _, report := range reports {
		value := strings.TrimSpace(report.EventID)
		key := strings.ToLower(value)
		if value != "" && !seen[key] {
			seen[key] = true
			values = append(values, value)
		}
	}
	sort.Strings(values)
	return values
}

func auditGroupSources(reports []adminAuditSummary) []string {
	values := make([]string, 0)
	seen := make(map[string]bool)
	for _, report := range reports {
		value := auditSourceName(report.Type)
		if value != "" && !seen[value] {
			seen[value] = true
			values = append(values, value)
		}
	}
	sort.Strings(values)
	return values
}

func auditSourceName(eventType string) string {
	switch strings.ToLower(strings.TrimSpace(eventType)) {
	case "jma_eew":
		return "日本气象厅"
	case "jma_eew_test":
		return "日本气象厅测试源"
	case "cenc_eew":
		return "中国地震台网"
	case "cq_eew":
		return "重庆地震局"
	case "sc_eew":
		return "四川地震局"
	case "fj_eew":
		return "福建地震局"
	case "cwa_eew":
		return "台湾气象署"
	default:
		value := strings.TrimSuffix(strings.ToUpper(strings.TrimSpace(eventType)), "_EEW")
		return strings.ReplaceAll(value, "_", " ")
	}
}

func auditHistoryIndex(cfg Config) auditHistoryLookup {
	lookup := auditHistoryLookup{ByID: make(map[string]HistoryRecord)}
	records := builtinHistoryRecords()
	if cache, err := loadHistoryCache(cfg.Server.HistoryPath); err == nil {
		records = append(records, cache.Records...)
	}
	for _, record := range records {
		for _, key := range []string{record.EventID, record.Key} {
			key = strings.ToLower(strings.TrimSpace(key))
			if key != "" {
				sourceKey := strings.ToLower(strings.TrimSpace(record.Source)) + "|" + key
				lookup.ByID[sourceKey] = record
				if _, exists := lookup.ByID[key]; !exists {
					lookup.ByID[key] = record
				}
			}
		}
		if originTime := auditHistoryOriginTime(record); !originTime.IsZero() {
			lookup.Candidates = append(lookup.Candidates, auditHistoryCandidate{Record: record, OriginTime: originTime})
		}
	}
	return lookup
}

func auditHistoryOriginTime(record HistoryRecord) time.Time {
	offset := 8 * 60 * 60
	if strings.EqualFold(record.Source, "jma") {
		offset = 9 * 60 * 60
	}
	return parseTimeInZone(record.OriginTime, offset)
}

func auditHistorySource(eventType string) string {
	value := strings.ToLower(strings.TrimSpace(eventType))
	if strings.HasPrefix(value, "jma_") {
		return "jma"
	}
	if strings.HasPrefix(value, "cenc_") || strings.HasPrefix(value, "cq_") || strings.HasPrefix(value, "sc_") || strings.HasPrefix(value, "fj_") {
		return "cenc"
	}
	return ""
}

func enrichAuditSummary(summary *deliveryAuditSummary, historyIndex auditHistoryLookup) {
	if summary == nil {
		return
	}
	eventID := strings.ToLower(strings.TrimSpace(summary.EventID))
	expectedSource := auditHistorySource(summary.Type)
	record, ok := historyIndex.ByID[expectedSource+"|"+eventID]
	if !ok && expectedSource == "" {
		record, ok = historyIndex.ByID[eventID]
	}
	if !ok {
		record, ok = nearestAuditHistoryRecord(*summary, expectedSource, historyIndex.Candidates)
	}
	if ok {
		applyAuditHistoryRecord(summary, record)
	}
}

func nearestAuditHistoryRecord(summary deliveryAuditSummary, expectedSource string, candidates []auditHistoryCandidate) (HistoryRecord, bool) {
	originTime := parseTimeInZone(summary.OriginTime, 8*60*60)
	if originTime.IsZero() || expectedSource == "" {
		return HistoryRecord{}, false
	}
	bestDelta := 31 * time.Second
	var best HistoryRecord
	found := false
	for _, candidate := range candidates {
		if !strings.EqualFold(candidate.Record.Source, expectedSource) {
			continue
		}
		delta := originTime.Sub(candidate.OriginTime)
		if delta < 0 {
			delta = -delta
		}
		if delta >= bestDelta {
			continue
		}
		if summary.Magnitude > 0 && candidate.Record.Magnitude > 0 && math.Abs(summary.Magnitude-candidate.Record.Magnitude) > 1.0 {
			continue
		}
		if validSubscriptionCoordinate(summary.Latitude, summary.Longitude) && validSubscriptionCoordinate(candidate.Record.Latitude, candidate.Record.Longitude) && haversineKM(summary.Latitude, summary.Longitude, candidate.Record.Latitude, candidate.Record.Longitude) > 150 {
			continue
		}
		bestDelta = delta
		best = candidate.Record
		found = true
	}
	return best, found
}

func applyAuditHistoryRecord(summary *deliveryAuditSummary, record HistoryRecord) {
	if summary.Hypocenter == "" {
		summary.Hypocenter = record.Hypocenter
	}
	if summary.Latitude == 0 && summary.Longitude == 0 {
		summary.Latitude, summary.Longitude = record.Latitude, record.Longitude
	}
	if summary.Magnitude == 0 {
		summary.Magnitude = record.Magnitude
	}
	if summary.DepthKM == 0 {
		summary.DepthKM = record.DepthKM
	}
	if summary.MaxIntensity == "" {
		summary.MaxIntensity = record.MaxIntensity
	}
	if summary.OriginTime == "" {
		summary.OriginTime = record.OriginTime
	}
}

func serveAdminAudits(w http.ResponseWriter, r *http.Request, cfg Config) {
	offset, limit := parseAdminPage(r, 100, 500)
	reports, reportTotal, err := listDeliveryAuditSummaries(cfg, 0)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: "读取投递审计失败"})
		return
	}
	groups := groupDeliveryAuditSummaries(cfg, reports)
	total := len(groups)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "ok", Data: map[string]any{
		"items": groups[offset:end], "total": total, "report_total": reportTotal, "offset": offset, "limit": limit,
	}})
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
	enrichAuditSummary(&summary, auditHistoryIndex(cfg))
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
