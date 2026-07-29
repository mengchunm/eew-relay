package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	adminServiceHistorySampleInterval = time.Minute
	adminServiceHistoryRetentionDays  = 30
)

type adminServiceHistoryPoint struct {
	CheckedAt string            `json:"checked_at"`
	Statuses  map[string]string `json:"statuses"`
}

type adminServiceHistoryService struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type adminServiceHistoryResponse struct {
	RangeHours    int                          `json:"range_hours"`
	BucketMinutes int                          `json:"bucket_minutes"`
	RetentionDays int                          `json:"retention_days"`
	StartedAt     string                       `json:"started_at,omitempty"`
	EndedAt       string                       `json:"ended_at,omitempty"`
	Services      []adminServiceHistoryService `json:"services"`
	Points        []adminServiceHistoryPoint   `json:"points"`
}

type adminServiceHistoryStore struct {
	path         string
	mu           sync.Mutex
	lastPruneDay string
}

var adminServiceHistoryServices = []adminServiceHistoryService{
	{ID: "overall", Name: "整体状态"},
	{ID: "application", Name: "EEW 应用"},
	{ID: "earthquake_source", Name: "地震数据源"},
	{ID: "subscription_store", Name: "订阅数据库"},
	{ID: "push_queue", Name: "推送队列"},
	{ID: "push_workers", Name: "推送 Worker"},
	{ID: "official_bark", Name: "官方 Bark"},
	{ID: "self_hosted_bark", Name: "自建 Bark"},
	{ID: "bark_device_store", Name: "Bark 设备库"},
	{ID: "audit_storage", Name: "通知审计存储"},
	{ID: "fanout_relay", Name: "分流中继"},
}

func newAdminServiceHistoryStore(cfg Config) *adminServiceHistoryStore {
	dataPath := strings.TrimSpace(cfg.Server.DataPath)
	if dataPath == "" {
		return &adminServiceHistoryStore{}
	}
	return &adminServiceHistoryStore{path: filepath.Join(filepath.Dir(dataPath), "service-health.jsonl")}
}

func (m *adminServiceMonitor) Start(ctx context.Context) {
	if m == nil || m.history == nil || m.history.path == "" || ctx == nil {
		return
	}
	go func() {
		m.recordHistoryPoint(ctx)
		ticker := time.NewTicker(adminServiceHistorySampleInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.recordHistoryPoint(ctx)
			}
		}
	}()
}

func (m *adminServiceMonitor) recordHistoryPoint(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	if err := m.history.Append(snapshotHistoryPoint(m.Snapshot(ctx)), time.Now()); err != nil {
		log.Printf("service health history: %v", err)
	}
}

func (m *adminServiceMonitor) History(ctx context.Context, hours int) (adminServiceHistoryResponse, error) {
	if hours < 1 {
		hours = 24
	}
	if hours > adminServiceHistoryRetentionDays*24 {
		hours = adminServiceHistoryRetentionDays * 24
	}
	current := snapshotHistoryPoint(m.Snapshot(ctx))
	response, err := m.history.Query(hours, current, time.Now())
	if err != nil {
		log.Printf("read service health history: %v", err)
	}
	return response, err
}

func snapshotHistoryPoint(snapshot adminServiceHealthSnapshot) adminServiceHistoryPoint {
	statuses := make(map[string]string, len(snapshot.Services)+1)
	statuses["overall"] = snapshot.Status
	for _, service := range snapshot.Services {
		statuses[service.ID] = service.Status
	}
	return adminServiceHistoryPoint{CheckedAt: snapshot.CheckedAt, Statuses: statuses}
}

func (s *adminServiceHistoryStore) Append(point adminServiceHistoryPoint, now time.Time) error {
	if s == nil || s.path == "" || point.CheckedAt == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.path), 0o750); err != nil {
		return fmt.Errorf("create history directory: %w", err)
	}
	day := now.UTC().Format("2006-01-02")
	if s.lastPruneDay != day {
		if err := s.pruneLocked(now.Add(-adminServiceHistoryRetentionDays * 24 * time.Hour)); err != nil {
			return err
		}
		s.lastPruneDay = day
	}
	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open history: %w", err)
	}
	defer file.Close()
	if err := json.NewEncoder(file).Encode(point); err != nil {
		return fmt.Errorf("append history: %w", err)
	}
	return nil
}

func (s *adminServiceHistoryStore) Query(hours int, current adminServiceHistoryPoint, now time.Time) (adminServiceHistoryResponse, error) {
	response := adminServiceHistoryResponse{RangeHours: hours, BucketMinutes: historyBucketMinutes(hours), RetentionDays: adminServiceHistoryRetentionDays}
	if s == nil {
		return response, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := now.Add(-time.Duration(hours) * time.Hour)
	points, err := s.readLocked(cutoff)
	if err != nil {
		return response, err
	}
	if at, ok := parseHistoryPointTime(current); ok && !at.Before(cutoff) {
		if len(points) == 0 || current.CheckedAt != points[len(points)-1].CheckedAt {
			points = append(points, current)
		}
	}
	points = bucketHistoryPoints(points, time.Duration(response.BucketMinutes)*time.Minute)
	response.Points = points
	response.Services = visibleHistoryServices(points)
	if len(points) > 0 {
		response.StartedAt = points[0].CheckedAt
		response.EndedAt = points[len(points)-1].CheckedAt
	}
	return response, nil
}

func (s *adminServiceHistoryStore) readLocked(cutoff time.Time) ([]adminServiceHistoryPoint, error) {
	if s.path == "" {
		return nil, nil
	}
	file, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open history: %w", err)
	}
	defer file.Close()
	return decodeHistoryPoints(file, cutoff), nil
}

func decodeHistoryPoints(reader io.Reader, cutoff time.Time) []adminServiceHistoryPoint {
	points := make([]adminServiceHistoryPoint, 0)
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		var point adminServiceHistoryPoint
		if json.Unmarshal(scanner.Bytes(), &point) != nil || len(point.Statuses) == 0 {
			continue
		}
		at, ok := parseHistoryPointTime(point)
		if ok && !at.Before(cutoff) {
			points = append(points, point)
		}
	}
	sort.Slice(points, func(i, j int) bool { return points[i].CheckedAt < points[j].CheckedAt })
	return points
}

func (s *adminServiceHistoryStore) pruneLocked(cutoff time.Time) error {
	points, err := s.readLocked(cutoff)
	if err != nil {
		return fmt.Errorf("prune history: %w", err)
	}
	if _, err := os.Stat(s.path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(s.path), ".service-health-*.jsonl")
	if err != nil {
		return fmt.Errorf("create pruned history: %w", err)
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	encoder := json.NewEncoder(temp)
	for _, point := range points {
		if err := encoder.Encode(point); err != nil {
			temp.Close()
			return err
		}
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempName, s.path); err != nil {
		return fmt.Errorf("replace history: %w", err)
	}
	return nil
}

func historyBucketMinutes(hours int) int {
	switch {
	case hours > 7*24:
		return 30
	case hours > 24:
		return 10
	default:
		return 1
	}
}

func bucketHistoryPoints(points []adminServiceHistoryPoint, bucket time.Duration) []adminServiceHistoryPoint {
	if bucket <= 0 || len(points) == 0 {
		return points
	}
	type bucketPoint struct {
		at       time.Time
		statuses map[string]string
	}
	buckets := make(map[int64]*bucketPoint)
	keys := make([]int64, 0)
	for _, point := range points {
		at, ok := parseHistoryPointTime(point)
		if !ok {
			continue
		}
		key := at.Unix() / int64(bucket.Seconds())
		entry := buckets[key]
		if entry == nil {
			entry = &bucketPoint{at: at, statuses: make(map[string]string)}
			buckets[key] = entry
			keys = append(keys, key)
		}
		if at.After(entry.at) {
			entry.at = at
		}
		for id, status := range point.Statuses {
			if historyStatusSeverity(status) > historyStatusSeverity(entry.statuses[id]) {
				entry.statuses[id] = status
			}
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	result := make([]adminServiceHistoryPoint, 0, len(keys))
	for _, key := range keys {
		entry := buckets[key]
		result = append(result, adminServiceHistoryPoint{CheckedAt: entry.at.UTC().Format(time.RFC3339), Statuses: entry.statuses})
	}
	return result
}

func historyStatusSeverity(status string) int {
	switch status {
	case "unhealthy":
		return 4
	case "degraded":
		return 3
	case "healthy":
		return 2
	case "disabled":
		return 1
	default:
		return 0
	}
}

func visibleHistoryServices(points []adminServiceHistoryPoint) []adminServiceHistoryService {
	visible := map[string]bool{"overall": true}
	for _, point := range points {
		for id, status := range point.Statuses {
			if status != "" && status != "disabled" {
				visible[id] = true
			}
		}
	}
	services := make([]adminServiceHistoryService, 0, len(visible))
	for _, service := range adminServiceHistoryServices {
		if visible[service.ID] {
			services = append(services, service)
		}
	}
	return services
}

func parseHistoryPointTime(point adminServiceHistoryPoint) (time.Time, bool) {
	at, err := time.Parse(time.RFC3339, point.CheckedAt)
	return at, err == nil
}
