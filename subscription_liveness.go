package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	subscriptionLivenessUntested             = "untested"
	subscriptionLivenessDevicePresent        = "device_present"
	subscriptionLivenessDeviceMissing        = "device_missing"
	subscriptionLivenessDeviceTokenMissing   = "device_token_missing"
	subscriptionLivenessConfigurationInvalid = "configuration_invalid"
	subscriptionLivenessOfficialUnverified   = "official_unverified"
	subscriptionLivenessFileVersion          = 2
	subscriptionLivenessLegacyFileVersion    = 1
)

type SubscriptionLivenessRecord struct {
	Status                string `json:"status"`
	CheckedAt             string `json:"checked_at,omitempty"`
	Message               string `json:"message,omitempty"`
	SubscriptionUpdatedAt int64  `json:"subscription_updated_at"`
	BarkServer            string `json:"bark_server,omitempty"`
}

type subscriptionLivenessFile struct {
	Version   int                                   `json:"version"`
	UpdatedAt string                                `json:"updated_at"`
	Records   map[string]SubscriptionLivenessRecord `json:"records"`
}

type subscriptionLivenessStore struct {
	mu        sync.RWMutex
	path      string
	version   int
	updatedAt string
	bark      BarkConfig
	records   map[string]SubscriptionLivenessRecord
}

func newSubscriptionLivenessStore(cfg Config) (*subscriptionLivenessStore, error) {
	store := &subscriptionLivenessStore{version: subscriptionLivenessFileVersion, bark: cfg.Bark, records: make(map[string]SubscriptionLivenessRecord)}
	dataPath := strings.TrimSpace(cfg.Server.DataPath)
	if dataPath == "" {
		return store, nil
	}
	store.path = filepath.Join(filepath.Dir(dataPath), "subscription-liveness.json")
	data, err := os.ReadFile(store.path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, err
	}
	var stored subscriptionLivenessFile
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&stored); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("subscription liveness labels must contain one JSON document")
		}
		return nil, err
	}
	if stored.Version != subscriptionLivenessLegacyFileVersion && stored.Version != subscriptionLivenessFileVersion {
		return nil, fmt.Errorf("unsupported subscription liveness label version %d", stored.Version)
	}
	for barkID, record := range stored.Records {
		if validateBarkID(barkID) != nil || !validSubscriptionLivenessStatus(record.Status) || record.SubscriptionUpdatedAt <= 0 {
			return nil, errors.New("subscription liveness labels contain an invalid record")
		}
		if _, err := time.Parse(time.RFC3339, record.CheckedAt); err != nil {
			return nil, errors.New("subscription liveness labels contain an invalid timestamp")
		}
		store.records[barkID] = record
	}
	store.version = stored.Version
	store.updatedAt = strings.TrimSpace(stored.UpdatedAt)
	return store, nil
}

func validSubscriptionLivenessStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case subscriptionLivenessDevicePresent, subscriptionLivenessDeviceMissing, subscriptionLivenessDeviceTokenMissing, subscriptionLivenessConfigurationInvalid, subscriptionLivenessOfficialUnverified:
		return true
	default:
		return false
	}
}

func subscriptionLivenessStatusLabel(status string) string {
	switch status {
	case subscriptionLivenessDevicePresent:
		return "设备可用"
	case subscriptionLivenessDeviceMissing:
		return "设备库缺失"
	case subscriptionLivenessDeviceTokenMissing:
		return "设备 Token 缺失"
	case subscriptionLivenessConfigurationInvalid:
		return "配置异常"
	case subscriptionLivenessOfficialUnverified:
		return "官方未验证"
	default:
		return "未测活"
	}
}

func (s *subscriptionLivenessStore) Snapshot(sub Subscription) SubscriptionLivenessRecord {
	if s == nil {
		return SubscriptionLivenessRecord{Status: subscriptionLivenessUntested, Message: "尚未测活"}
	}
	s.mu.RLock()
	record, ok := s.records[sub.BarkID]
	s.mu.RUnlock()
	if !ok || !s.recordMatchesSubscription(record, sub) {
		return SubscriptionLivenessRecord{Status: subscriptionLivenessUntested, Message: "尚未测活"}
	}
	return record
}

func (s *subscriptionLivenessStore) recordMatchesSubscription(record SubscriptionLivenessRecord, sub Subscription) bool {
	recordServer := strings.TrimRight(strings.TrimSpace(record.BarkServer), "/")
	if recordServer == "" {
		// Version 1 records did not persist the Bark server. Keep their old
		// timestamp behavior only until startup migration can bind them safely.
		return record.SubscriptionUpdatedAt == sub.UpdatedAt
	}
	return recordServer == s.subscriptionServer(sub)
}

func (s *subscriptionLivenessStore) subscriptionServer(sub Subscription) string {
	return normalizeBarkServer(sub.BarkServer, Config{Bark: s.bark})
}

// MigrateLegacy binds version 1 labels to the current Bark server only when
// the subscription has not changed since it was measured. This preserves all
// trustworthy labels without guessing about already-stale records.
func (s *subscriptionLivenessStore) MigrateLegacy(subscriptions []Subscription) (int, error) {
	if s == nil || s.path == "" {
		return 0, nil
	}
	active := make(map[string]Subscription, len(subscriptions))
	for _, sub := range subscriptions {
		active[sub.BarkID] = sub
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next := make(map[string]SubscriptionLivenessRecord, len(s.records))
	for barkID, record := range s.records {
		next[barkID] = record
	}
	migrated := 0
	for barkID, record := range next {
		if strings.TrimSpace(record.BarkServer) != "" {
			continue
		}
		sub, ok := active[barkID]
		if !ok || record.SubscriptionUpdatedAt != sub.UpdatedAt {
			continue
		}
		record.BarkServer = s.subscriptionServer(sub)
		next[barkID] = record
		migrated++
	}
	if migrated == 0 && s.version == subscriptionLivenessFileVersion {
		return 0, nil
	}
	if err := s.persistLocked(next, s.updatedAt); err != nil {
		return 0, err
	}
	return migrated, nil
}

func (s *subscriptionLivenessStore) Update(records map[string]SubscriptionLivenessRecord, subscriptions []Subscription, now time.Time) error {
	if s == nil || s.path == "" {
		return errors.New("subscription liveness label path is unavailable")
	}
	active := make(map[string]Subscription, len(subscriptions))
	for _, sub := range subscriptions {
		active[sub.BarkID] = sub
	}
	checkedAt := now.UTC().Format(time.RFC3339)

	s.mu.Lock()
	defer s.mu.Unlock()
	next := make(map[string]SubscriptionLivenessRecord, len(s.records)+len(records))
	for barkID, record := range s.records {
		sub, ok := active[barkID]
		if ok && s.recordMatchesSubscription(record, sub) {
			next[barkID] = record
		}
	}
	for barkID, record := range records {
		sub, ok := active[barkID]
		if !ok || !validSubscriptionLivenessStatus(record.Status) {
			return errors.New("cannot save an invalid subscription liveness label")
		}
		record.CheckedAt = checkedAt
		record.SubscriptionUpdatedAt = sub.UpdatedAt
		record.BarkServer = s.subscriptionServer(sub)
		next[barkID] = record
	}
	if err := s.persistLocked(next, checkedAt); err != nil {
		return err
	}
	return nil
}

func (s *subscriptionLivenessStore) persistLocked(next map[string]SubscriptionLivenessRecord, updatedAt string) error {
	payload := subscriptionLivenessFile{Version: subscriptionLivenessFileVersion, UpdatedAt: updatedAt, Records: next}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o750); err != nil {
		return fmt.Errorf("create subscription liveness label directory: %w", err)
	}
	temp, err := os.CreateTemp(filepath.Dir(s.path), ".subscription-liveness-*.json")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err == nil {
		_, err = temp.Write(append(data, '\n'))
	}
	if err == nil {
		err = temp.Sync()
	}
	if closeErr := temp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(tempName, s.path); err != nil {
		return err
	}
	s.version = subscriptionLivenessFileVersion
	s.records = next
	s.updatedAt = updatedAt
	return nil
}

func subscriptionLivenessRecordFor(cfg Config, sub Subscription) SubscriptionLivenessRecord {
	if cfg.subscriptionLiveness == nil {
		return SubscriptionLivenessRecord{Status: subscriptionLivenessUntested, Message: "尚未测活"}
	}
	return cfg.subscriptionLiveness.Snapshot(sub)
}
