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

type NotificationDisplaySettings struct {
	ShowIntensity     bool   `json:"show_intensity"`
	ShowEstimatedTime bool   `json:"show_estimated_time"`
	UpdatedAt         string `json:"updated_at,omitempty"`
}

type notificationDisplaySettingsStore struct {
	mu       sync.RWMutex
	path     string
	settings NotificationDisplaySettings
}

func defaultNotificationDisplaySettings() NotificationDisplaySettings {
	return NotificationDisplaySettings{ShowIntensity: true, ShowEstimatedTime: true}
}

func newNotificationDisplaySettingsStore(cfg Config) (*notificationDisplaySettingsStore, error) {
	settings := defaultNotificationDisplaySettings()
	dataPath := strings.TrimSpace(cfg.Server.DataPath)
	if dataPath == "" {
		return &notificationDisplaySettingsStore{settings: settings}, nil
	}
	path := filepath.Join(filepath.Dir(dataPath), "notification-display.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return &notificationDisplaySettingsStore{path: path, settings: settings}, nil
	}
	if err != nil {
		return nil, err
	}
	var stored struct {
		ShowIntensity     *bool  `json:"show_intensity"`
		ShowEstimatedTime *bool  `json:"show_estimated_time"`
		UpdatedAt         string `json:"updated_at"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&stored); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("notification display settings must contain one JSON document")
		}
		return nil, err
	}
	if stored.ShowIntensity != nil {
		settings.ShowIntensity = *stored.ShowIntensity
	}
	if stored.ShowEstimatedTime != nil {
		settings.ShowEstimatedTime = *stored.ShowEstimatedTime
	}
	settings.UpdatedAt = strings.TrimSpace(stored.UpdatedAt)
	return &notificationDisplaySettingsStore{path: path, settings: settings}, nil
}

func (s *notificationDisplaySettingsStore) Snapshot() NotificationDisplaySettings {
	if s == nil {
		return defaultNotificationDisplaySettings()
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.settings
}

func (s *notificationDisplaySettingsStore) Update(showIntensity, showEstimatedTime bool, now time.Time) (NotificationDisplaySettings, error) {
	if s == nil || s.path == "" {
		return NotificationDisplaySettings{}, errors.New("notification display settings path is unavailable")
	}
	settings := NotificationDisplaySettings{
		ShowIntensity:     showIntensity,
		ShowEstimatedTime: showEstimatedTime,
		UpdatedAt:         now.UTC().Format(time.RFC3339),
	}
	data, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return NotificationDisplaySettings{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.path), 0o750); err != nil {
		return NotificationDisplaySettings{}, fmt.Errorf("create settings directory: %w", err)
	}
	temp, err := os.CreateTemp(filepath.Dir(s.path), ".notification-display-*.json")
	if err != nil {
		return NotificationDisplaySettings{}, err
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
		return NotificationDisplaySettings{}, err
	}
	if err := os.Rename(tempName, s.path); err != nil {
		return NotificationDisplaySettings{}, err
	}
	s.settings = settings
	return settings, nil
}

func notificationDisplaySettings(cfg Config) NotificationDisplaySettings {
	if cfg.notificationDisplay == nil {
		return defaultNotificationDisplaySettings()
	}
	return cfg.notificationDisplay.Snapshot()
}
