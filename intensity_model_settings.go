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

type IntensityModelSettings struct {
	Mode              string            `json:"mode"`
	MaxCorrection     float64           `json:"max_correction"`
	ModelVersion      string            `json:"model_version"`
	ModelVersions     map[string]string `json:"model_versions"`
	StandardName      string            `json:"standard_name"`
	SiteDataVersion   string            `json:"site_data_version"`
	SiteCorrectionMax float64           `json:"site_correction_max"`
	Forced            bool              `json:"forced"`
	UpdatedAt         string            `json:"updated_at,omitempty"`
}

type intensityModelSettingsStore struct {
	mu       sync.RWMutex
	path     string
	settings IntensityModelSettings
}

var intensityModelVersions = map[string]string{
	intensityModelModeLegacy:  "legacy",
	intensityModelModeActive:  officialIntensityModelVersion,
	intensityModelModeGBT2020: gbtIntensityModelVersion,
	intensityModelModeHybrid:  gbtIntensityModelVersion,
	intensityModelModeShadow:  gbtIntensityModelVersion,
}

func defaultIntensityModelSettings(alert AlertConfig) IntensityModelSettings {
	mode := normalizeIntensityModelMode(alert.IntensityModelMode)
	maxCorrection := alert.IntensityModelMaxCorrection
	if maxCorrection <= 0 || maxCorrection > 2 {
		maxCorrection = defaultModelCorrection
	}
	return IntensityModelSettings{
		Mode:              mode,
		MaxCorrection:     maxCorrection,
		ModelVersion:      intensityModelVersionForMode(mode),
		ModelVersions:     availableIntensityModelVersions(),
		StandardName:      gbtIntensityStandardName,
		SiteDataVersion:   geoSCKSiteDataVersion,
		SiteCorrectionMax: maxSiteIntensityIncrease,
	}
}

func defaultIntensityModelSettingsStore(cfg Config) *intensityModelSettingsStore {
	store := &intensityModelSettingsStore{settings: defaultIntensityModelSettings(cfg.Alert)}
	if dataPath := strings.TrimSpace(cfg.Server.DataPath); dataPath != "" {
		store.path = filepath.Join(filepath.Dir(dataPath), "intensity-model.json")
	}
	return store
}

func newIntensityModelSettingsStore(cfg Config) (*intensityModelSettingsStore, error) {
	store := defaultIntensityModelSettingsStore(cfg)
	if store.path == "" {
		return store, nil
	}
	data, err := os.ReadFile(store.path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, err
	}
	var stored struct {
		Mode          string   `json:"mode"`
		MaxCorrection *float64 `json:"max_correction"`
		UpdatedAt     string   `json:"updated_at"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&stored); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("intensity model settings must contain one JSON document")
		}
		return nil, err
	}
	mode, err := validateIntensityModelMode(stored.Mode)
	if err != nil {
		return nil, err
	}
	store.settings.Mode = mode
	if stored.MaxCorrection != nil {
		if err := validateIntensityModelCorrection(*stored.MaxCorrection); err != nil {
			return nil, err
		}
		store.settings.MaxCorrection = *stored.MaxCorrection
	}
	store.settings.UpdatedAt = strings.TrimSpace(stored.UpdatedAt)
	return store, nil
}

func validateIntensityModelMode(value string) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(value))
	if mode != intensityModelModeLegacy && mode != intensityModelModeShadow && mode != intensityModelModeActive &&
		mode != intensityModelModeGBT2020 && mode != intensityModelModeHybrid {
		return "", errors.New("intensity model mode must be legacy, shadow, active, gbt2020, or hybrid")
	}
	return mode, nil
}

func validateIntensityModelCorrection(value float64) error {
	if value <= 0 || value > 2 {
		return errors.New("intensity model max correction must be greater than 0 and at most 2")
	}
	return nil
}

func (s *intensityModelSettingsStore) Snapshot() IntensityModelSettings {
	if s == nil {
		return defaultIntensityModelSettings(AlertConfig{})
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	settings := s.settings
	settings.ModelVersion = intensityModelVersionForMode(settings.Mode)
	settings.ModelVersions = availableIntensityModelVersions()
	settings.StandardName = gbtIntensityStandardName
	settings.SiteDataVersion = geoSCKSiteDataVersion
	settings.SiteCorrectionMax = maxSiteIntensityIncrease
	return settings
}

func (s *intensityModelSettingsStore) Update(mode string, now time.Time) (IntensityModelSettings, error) {
	if s == nil || s.path == "" {
		return IntensityModelSettings{}, errors.New("intensity model settings path is unavailable")
	}
	mode, err := validateIntensityModelMode(mode)
	if err != nil {
		return IntensityModelSettings{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	settings := s.settings
	settings.Mode = mode
	settings.ModelVersion = intensityModelVersionForMode(mode)
	settings.ModelVersions = availableIntensityModelVersions()
	settings.StandardName = gbtIntensityStandardName
	settings.SiteDataVersion = geoSCKSiteDataVersion
	settings.SiteCorrectionMax = maxSiteIntensityIncrease
	settings.Forced = false
	settings.UpdatedAt = now.UTC().Format(time.RFC3339)
	data, err := json.MarshalIndent(struct {
		Mode          string  `json:"mode"`
		MaxCorrection float64 `json:"max_correction"`
		UpdatedAt     string  `json:"updated_at"`
	}{settings.Mode, settings.MaxCorrection, settings.UpdatedAt}, "", "  ")
	if err != nil {
		return IntensityModelSettings{}, err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o750); err != nil {
		return IntensityModelSettings{}, fmt.Errorf("create settings directory: %w", err)
	}
	temp, err := os.CreateTemp(filepath.Dir(s.path), ".intensity-model-*.json")
	if err != nil {
		return IntensityModelSettings{}, err
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
		return IntensityModelSettings{}, err
	}
	if err := os.Rename(tempName, s.path); err != nil {
		return IntensityModelSettings{}, err
	}
	s.settings = settings
	return settings, nil
}

func intensityModelSettings(cfg Config) IntensityModelSettings {
	settings := defaultIntensityModelSettings(cfg.Alert)
	if cfg.intensityModelSettings != nil {
		settings = cfg.intensityModelSettings.Snapshot()
	}
	if cfg.intensityModelModeOverride != "" {
		settings.Mode = cfg.intensityModelModeOverride
		settings.Forced = true
	}
	settings.ModelVersion = intensityModelVersionForMode(settings.Mode)
	settings.ModelVersions = availableIntensityModelVersions()
	settings.StandardName = gbtIntensityStandardName
	settings.SiteDataVersion = geoSCKSiteDataVersion
	settings.SiteCorrectionMax = maxSiteIntensityIncrease
	return settings
}

func availableIntensityModelVersions() map[string]string {
	return intensityModelVersions
}
