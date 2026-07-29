package main

import (
	"strings"
	"testing"
	"time"
)

func TestAdminServiceHistoryPersistsAndKeepsWorstBucketStatus(t *testing.T) {
	cfg := adminTestConfig(t)
	store := newAdminServiceHistoryStore(cfg)
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	healthy := adminServiceHistoryPoint{
		CheckedAt: now.Add(-9 * time.Minute).Format(time.RFC3339),
		Statuses:  map[string]string{"overall": "healthy", "application": "healthy"},
	}
	unhealthy := adminServiceHistoryPoint{
		CheckedAt: now.Add(-4 * time.Minute).Format(time.RFC3339),
		Statuses:  map[string]string{"overall": "unhealthy", "application": "unhealthy"},
	}
	if err := store.Append(healthy, now); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(unhealthy, now); err != nil {
		t.Fatal(err)
	}

	reopened := newAdminServiceHistoryStore(cfg)
	current := adminServiceHistoryPoint{
		CheckedAt: now.Format(time.RFC3339),
		Statuses:  map[string]string{"overall": "healthy", "application": "healthy"},
	}
	response, err := reopened.Query(7*24, current, now)
	if err != nil {
		t.Fatal(err)
	}
	if response.BucketMinutes != 10 || len(response.Points) != 2 {
		t.Fatalf("unexpected history response: %#v", response)
	}
	if response.Points[0].Statuses["overall"] != "unhealthy" || response.Points[0].Statuses["application"] != "unhealthy" {
		t.Fatalf("bucket did not retain its worst status: %#v", response.Points[0])
	}
	if response.Points[1].Statuses["overall"] != "healthy" || response.StartedAt == "" || response.EndedAt == "" {
		t.Fatalf("current history point missing: %#v", response)
	}
}

func TestDecodeAdminServiceHistoryDropsExpiredAndInvalidLines(t *testing.T) {
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	input := strings.Join([]string{
		`{"checked_at":"2026-06-01T00:00:00Z","statuses":{"overall":"unhealthy"}}`,
		`not-json`,
		`{"checked_at":"2026-07-29T11:59:00Z","statuses":{"overall":"healthy"}}`,
	}, "\n")
	points := decodeHistoryPoints(strings.NewReader(input), now.Add(-30*24*time.Hour))
	if len(points) != 1 || points[0].Statuses["overall"] != "healthy" {
		t.Fatalf("unexpected retained history: %#v", points)
	}
}
