package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type subscriptionStoreBackend interface {
	Upsert(Subscription) error
	Delete(string) error
	Close() error
}

type postgresSubscriptionBackend struct {
	db                       *sql.DB
	adminBoundariesAvailable bool
}

type administrativeBoundaryLookup interface {
	LookupAdministrativeLocation(context.Context, float64, float64) (GeocodeResult, bool, error)
}

func NewConfiguredStore(path, postgresDSN string) (*Store, error) {
	store, err := NewStore(path)
	if err != nil {
		return nil, err
	}
	postgresDSN = strings.TrimSpace(postgresDSN)
	if postgresDSN == "" {
		return store, nil
	}
	backend, subscriptions, err := openPostgresSubscriptionBackend(postgresDSN, store.List())
	if err != nil {
		return nil, err
	}
	store.mu.Lock()
	store.backend = backend
	store.subscriptions = make(map[string]Subscription, len(subscriptions))
	for _, sub := range subscriptions {
		store.subscriptions[sub.BarkID] = sub
	}
	store.mu.Unlock()
	return store, nil
}

func openPostgresSubscriptionBackend(dsn string, initial []Subscription) (*postgresSubscriptionBackend, []Subscription, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, nil, err
	}
	db.SetMaxOpenConns(12)
	db.SetMaxIdleConns(4)
	db.SetConnMaxIdleTime(5 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("connect postgres: %w", err)
	}
	backend := &postgresSubscriptionBackend{db: db}
	if err := backend.ensureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	if err := backend.detectAdministrativeBoundaries(ctx); err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	if len(initial) > 0 {
		if err := backend.importSubscriptions(ctx, initial); err != nil {
			_ = db.Close()
			return nil, nil, fmt.Errorf("import subscriptions into postgres: %w", err)
		}
	}
	subscriptions, err := backend.loadAll(ctx)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return backend, subscriptions, nil
}

func (p *postgresSubscriptionBackend) detectAdministrativeBoundaries(ctx context.Context) error {
	return p.db.QueryRowContext(ctx, `
		SELECT to_regclass('public.eew_admin_boundaries') IS NOT NULL
			AND to_regclass('public.eew_admin_boundary_parts') IS NOT NULL`).Scan(&p.adminBoundariesAvailable)
}

func (p *postgresSubscriptionBackend) ensureSchema(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS eew_subscriptions (
			bark_id text PRIMARY KEY,
			data jsonb NOT NULL,
			created_at bigint NOT NULL,
			updated_at bigint NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS eew_subscription_locations (
			bark_id text NOT NULL REFERENCES eew_subscriptions(bark_id) ON DELETE CASCADE,
			location_index integer NOT NULL,
			latitude double precision NOT NULL,
			longitude double precision NOT NULL,
			PRIMARY KEY (bark_id, location_index)
		)`,
	}
	for _, statement := range statements {
		if _, err := p.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("initialize postgres subscription schema: %w", err)
		}
	}
	return nil
}

func (p *postgresSubscriptionBackend) loadAll(ctx context.Context) ([]Subscription, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT data FROM eew_subscriptions ORDER BY bark_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var subscriptions []Subscription
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var sub Subscription
		if err := json.Unmarshal(data, &sub); err != nil {
			return nil, err
		}
		normalizeSubscription(&sub)
		if sub.BarkID != "" && len(sub.Locations) > 0 {
			subscriptions = append(subscriptions, sub)
		}
	}
	return subscriptions, rows.Err()
}

func (p *postgresSubscriptionBackend) importSubscriptions(ctx context.Context, subscriptions []Subscription) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, sub := range subscriptions {
		if err := upsertPostgresSubscription(ctx, tx, sub); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (p *postgresSubscriptionBackend) Upsert(sub Subscription) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := upsertPostgresSubscription(ctx, tx, sub); err != nil {
		return err
	}
	return tx.Commit()
}

func upsertPostgresSubscription(ctx context.Context, tx *sql.Tx, sub Subscription) error {
	data, err := json.Marshal(sub)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
		INSERT INTO eew_subscriptions (bark_id, data, created_at, updated_at)
		VALUES ($1, $2::jsonb, $3, $4)
		ON CONFLICT (bark_id) DO UPDATE SET data=EXCLUDED.data, updated_at=EXCLUDED.updated_at
		WHERE EXCLUDED.updated_at > eew_subscriptions.updated_at`,
		sub.BarkID, data, sub.CreatedAt, sub.UpdatedAt)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM eew_subscription_locations WHERE bark_id=$1`, sub.BarkID); err != nil {
		return err
	}
	for index, location := range sub.Locations {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO eew_subscription_locations (bark_id, location_index, latitude, longitude)
			VALUES ($1, $2, $3, $4)`, sub.BarkID, index, location.Latitude, location.Longitude); err != nil {
			return err
		}
	}
	return nil
}

func (p *postgresSubscriptionBackend) Delete(barkID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := p.db.ExecContext(ctx, `DELETE FROM eew_subscriptions WHERE bark_id=$1`, barkID)
	return err
}

func (p *postgresSubscriptionBackend) LookupAdministrativeLocation(ctx context.Context, latitude, longitude float64) (GeocodeResult, bool, error) {
	if !p.adminBoundariesAvailable {
		return GeocodeResult{}, false, nil
	}
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	lookupLatitude, lookupLongitude := wgs84ToGCJ02(latitude, longitude)
	rows, err := p.db.QueryContext(queryCtx, `
		WITH point AS (
			SELECT ST_SetSRID(ST_Point($1, $2), 4326) AS geom
		)
		SELECT DISTINCT ON (boundary.level)
			boundary.level,
			boundary.gid,
			COALESCE(NULLIF(boundary.name_zh, ''), boundary.name_en)
		FROM eew_admin_boundary_parts AS boundary
		CROSS JOIN point
		WHERE boundary.geom && point.geom
			AND ST_Intersects(boundary.geom, point.geom)
		ORDER BY boundary.level, boundary.area_sq_degrees`, lookupLongitude, lookupLatitude)
	if err != nil {
		return GeocodeResult{}, false, err
	}
	defer rows.Close()
	var province, city, district string
	var deepestID string
	var deepestLevel int
	for rows.Next() {
		var level int
		var gid string
		var name string
		if err := rows.Scan(&level, &gid, &name); err != nil {
			return GeocodeResult{}, false, err
		}
		if level >= deepestLevel {
			deepestLevel = level
			deepestID = strings.TrimSpace(gid)
		}
		switch level {
		case 1:
			province = strings.TrimSpace(name)
		case 2:
			city = strings.TrimSpace(name)
		case 3:
			district = strings.TrimSpace(name)
		}
	}
	if err := rows.Err(); err != nil {
		return GeocodeResult{}, false, err
	}
	if province == "" && city == "" && district == "" {
		return GeocodeResult{}, false, nil
	}
	name := joinAdministrativeNames(province, city, district)
	return GeocodeResult{
		Name:                name,
		Address:             name,
		Latitude:            latitude,
		Longitude:           longitude,
		AdministrativeID:    deepestID,
		AdministrativeLevel: deepestLevel,
	}, true, nil
}

func joinAdministrativeNames(parts ...string) string {
	unique := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || (len(unique) > 0 && unique[len(unique)-1] == part) {
			continue
		}
		unique = append(unique, part)
	}
	return strings.Join(unique, " ")
}

func (p *postgresSubscriptionBackend) Close() error {
	return p.db.Close()
}
