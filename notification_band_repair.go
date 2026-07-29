package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

type notificationBandRepairSummary struct {
	Scanned              int
	Repaired             int
	IndividuallyRepaired int
	ResetToDefaults      int
}

type notificationBandRepairPlan struct {
	BeforeUpdatedAt int64
	Subscription    Subscription
}

type notificationBandRepairItem struct {
	band    NotificationBand
	rank    int
	invalid bool
}

func repairStoredNotificationBands(ctx context.Context, store *Store, apply bool) (notificationBandRepairSummary, error) {
	summary := notificationBandRepairSummary{Scanned: store.Count()}
	plans := make([]notificationBandRepairPlan, 0)
	for _, sub := range store.List() {
		repaired, changed, resetToDefaults := repairInvalidNotificationBands(sub.NotifyBands)
		if !changed {
			continue
		}
		candidate := sub
		candidate.NotifyBands = repaired
		if err := validateSubscription(candidate); err != nil {
			return summary, fmt.Errorf("repaired subscription %s is still invalid: %w", maskKey(sub.BarkID), err)
		}
		plans = append(plans, notificationBandRepairPlan{BeforeUpdatedAt: sub.UpdatedAt, Subscription: candidate})
		summary.Repaired++
		if resetToDefaults {
			summary.ResetToDefaults++
		} else {
			summary.IndividuallyRepaired++
		}
	}
	if apply && len(plans) > 0 {
		if err := applyNotificationBandRepairs(ctx, store, plans); err != nil {
			return summary, err
		}
	}
	return summary, nil
}

func repairInvalidNotificationBands(bands []NotificationBand) ([]NotificationBand, bool, bool) {
	normalized := normalizeNotificationBands(append([]NotificationBand(nil), bands...), NotificationRules{})
	if validateNotificationBands(normalized) == nil {
		return normalized, false, false
	}
	if len(normalized) == 0 || len(normalized) > 3 {
		return defaultNotificationBandsForRepair(), true, true
	}

	ordered := make([]notificationBandRepairItem, 0, len(normalized))
	seenLevels := make(map[string]bool, len(normalized))
	for _, band := range normalized {
		rank, ok := notificationBandLevelRank(band.Level)
		if !ok || seenLevels[band.Level] || !notificationBandBoundsRepairable(band) {
			return defaultNotificationBandsForRepair(), true, true
		}
		seenLevels[band.Level] = true
		ordered = append(ordered, notificationBandRepairItem{band: band, rank: rank, invalid: band.Max != notificationOpenEndedMax && band.Min >= band.Max})
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].rank < ordered[j].rank })

	previousValid := -1
	for index, item := range ordered {
		if item.invalid {
			continue
		}
		if previousValid >= 0 && ordered[previousValid].band.Max > item.band.Min {
			return defaultNotificationBandsForRepair(), true, true
		}
		previousValid = index
	}

	for start := 0; start < len(ordered); {
		if !ordered[start].invalid {
			start++
			continue
		}
		end := start
		for end+1 < len(ordered) && ordered[end+1].invalid {
			end++
		}
		for index := start; index <= end; index++ {
			if ordered[index].band.Level == "critical" {
				return defaultNotificationBandsForRepair(), true, true
			}
		}
		lower := 0
		if start > 0 {
			lower = ordered[start-1].band.Max
		}
		upper := 8
		if end+1 < len(ordered) {
			upper = ordered[end+1].band.Min
		}
		starts, ok := closestNonConflictingUnitBands(ordered[start:end+1], lower, upper)
		if !ok {
			return defaultNotificationBandsForRepair(), true, true
		}
		for offset, minValue := range starts {
			ordered[start+offset].band.Min = minValue
			ordered[start+offset].band.Max = minValue + 1
		}
		start = end + 1
	}

	repaired := make([]NotificationBand, 0, len(ordered))
	for _, item := range ordered {
		repaired = append(repaired, item.band)
	}
	repaired = normalizeNotificationBands(repaired, NotificationRules{})
	if validateNotificationBands(repaired) != nil {
		return defaultNotificationBandsForRepair(), true, true
	}
	return repaired, true, false
}

func defaultNotificationBandsForRepair() []NotificationBand {
	return normalizeNotificationBands(defaultNotificationBands(), NotificationRules{})
}

func notificationBandLevelRank(level string) (int, bool) {
	switch normalizeNotifyLevel(level) {
	case "passive":
		return 0, true
	case "active":
		return 1, true
	case "critical":
		return 2, true
	default:
		return 0, false
	}
}

func notificationBandBoundsRepairable(band NotificationBand) bool {
	if band.Min < 0 || band.Min > 7 || band.Max < 0 || band.Max > notificationOpenEndedMax {
		return false
	}
	return band.Max <= 7 || band.Max == notificationOpenEndedMax
}

func closestNonConflictingUnitBands(items []notificationBandRepairItem, lower, upper int) ([]int, bool) {
	if len(items) == 0 || lower < 0 || upper > 8 || upper-lower < len(items) {
		return nil, false
	}
	current := make([]int, len(items))
	best := []int(nil)
	bestCost := int(^uint(0) >> 1)
	var search func(index, nextMin int)
	search = func(index, nextMin int) {
		if index == len(items) {
			cost := 0
			for itemIndex, start := range current {
				desired := 1
				if items[itemIndex].rank == 1 {
					desired = 2
				}
				cost += absInt(start-desired)*1000 + absInt(start-items[itemIndex].band.Min)
			}
			if cost < bestCost {
				bestCost = cost
				best = append(best[:0], current...)
			}
			return
		}
		lastStart := upper - (len(items) - index)
		for start := nextMin; start <= lastStart; start++ {
			current[index] = start
			search(index+1, start+1)
		}
	}
	search(0, lower)
	return best, len(best) == len(items)
}

func applyNotificationBandRepairs(ctx context.Context, store *Store, plans []notificationBandRepairPlan) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	now := time.Now().UnixMilli()
	prepared := make([]notificationBandRepairPlan, len(plans))
	copy(prepared, plans)
	for index := range prepared {
		current, ok := store.subscriptions[prepared[index].Subscription.BarkID]
		if !ok || current.UpdatedAt != prepared[index].BeforeUpdatedAt {
			return fmt.Errorf("subscription %s changed while preparing repair", maskKey(prepared[index].Subscription.BarkID))
		}
		prepared[index].Subscription.CreatedAt = current.CreatedAt
		prepared[index].Subscription.UpdatedAt = now + int64(index)
	}

	if backend, ok := store.backend.(*postgresSubscriptionBackend); ok {
		tx, err := backend.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		for _, plan := range prepared {
			data, err := json.Marshal(plan.Subscription)
			if err != nil {
				return err
			}
			result, err := tx.ExecContext(ctx, `
				UPDATE eew_subscriptions
				SET data=$1::jsonb, updated_at=$2
				WHERE bark_id=$3 AND updated_at=$4`, data, plan.Subscription.UpdatedAt, plan.Subscription.BarkID, plan.BeforeUpdatedAt)
			if err != nil {
				return err
			}
			changed, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if changed != 1 {
				return fmt.Errorf("subscription %s changed concurrently; no repairs were committed", maskKey(plan.Subscription.BarkID))
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		for _, plan := range prepared {
			store.subscriptions[plan.Subscription.BarkID] = plan.Subscription
		}
		return nil
	}

	previous := make(map[string]Subscription, len(prepared))
	for _, plan := range prepared {
		previous[plan.Subscription.BarkID] = store.subscriptions[plan.Subscription.BarkID]
		store.subscriptions[plan.Subscription.BarkID] = plan.Subscription
	}
	if err := store.saveLocked(); err != nil {
		for barkID, sub := range previous {
			store.subscriptions[barkID] = sub
		}
		return err
	}
	return nil
}
