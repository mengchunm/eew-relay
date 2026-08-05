package main

import "testing"

func TestGeoSCKGridKnownMainlandLocations(t *testing.T) {
	for name, test := range map[string]struct {
		lat, lon float64
		want     float64
	}{
		"Chengdu":  {30.5728, 104.0668, 336},
		"Beijing":  {39.9042, 116.4074, 272},
		"Shanghai": {31.2304, 121.4737, 160},
		"Lhasa":    {29.6604, 91.1322, 216},
	} {
		t.Run(name, func(t *testing.T) {
			site, ok := lookupGeoSCKSiteCondition(test.lat, test.lon)
			if !ok || site.VS30 != test.want || site.Version != geoSCKSiteDataVersion {
				t.Fatalf("unexpected site lookup: ok=%t site=%#v", ok, site)
			}
		})
	}
}

func TestGeoSCKGridFailsSafeOutsideDataCoverage(t *testing.T) {
	for _, coordinates := range [][2]float64{{25.033, 121.5654}, {35.0, 140.0}, {-33.9, 151.2}} {
		if site, ok := lookupGeoSCKSiteCondition(coordinates[0], coordinates[1]); ok {
			t.Fatalf("expected no site data at %v, got %#v", coordinates, site)
		}
	}
}

func TestNormalizeSubscriptionUsesAuthoritativeSiteGrid(t *testing.T) {
	subscription := Subscription{
		LocationName: "成都",
		Latitude:     30.5728,
		Longitude:    104.0668,
		SiteVS30:     9999,
		Locations: []SubscriptionLocation{{
			Name: "成都", Latitude: 30.5728, Longitude: 104.0668,
			SiteVS30: 9999, SiteDataVersion: "untrusted-client-value",
		}},
	}
	normalizeSubscription(&subscription)
	if subscription.SiteVS30 != 336 || subscription.SiteDataVersion != geoSCKSiteDataVersion {
		t.Fatalf("top-level site condition was not normalized: %#v", subscription)
	}
	if len(subscription.Locations) != 1 || subscription.Locations[0].SiteVS30 != 336 || subscription.Locations[0].SiteDataVersion != geoSCKSiteDataVersion {
		t.Fatalf("location site condition was not normalized: %#v", subscription.Locations)
	}
}

func TestNormalizeSubscriptionClearsUntrustedSiteDataWithoutValidLocation(t *testing.T) {
	subscription := Subscription{
		LocationName:    "无效位置",
		Latitude:        999,
		Longitude:       999,
		SiteVS30:        120,
		SiteUncertainty: 0.01,
		SiteDataVersion: "untrusted-client-value",
	}
	normalizeSubscription(&subscription)
	if subscription.SiteVS30 != 0 || subscription.SiteUncertainty != 0 || subscription.SiteDataVersion != "" {
		t.Fatalf("untrusted site condition must be cleared: %#v", subscription)
	}
}

func BenchmarkGeoSCKLookup700Locations(b *testing.B) {
	_, _ = loadGeoSCKGrid()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		for index := 0; index < 700; index++ {
			latitude := 20 + float64(index%300)/10
			longitude := 80 + float64(index%500)/10
			_, _ = lookupGeoSCKSiteCondition(latitude, longitude)
		}
	}
}
