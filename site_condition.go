package main

import (
	"math"
)

const (
	geoSCKSiteDataVersion    = "geosck-cn-2026-1arcmin-v1"
	geoSCKReferenceVS30      = 412.0
	maxSiteIntensityIncrease = 0.25
	maxSiteIntensityDecrease = 0.25
)

type SiteCondition struct {
	VS30        float64
	Uncertainty float64
	Version     string
}

func applySiteCondition(location *SubscriptionLocation) {
	if location == nil {
		return
	}
	site, ok := lookupGeoSCKSiteCondition(location.Latitude, location.Longitude)
	if !ok {
		location.SiteVS30 = 0
		location.SiteUncertainty = 0
		location.SiteDataVersion = ""
		return
	}
	location.SiteVS30 = site.VS30
	location.SiteUncertainty = site.Uncertainty
	location.SiteDataVersion = site.Version
}

func siteIntensityCorrection(site SiteCondition, baseline float64) (float64, string) {
	if site.Version != geoSCKSiteDataVersion || site.VS30 == 0 {
		return 0, "site_data_unavailable"
	}
	if math.IsNaN(site.VS30) || math.IsInf(site.VS30, 0) || site.VS30 < 100 || site.VS30 > 2200 {
		return 0, "site_data_invalid"
	}
	// The distributed GeoSCK raster does not include its uncertainty layer.
	// Treat proxy-only cells conservatively instead of assuming measured-site
	// confidence. A future measured value can supply its explicit uncertainty.
	confidence := 0.65
	if site.Uncertainty > 0 {
		if math.IsNaN(site.Uncertainty) || math.IsInf(site.Uncertainty, 0) || site.Uncertainty > 0.35 {
			return 0, "site_data_low_confidence"
		}
		confidence = clampFloat((0.35-site.Uncertainty)/0.25, 0.25, 1)
	}
	// GeoSCK provides the receiving site's Vs30. Use it only as a small,
	// confidence-weighted correction around the GeoSCK mainland mean. Relative
	// soft and hard sites get small opposite corrections without assuming that
	// all terrain effects are monotonic.
	// The correction weakens for already severe shaking and remains inside the
	// model's existing overall safety bound.
	severityScale := clampFloat(1-math.Max(0, baseline-6)*0.12, 0.55, 1)
	correction := 0.24 * math.Log(geoSCKReferenceVS30/site.VS30) * confidence * severityScale
	correction = clampFloat(correction, -maxSiteIntensityDecrease, maxSiteIntensityIncrease)
	if math.Abs(correction) < 0.05 {
		return 0, "site_correction_negligible"
	}
	return correction, ""
}
