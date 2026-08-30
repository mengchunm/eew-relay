package main

import (
	"math"
)

const (
	yu2013IntensityAlgorithmVersion = "yu2013-gbt17742-continuous-coverage-v2"
	yu2013ReferenceDepthKM          = 15.0
	gbtIntensityStandardName        = "GB/T 17742-2020"
	// An event-balanced replay of 31,956 official strong-motion records from
	// 487 earthquakes put the broad minimum near 0.40-0.43. Use the round
	// value so the runtime formula remains easy to audit and reproduce.
	yu2013GroundMotionWeight = 0.40
	yu2013BaselineWeight     = 1 - yu2013GroundMotionWeight
)

// yu2013Coefficients contains the published China-wide Yu et al. (2013)
// attenuation coefficients. The equations are reimplemented from the paper;
// no OpenQuake source code is included in this project.
type yu2013Coefficients struct {
	a, b, c, d, e      float64
	ua, ub, uc, ud, ue float64
	ma, mb, mc, md, me float64
	ia, ib, ic, id, ie float64
}

var (
	yu2013PGACoefficients = yu2013Coefficients{
		a: 4.1193, b: 1.656, c: -2.389, d: 1.772, e: 0.424,
		ua: 7.8269, ub: 1.0856, uc: -2.389, ud: 1.772, ue: 0.424,
		ma: 2.2609, mb: 1.6399, mc: -2.118, md: 0.825, me: 0.465,
		ia: 6.003, ib: 1.0649, ic: -2.118, id: 0.825, ie: 0.465,
	}
	yu2013PGVCoefficients = yu2013Coefficients{
		a: -1.2581, b: 1.932, c: -2.181, d: 1.772, e: 0.424,
		ua: 3.013, ub: 1.2742, uc: -2.181, ud: 1.772, ue: 0.424,
		ma: -3.1073, mb: 1.9389, mc: -1.945, md: 0.825, me: 0.465,
		ia: 1.3087, ib: 1.2627, ic: -1.945, id: 0.825, ie: 0.465,
	}
)

type yu2013SelectedCoefficients struct {
	a1a, a1b, a1c, a1d, a1e float64
	a2a, a2b, a2c, a2d, a2e float64
}

func selectYu2013Coefficients(coeff yu2013Coefficients, magnitude float64) yu2013SelectedCoefficients {
	if magnitude > 6.5 {
		return yu2013SelectedCoefficients{
			a1a: coeff.ua, a1b: coeff.ub, a1c: coeff.uc, a1d: coeff.ud, a1e: coeff.ue,
			a2a: coeff.ia, a2b: coeff.ib, a2c: coeff.ic, a2d: coeff.id, a2e: coeff.ie,
		}
	}
	return yu2013SelectedCoefficients{
		a1a: coeff.a, a1b: coeff.b, a1c: coeff.c, a1d: coeff.d, a1e: coeff.e,
		a2a: coeff.ma, a2b: coeff.mb, a2c: coeff.mc, a2d: coeff.md, a2e: coeff.me,
	}
}

func yu2013MinorAxis(majorAxis, magnitude float64, selected yu2013SelectedCoefficients) float64 {
	term1 := selected.a1a + selected.a1b*magnitude + selected.a1c*math.Log(majorAxis+selected.a1d*math.Exp(selected.a1e*magnitude))
	term2 := selected.a2a + selected.a2b*magnitude
	minorAxis := math.Exp((term1-term2)/selected.a2c) - selected.a2d*math.Exp(selected.a2e*magnitude)
	if minorAxis < 0 || math.IsNaN(minorAxis) || math.IsInf(minorAxis, 0) {
		return 0
	}
	return minorAxis
}

func yu2013EquivalentMajorAxis(epicentralKM, azimuthDegrees, magnitude float64, selected yu2013SelectedCoefficients) float64 {
	if epicentralKM <= 0 {
		return 0
	}
	angle := azimuthDegrees * math.Pi / 180
	sinSquared := math.Pow(math.Sin(angle), 2)
	cosSquared := math.Pow(math.Cos(angle), 2)
	adjustedDistance := func(majorAxis float64) float64 {
		minorAxis := yu2013MinorAxis(majorAxis, magnitude, selected)
		denominator := math.Sqrt(majorAxis*majorAxis*sinSquared + minorAxis*minorAxis*cosSquared)
		if denominator <= 0 {
			return 0
		}
		return majorAxis * minorAxis / denominator
	}

	low := 0.0
	high := math.Max(200, epicentralKM*2)
	for attempts := 0; attempts < 4 && adjustedDistance(high) < epicentralKM; attempts++ {
		high *= 2
	}
	// Twenty-four bisection steps are well below one metre at the supported
	// range while keeping the per-subscription calculation deterministic.
	for step := 0; step < 24; step++ {
		mid := (low + high) / 2
		if adjustedDistance(mid) < epicentralKM {
			low = mid
		} else {
			high = mid
		}
	}
	return (low + high) / 2
}

func yu2013GroundMotion(magnitude, epicentralKM, azimuthDegrees float64, coeff yu2013Coefficients) float64 {
	selected := selectYu2013Coefficients(coeff, magnitude)
	majorAxis := yu2013EquivalentMajorAxis(epicentralKM, azimuthDegrees, magnitude, selected)
	minorAxis := yu2013MinorAxis(majorAxis, magnitude, selected)
	referenceDepthSquared := yu2013ReferenceDepthKM * yu2013ReferenceDepthKM
	meanMajor := selected.a1a + selected.a1b*magnitude + selected.a1c*math.Log(math.Sqrt(majorAxis*majorAxis+referenceDepthSquared)+selected.a1d*math.Exp(selected.a1e*magnitude))
	meanMinor := selected.a2a + selected.a2b*magnitude + selected.a2c*math.Log(math.Sqrt(minorAxis*minorAxis+referenceDepthSquared)+selected.a2d*math.Exp(selected.a2e*magnitude))
	angle := azimuthDegrees * math.Pi / 180
	// Combine positive ground-motion amplitudes rather than their logarithms.
	// Combining signed log values creates an artificial upturn after a value
	// crosses 1 cm/s, which violates the attenuation relation.
	motionMajor := math.Exp(meanMajor)
	motionMinor := math.Exp(meanMinor)
	denominator := math.Sqrt(math.Pow(motionMajor*math.Sin(angle), 2) + math.Pow(motionMinor*math.Cos(angle), 2))
	if denominator <= 0 {
		return 0
	}
	value := motionMajor * motionMinor / denominator / 100 // published units are cm/s² or cm/s
	if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return value
}

func yu2013Intensity(event Event, epicentralKM, azimuthDegrees float64) (standard, pgaMPS2, pgvMPS float64, reason string) {
	if math.IsNaN(event.Magnitude) || math.IsInf(event.Magnitude, 0) ||
		math.IsNaN(event.DepthKM) || math.IsInf(event.DepthKM, 0) ||
		math.IsNaN(event.Latitude) || math.IsInf(event.Latitude, 0) ||
		math.IsNaN(event.Longitude) || math.IsInf(event.Longitude, 0) ||
		math.IsNaN(epicentralKM) || math.IsInf(epicentralKM, 0) ||
		math.IsNaN(azimuthDegrees) || math.IsInf(azimuthDegrees, 0) {
		return 0, 0, 0, "non_finite_input"
	}
	if event.Magnitude <= 0 {
		return 0, 0, 0, "magnitude_out_of_domain"
	}
	if epicentralKM < 0 {
		return 0, 0, 0, "distance_out_of_domain"
	}

	// The complete algorithm always has a deterministic result. For depths
	// beyond Yu's 15 km reference, preserve hypocentral distance by increasing
	// the equivalent epicentral distance. Shallower events retain the published
	// reference depth instead of receiving an unsupported near-surface boost.
	effectiveDistanceKM := yu2013EffectiveEpicentralDistance(epicentralKM, event.DepthKM)
	baseline := stableIntensityBaseline(event.Magnitude, effectiveDistanceKM)
	motionDistanceKM := math.Max(1, effectiveDistanceKM)
	pgaMPS2 = yu2013GroundMotion(event.Magnitude, motionDistanceKM, azimuthDegrees, yu2013PGACoefficients)
	pgvMPS = yu2013GroundMotion(event.Magnitude, motionDistanceKM, azimuthDegrees, yu2013PGVCoefficients)
	groundMotionIntensity := gbtPredictiveIntensity(pgaMPS2, pgvMPS)
	if groundMotionIntensity < 0 {
		return 0, 0, 0, "non_finite_algorithm_output"
	}
	standard = clampFloat(yu2013GroundMotionWeight*groundMotionIntensity+yu2013BaselineWeight*baseline, 0, 12)
	return standard, pgaMPS2, pgvMPS, ""
}

func yu2013EffectiveEpicentralDistance(epicentralKM, depthKM float64) float64 {
	depthKM = math.Max(0, depthKM)
	if depthKM <= yu2013ReferenceDepthKM {
		return epicentralKM
	}
	return math.Sqrt(epicentralKM*epicentralKM + depthKM*depthKM - yu2013ReferenceDepthKM*yu2013ReferenceDepthKM)
}

func initialBearingDegrees(fromLat, fromLon, toLat, toLon float64) float64 {
	fromLatitude := fromLat * math.Pi / 180
	toLatitude := toLat * math.Pi / 180
	deltaLongitude := (toLon - fromLon) * math.Pi / 180
	y := math.Sin(deltaLongitude) * math.Cos(toLatitude)
	x := math.Cos(fromLatitude)*math.Sin(toLatitude) - math.Sin(fromLatitude)*math.Cos(toLatitude)*math.Cos(deltaLongitude)
	bearing := math.Atan2(y, x) * 180 / math.Pi
	if bearing < 0 {
		bearing += 360
	}
	return bearing
}
