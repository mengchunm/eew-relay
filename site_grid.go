package main

import (
	"bytes"
	"compress/gzip"
	_ "embed"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"
)

const geoSCKGridHeaderSize = 48

//go:embed site_data/geosck_vs30_cn_1arcmin.bin.gz
var geoSCKGridCompressed []byte

type geoSCKGrid struct {
	width     int
	height    int
	originLon float64
	originLat float64
	step      float64
	scale     float64
	cells     []byte
}

var (
	geoSCKGridOnce sync.Once
	geoSCKGridData *geoSCKGrid
	geoSCKGridErr  error
)

func loadGeoSCKGrid() (*geoSCKGrid, error) {
	geoSCKGridOnce.Do(func() {
		reader, err := gzip.NewReader(bytes.NewReader(geoSCKGridCompressed))
		if err != nil {
			geoSCKGridErr = err
			return
		}
		defer reader.Close()
		header := make([]byte, geoSCKGridHeaderSize)
		if _, err := io.ReadFull(reader, header); err != nil {
			geoSCKGridErr = err
			return
		}
		if !bytes.Equal(header[:8], []byte("EEWVS30\x01")) {
			geoSCKGridErr = errors.New("invalid GeoSCK grid magic")
			return
		}
		width := int(binary.LittleEndian.Uint32(header[8:12]))
		height := int(binary.LittleEndian.Uint32(header[12:16]))
		originLon := math.Float64frombits(binary.LittleEndian.Uint64(header[16:24]))
		originLat := math.Float64frombits(binary.LittleEndian.Uint64(header[24:32]))
		step := math.Float64frombits(binary.LittleEndian.Uint64(header[32:40]))
		scale := math.Float64frombits(binary.LittleEndian.Uint64(header[40:48]))
		if width <= 0 || height <= 0 || width > 10000 || height > 10000 || step <= 0 || scale <= 0 {
			geoSCKGridErr = errors.New("invalid GeoSCK grid metadata")
			return
		}
		cellCount := width * height
		cells, err := io.ReadAll(io.LimitReader(reader, int64(cellCount+1)))
		if err != nil {
			geoSCKGridErr = err
			return
		}
		if len(cells) != cellCount {
			geoSCKGridErr = errors.New("invalid GeoSCK grid cell count")
			return
		}
		geoSCKGridData = &geoSCKGrid{
			width: width, height: height,
			originLon: originLon, originLat: originLat,
			step: step, scale: scale, cells: cells,
		}
	})
	return geoSCKGridData, geoSCKGridErr
}

func lookupGeoSCKSiteCondition(latitude, longitude float64) (SiteCondition, bool) {
	if math.IsNaN(latitude) || math.IsInf(latitude, 0) || math.IsNaN(longitude) || math.IsInf(longitude, 0) {
		return SiteCondition{}, false
	}
	grid, err := loadGeoSCKGrid()
	if err != nil || grid == nil {
		return SiteCondition{}, false
	}
	column := int(math.Floor((longitude - grid.originLon) / grid.step))
	row := int(math.Floor((grid.originLat - latitude) / grid.step))
	if column < 0 || column >= grid.width || row < 0 || row >= grid.height {
		return SiteCondition{}, false
	}
	value := grid.cells[row*grid.width+column]
	if value == 0 {
		return SiteCondition{}, false
	}
	return SiteCondition{
		VS30:    float64(value) * grid.scale,
		Version: geoSCKSiteDataVersion,
	}, true
}
