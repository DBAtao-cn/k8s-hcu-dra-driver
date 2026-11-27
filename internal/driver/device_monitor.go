// Copyright (c) 2026 Hygon Information Technology Co., Ltd.
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"fmt"
	"time"

	dcgm "github.com/HYGON-AI/hcu-dcgm/v3/pkg/dcgm"
	"k8s.io/klog/v2"
)

// monitorMaxFailStreak is the number of consecutive sampling failures after
// which a device is marked Unhealthy (the card may be offline or the driver
// unresponsive).
const monitorMaxFailStreak = 3

// DeviceHealthStatus is the tri-state health verdict for one physical HCU.
type DeviceHealthStatus string

const (
	DeviceHealthy   DeviceHealthStatus = "Healthy"
	DeviceUnhealthy DeviceHealthStatus = "Unhealthy"
	DeviceUnknown   DeviceHealthStatus = "Unknown"
)

// DeviceHealthInfo holds the latest metrics snapshot and health verdict for one device.
type DeviceHealthInfo struct {
	DeviceName   string // canonical ResourceSlice device name (PCI-based)
	DvInd        int
	PciBusNumber string

	// Latest metrics snapshot (best-effort; zero values when the query failed).
	TemperatureC   float64
	PowerUsageW    int64
	MemoryUsedMiB  float64
	MemoryTotalMiB float64
	UtilPercent    int

	Health      DeviceHealthStatus
	LastUpdated time.Time
	FailStreak  int // consecutive sampling failures; >= monitorMaxFailStreak marks Unhealthy
}

// sampleDeviceMetrics collects one metrics snapshot for alloc via dcgm.
// Metric fields stay at their zero values when the individual query fails;
// err is non-nil when a baseline query (temperature or utilization) failed.
func sampleDeviceMetrics(alloc *AllocatableDevice) (*DeviceHealthInfo, error) {
	info := &DeviceHealthInfo{
		DeviceName:   alloc.Name,
		DvInd:        alloc.DvInd,
		PciBusNumber: alloc.PciBusNumber,
		LastUpdated:  time.Now(),
	}

	var firstErr error
	markErr := func(what string, err error) {
		if err == nil || firstErr != nil {
			return
		}
		firstErr = fmt.Errorf("%s: %w", what, err)
	}

	temp, err := dcgm.BoardTemperature(alloc.DvInd)
	markErr("temperature", err)
	info.TemperatureC = temp

	util, err := dcgm.HCUUse(alloc.DvInd)
	markErr("utilization", err)
	info.UtilPercent = util

	if power, err := dcgm.Power(alloc.DvInd); err == nil {
		info.PowerUsageW = power
	}
	if total, err := dcgm.MemoryTotal(alloc.DvInd); err == nil {
		info.MemoryTotalMiB = total / (1024 * 1024)
	}
	if used, err := dcgm.MemoryUsed(alloc.DvInd); err == nil {
		info.MemoryUsedMiB = used / (1024 * 1024)
	}

	return info, firstErr
}

// evaluateHealth folds the sample result into the device verdict.
// A sampling failure keeps the previous verdict until the fail streak reaches
// monitorMaxFailStreak, so a transient dcgm error does not flap the device.
func evaluateHealth(info *DeviceHealthInfo, sampleErr error) DeviceHealthStatus {
	if sampleErr != nil {
		info.FailStreak++
		if info.FailStreak >= monitorMaxFailStreak {
			return DeviceUnhealthy
		}
		if info.Health == "" {
			return DeviceUnknown
		}
		return info.Health
	}

	info.FailStreak = 0
	if info.TemperatureC >= DeviceMonitorMaxTempC {
		return DeviceUnhealthy
	}
	return DeviceHealthy
}

// RefreshDeviceHealth samples all allocatable devices and updates health state.
// Returns non-nil when at least one device sampling failed; per-device failures
// only grow that device's fail streak without failing the whole round.
func (s *DeviceState) RefreshDeviceHealth() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refreshDeviceHealth()
}

// refreshDeviceHealth samples health for every allocatable device and prunes
// health entries for devices that are no longer enumerated.
// Caller must hold s.mu.
func (s *DeviceState) refreshDeviceHealth() error {
	var firstErr error
	changed := false

	for _, alloc := range s.allocatable {
		sampled, sampleErr := sampleDeviceMetrics(alloc)
		if sampleErr != nil {
			if firstErr == nil {
				firstErr = sampleErr
			}
			streak := 0
			if existing := s.health[alloc.Name]; existing != nil {
				streak = existing.FailStreak
			}
			klog.Warningf("health sampling failed device=%s dvInd=%d streak=%d: %v",
				alloc.Name, alloc.DvInd, streak+1, sampleErr)
		}

		info := s.health[alloc.Name]
		if info == nil {
			info = &DeviceHealthInfo{Health: DeviceUnknown}
		}

		// Refresh the metrics snapshot only on success so LastUpdated reflects
		// the last good reading; the verdict always runs against the latest info.
		if sampleErr == nil {
			info.DeviceName = sampled.DeviceName
			info.DvInd = sampled.DvInd
			info.PciBusNumber = sampled.PciBusNumber
			info.TemperatureC = sampled.TemperatureC
			info.PowerUsageW = sampled.PowerUsageW
			info.MemoryUsedMiB = sampled.MemoryUsedMiB
			info.MemoryTotalMiB = sampled.MemoryTotalMiB
			info.UtilPercent = sampled.UtilPercent
			info.LastUpdated = sampled.LastUpdated
		}

		prev := info.Health
		info.Health = evaluateHealth(info, sampleErr)
		if prev != info.Health {
			changed = true
			klog.Infof("device health changed: device=%s dvInd=%d %s -> %s (temp=%.1fC util=%d%% memUsed=%.0fMiB)",
				alloc.Name, alloc.DvInd, prev, info.Health, info.TemperatureC, info.UtilPercent, info.MemoryUsedMiB)
		}
		s.health[alloc.Name] = info
	}

	// Prune entries for devices that disappeared from the last enumeration so
	// the gRPC snapshot and publish filtering do not leak stale cards.
	for name := range s.health {
		if _, ok := s.allocatable[name]; !ok {
			delete(s.health, name)
			changed = true
		}
	}

	if changed {
		s.notifyHealthChangedLocked()
	}
	return firstErr
}

// notifyHealthChangedLocked hints listeners that at least one health verdict changed.
// Caller must hold s.mu.
func (s *DeviceState) notifyHealthChangedLocked() {
	select {
	case s.healthCh <- struct{}{}:
	default:
	}
}

// HealthSnapshot returns a copy of the current health map for external
// consumers (health gRPC stream, publish filtering).
func (s *DeviceState) HealthSnapshot() map[string]*DeviceHealthInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := make(map[string]*DeviceHealthInfo, len(s.health))
	for name, info := range s.health {
		cp := *info
		snapshot[name] = &cp
	}
	return snapshot
}

// IsDeviceUnhealthy reports whether a device must be excluded from publishing.
// Unknown devices stay published: absence of data must not pull a working card.
func (s *DeviceState) IsDeviceUnhealthy(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	info, ok := s.health[name]
	return ok && info.Health == DeviceUnhealthy
}

// HealthChanges returns the channel hinted on health transitions.
func (s *DeviceState) HealthChanges() <-chan struct{} {
	return s.healthCh
}
