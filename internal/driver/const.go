// Copyright (c) 2026 Hygon Information Technology Co., Ltd.
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"fmt"
	"os"
	"strconv"
)

// DriverName is the DRA driver name registered with kubelet and used in ResourceSlice.
const DriverName = "dra.hygon.com"

const (
	deviceSplitCountEnv     = "DEVICE_SPLIT_COUNT"
	defaultDeviceSplitCount = 4
)

const (
	deviceMonitorIntervalSecEnv     = "DEVICE_MONITOR_INTERVAL_SECONDS"
	defaultDeviceMonitorIntervalSec = 30
	deviceMonitorMaxTempCEnv        = "DEVICE_MONITOR_MAX_TEMP_C"
	defaultDeviceMonitorMaxTempC    = 105.0
)

// DeviceSplitCount is the max number of vHCUs one physical HCU can be split into.
// Loaded from DEVICE_SPLIT_COUNT at startup; defaults to 4.
var DeviceSplitCount int64 = defaultDeviceSplitCount

// DeviceMonitorIntervalSec is the device health sampling interval in seconds.
// Loaded from DEVICE_MONITOR_INTERVAL_SECONDS at startup; defaults to 30.
var DeviceMonitorIntervalSec int64 = defaultDeviceMonitorIntervalSec

// DeviceMonitorMaxTempC is the board temperature (Celsius) at or above which a
// device is marked Unhealthy. Loaded from DEVICE_MONITOR_MAX_TEMP_C at startup;
// defaults to 105.
var DeviceMonitorMaxTempC = defaultDeviceMonitorMaxTempC

// InitDeviceSplitCountFromEnv loads DEVICE_SPLIT_COUNT into DeviceSplitCount.
// Empty value keeps the default; non-positive or non-integer values return an error.
func InitDeviceSplitCountFromEnv() error {
	s := os.Getenv(deviceSplitCountEnv)
	if s == "" {
		DeviceSplitCount = defaultDeviceSplitCount
		return nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("%s must be a positive integer: %w", deviceSplitCountEnv, err)
	}
	if n <= 0 {
		return fmt.Errorf("%s must be a positive integer, got %d", deviceSplitCountEnv, n)
	}
	DeviceSplitCount = n
	return nil
}

// InitDeviceMonitorFromEnv loads DEVICE_MONITOR_INTERVAL_SECONDS and
// DEVICE_MONITOR_MAX_TEMP_C. Empty values keep the defaults; non-positive or
// non-numeric values return an error.
func InitDeviceMonitorFromEnv() error {
	if s := os.Getenv(deviceMonitorIntervalSecEnv); s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("%s must be a positive integer: %w", deviceMonitorIntervalSecEnv, err)
		}
		if n <= 0 {
			return fmt.Errorf("%s must be a positive integer, got %d", deviceMonitorIntervalSecEnv, n)
		}
		DeviceMonitorIntervalSec = n
	}

	if s := os.Getenv(deviceMonitorMaxTempCEnv); s != "" {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return fmt.Errorf("%s must be a positive number: %w", deviceMonitorMaxTempCEnv, err)
		}
		if f <= 0 {
			return fmt.Errorf("%s must be a positive number, got %f", deviceMonitorMaxTempCEnv, f)
		}
		DeviceMonitorMaxTempC = f
	}
	return nil
}
