// Copyright (c) 2026 Hygon Information Technology Co., Ltd.
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"sync"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// DeviceStateOptions configures the DRA driver runtime.
type DeviceStateOptions struct {
	NodeName   string
	DriverName string
	Namespace  string
	CDIRoot    string
	Clientset  kubernetes.Interface
}

// DeviceState holds node-local state: allocatable devices, device health and
// CDI base spec settings.
type DeviceState struct {
	allocatable map[string]*AllocatableDevice

	// health tracks the latest monitor snapshot per allocatable device name.
	// It is transient (not persisted): rebuilt by the first sampling round
	// after a restart.
	health map[string]*DeviceHealthInfo

	// healthCh is buffered(1) and receives a hint on any health transition;
	// the DRA health gRPC stream selects on it.
	healthCh chan struct{}

	cdiRoot   string
	cdiVendor string
	nodeName  string

	clientset kubernetes.Interface

	// stateFile is the path to the JSON file used to persist prepared state
	// across process restarts. It lives under cdiRoot.
	stateFile string

	mu       sync.Mutex
	prepared map[types.UID][]PreparedDevice
}

type PreparedDevice struct {
	ClaimUID     types.UID `json:"claimUID"`
	ParentDevice string    `json:"parentDevice"`
	RequestName  string    `json:"requestName"`
	PoolName     string    `json:"poolName"`

	// WholeCard is true when the device is a physical whole-card pass-through
	// (no vHCU created). When false, VDvInd holds the virtual device index.
	WholeCard bool `json:"wholeCard"`

	// vHCU fields — only valid when WholeCard is false.
	VDvInd       int    `json:"vdvInd"`
	PciBusNumber string `json:"pciBusNumber"`

	CDIName     string `json:"cdiName"`
	CDIDeviceID string `json:"cdiDeviceID"`

	// HAMI-style dynamic mark under /etc/vdev/dynamic (hcu-exporter reads this directory).
	DynamicMarkFile string `json:"dynamicMarkFile,omitempty"`
	PodUID          string `json:"podUID,omitempty"`
	ContainerName   string `json:"containerName,omitempty"`
	DvInd           int    `json:"dvInd"`
}
