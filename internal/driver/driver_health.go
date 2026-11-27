// Copyright (c) 2026 Hygon Information Technology Co., Ltd.
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"errors"
	"io"

	"google.golang.org/grpc"
	"k8s.io/klog/v2"
	drahealthv1alpha1 "k8s.io/kubelet/pkg/apis/dra-health/v1alpha1"
)

var _ drahealthv1alpha1.DRAResourceHealthServer = (*Driver)(nil)

// healthStatusToProto maps the internal tri-state verdict to the kubelet enum.
func healthStatusToProto(status DeviceHealthStatus) drahealthv1alpha1.HealthStatus {
	switch status {
	case DeviceHealthy:
		return drahealthv1alpha1.HealthStatus_HEALTHY
	case DeviceUnhealthy:
		return drahealthv1alpha1.HealthStatus_UNHEALTHY
	default:
		return drahealthv1alpha1.HealthStatus_UNKNOWN
	}
}

// healthSnapshotProto converts the current health map to the wire format.
func (d *Driver) healthSnapshotProto() *drahealthv1alpha1.NodeWatchResourcesResponse {
	snapshot := d.state.HealthSnapshot()
	resp := &drahealthv1alpha1.NodeWatchResourcesResponse{
		Devices: make([]*drahealthv1alpha1.DeviceHealth, 0, len(snapshot)),
	}
	for name, info := range snapshot {
		resp.Devices = append(resp.Devices, &drahealthv1alpha1.DeviceHealth{
			Device: &drahealthv1alpha1.DeviceIdentifier{
				PoolName:   d.state.nodeName,
				DeviceName: name,
			},
			Health:          healthStatusToProto(info.Health),
			LastUpdatedTime: info.LastUpdated.Unix(),
		})
	}
	return resp
}

// NodeWatchResources streams device health updates to kubelet (DRAResourceHealth
// alpha service). kubeletplugin detects this interface and registers the service
// automatically. It sends a full snapshot first, then re-sends it on every
// health transition — the device count per node is small, so a full rebuild is
// simpler and as reliable as PublishResources' full ResourceSlice rebuild.
func (d *Driver) NodeWatchResources(_ *drahealthv1alpha1.NodeWatchResourcesRequest,
	stream grpc.ServerStreamingServer[drahealthv1alpha1.NodeWatchResourcesResponse]) error {

	klog.V(4).Infof("NodeWatchResources: health stream started")

	send := func() error {
		resp := d.healthSnapshotProto()
		if err := stream.Send(resp); err != nil {
			if errors.Is(err, io.EOF) {
				return io.EOF
			}
			return err
		}
		klog.V(4).Infof("NodeWatchResources: sent %d device health entries", len(resp.Devices))
		return nil
	}

	if err := send(); err != nil {
		klog.Warningf("NodeWatchResources: send initial snapshot: %v", err)
		return nil
	}

	for {
		select {
		case <-stream.Context().Done():
			klog.V(4).Infof("NodeWatchResources: health stream closed")
			return nil
		case <-d.state.HealthChanges():
			if err := send(); err != nil {
				if !errors.Is(err, io.EOF) {
					klog.Warningf("NodeWatchResources: send health update: %v", err)
				}
				return nil
			}
		}
	}
}
