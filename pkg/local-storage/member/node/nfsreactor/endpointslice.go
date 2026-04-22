package nfsreactor

import (
	"context"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NFSPort is the NFSv4 TCP port; we skip rpcbind because NFSv4 does not need
// it.
const NFSPort int32 = 2049

// EndpointSliceClient writes/deletes the per-node EndpointSlice that fronts
// this pod's Ganesha for the tenant Service matching the backing PVC.
type EndpointSliceClient interface {
	// Put creates or updates "<serviceName>-<node>" in serviceNamespace,
	// pointing at podIP:2049. Returns nil if the Service doesn't exist
	// (the controller hasn't created it yet); the next resync will retry.
	Put(ctx context.Context, serviceName, serviceNamespace string) error
	// Delete removes the per-node EndpointSlice for (serviceName, ns).
	// Returns nil if it doesn't exist.
	Delete(ctx context.Context, serviceName, serviceNamespace string) error
}

type realSlices struct {
	client   client.Client
	nodeName string
	podIP    string
}

// NewEndpointSliceClient builds a real EndpointSliceClient.
func NewEndpointSliceClient(c client.Client, nodeName, podIP string) EndpointSliceClient {
	return &realSlices{client: c, nodeName: nodeName, podIP: podIP}
}

// sanitizeNodeName lowercases and replaces characters that aren't allowed in
// Kubernetes object names. EndpointSlice names must be RFC 1123 labels.
func sanitizeNodeName(n string) string {
	n = strings.ToLower(n)
	var b strings.Builder
	for _, r := range n {
		switch {
		case (r >= 'a' && r <= 'z'), (r >= '0' && r <= '9'), r == '-':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		out = "node"
	}
	return out
}

// sliceName returns the EndpointSlice name for this (service, node) pair.
func sliceName(serviceName, nodeName string) string {
	return fmt.Sprintf("%s-%s", serviceName, sanitizeNodeName(nodeName))
}

func (r *realSlices) Put(ctx context.Context, serviceName, serviceNamespace string) error {
	// Look up the Service first — if it doesn't exist yet, skip quietly
	// so we don't create orphan EndpointSlices the service controller
	// could later reap.
	svc := &corev1.Service{}
	err := r.client.Get(ctx, types.NamespacedName{Namespace: serviceNamespace, Name: serviceName}, svc)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.WithFields(log.Fields{
				"service":   serviceName,
				"namespace": serviceNamespace,
			}).Info("nfsreactor: backing Service not found yet, skipping EndpointSlice put")
			return nil
		}
		return fmt.Errorf("get service %s/%s: %w", serviceNamespace, serviceName, err)
	}

	name := sliceName(serviceName, r.nodeName)
	nodeName := r.nodeName
	ready := true
	portName := "nfs"
	port := NFSPort
	proto := corev1.ProtocolTCP

	desired := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: serviceNamespace,
			Labels: map[string]string{
				discoveryv1.LabelServiceName: serviceName,
				// endpointslice.kubernetes.io/managed-by must be a valid
				// label value (alphanumeric + '-_.'), no '/'.
				discoveryv1.LabelManagedBy: "hwameistor-nfs-reactor",
			},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses:  []string{r.podIP},
			Conditions: discoveryv1.EndpointConditions{Ready: &ready},
			NodeName:   &nodeName,
		}},
		Ports: []discoveryv1.EndpointPort{{
			Name:     &portName,
			Port:     &port,
			Protocol: &proto,
		}},
	}

	existing := &discoveryv1.EndpointSlice{}
	err = r.client.Get(ctx, types.NamespacedName{Namespace: serviceNamespace, Name: name}, existing)
	if apierrors.IsNotFound(err) {
		return r.client.Create(ctx, desired)
	}
	if err != nil {
		return fmt.Errorf("get endpointslice %s/%s: %w", serviceNamespace, name, err)
	}
	// Update in place — preserve ResourceVersion.
	existing.Labels = desired.Labels
	existing.AddressType = desired.AddressType
	existing.Endpoints = desired.Endpoints
	existing.Ports = desired.Ports
	return r.client.Update(ctx, existing)
}

func (r *realSlices) Delete(ctx context.Context, serviceName, serviceNamespace string) error {
	name := sliceName(serviceName, r.nodeName)
	es := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: serviceNamespace},
	}
	err := r.client.Delete(ctx, es)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete endpointslice %s/%s: %w", serviceNamespace, name, err)
	}
	return nil
}
