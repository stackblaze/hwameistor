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

// NFSPort is the NFSv4 TCP port. rpcbind is skipped — NFSv4 doesn't need it.
const NFSPort int32 = 2049

type EndpointSliceClient interface {
	Put(ctx context.Context, serviceName, serviceNamespace string) error
	Delete(ctx context.Context, serviceName, serviceNamespace string) error
}

type realSlices struct {
	client   client.Client
	nodeName string
	podIP    string
}

func NewEndpointSliceClient(c client.Client, nodeName, podIP string) EndpointSliceClient {
	return &realSlices{client: c, nodeName: nodeName, podIP: podIP}
}

// sanitizeNodeName: EndpointSlice names must be RFC 1123 labels.
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

func sliceName(serviceName, nodeName string) string {
	return fmt.Sprintf("%s-%s", serviceName, sanitizeNodeName(nodeName))
}

// Put creates or updates "<serviceName>-<node>" pointing at podIP:2049.
// If the Service doesn't exist yet (controller hasn't created it), we
// skip — the next resync will retry.
func (r *realSlices) Put(ctx context.Context, serviceName, serviceNamespace string) error {
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
				// managed-by label value can't contain '/'.
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
