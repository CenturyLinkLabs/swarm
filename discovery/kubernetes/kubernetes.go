package kubernetes

import (
	"github.com/GoogleCloudPlatform/kubernetes/pkg/client"
	"github.com/docker/swarm/discovery"
)

func init() {
	discovery.Register("k8s", &KubernetesDiscoveryService{})
}

type KubernetesDiscoveryService struct {
	client *client.Client
}

func (s *KubernetesDiscoveryService) Initialize(uri string, heartbeat int) error {
	config := &client.Config{Host: "http://" + uri}
	client, err := client.New(config)
	if err != nil {
		return err
	}

	s.client = client
	return nil
}

func (s *KubernetesDiscoveryService) Fetch() ([]*discovery.Entry, error) {
	nl, err := s.client.Nodes().List()
	if err != nil {
		return nil, err
	}

	addrs := []string{}
	for _, n := range nl.Items {
		// TODO this might actually need to be Kubernetes minion addresses, and in
		// the scheduler NewEntries we convert to Docker endpoints. But it needs a
		// port so... ??
		addrs = append(addrs, n.Status.HostIP+":2375")
	}

	return discovery.CreateEntries(addrs)
}

func (s *KubernetesDiscoveryService) Watch(callback discovery.WatchCallback) {
	// We'll need to do this so that the Docker daemon on new minions can be
	// queried as they come up and down.
}

func (s *KubernetesDiscoveryService) Register(addr string) error {
	// Swarm isn't the one responsible for joining things to the Kubernetes
	// cluster. We can't add somebody to a discovery service. It's a read-only
	// thing as far as we know.
	return nil
}
