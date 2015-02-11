package filter

import (
	"fmt"
	"testing"

	"github.com/docker/swarm/cluster"
	"github.com/samalba/dockerclient"
	"github.com/stretchr/testify/assert"
)

func makeBinding(ip, port string) map[string][]dockerclient.PortBinding {
	return map[string][]dockerclient.PortBinding{
		fmt.Sprintf("%s/tcp", port): {
			{
				HostIp:   ip,
				HostPort: port,
			},
		},
	}
}

func TestPortFilterNoConflicts(t *testing.T) {
	var (
		p     = PortFilter{}
		nodes = []*cluster.Node{
			cluster.NewNode("node-1", 0),
			cluster.NewNode("node-2", 0),
			cluster.NewNode("node-3", 0),
		}
		result []*cluster.Node
		err    error
	)

	// Request no ports.
	config := &dockerclient.ContainerConfig{
		HostConfig: dockerclient.HostConfig{
			PortBindings: map[string][]dockerclient.PortBinding{},
		},
	}
	// Make sure we don't filter anything out.
	result, err = p.Filter(config, nodes)
	assert.NoError(t, err)
	assert.Equal(t, result, nodes)

	// Request port 80.
	config = &dockerclient.ContainerConfig{
		HostConfig: dockerclient.HostConfig{
			PortBindings: makeBinding("", "80"),
		},
	}

	// Since there are no other containers in the cluster, this shouldn't
	// filter anything either.
	result, err = p.Filter(config, nodes)
	assert.NoError(t, err)
	assert.Equal(t, result, nodes)

	// Add a container taking a different (4242) port.
	container := &cluster.Container{Container: dockerclient.Container{Id: "c1"}, Info: dockerclient.ContainerInfo{HostConfig: &dockerclient.HostConfig{PortBindings: makeBinding("", "4242")}}}
	assert.NoError(t, nodes[0].AddContainer(container))

	// Since no node is using port 80, there should be no filter
	result, err = p.Filter(config, nodes)
	assert.NoError(t, err)
	assert.Equal(t, result, nodes)
}

func TestPortFilterSimple(t *testing.T) {
	var (
		p     = PortFilter{}
		nodes = []*cluster.Node{
			cluster.NewNode("node-1", 0),
			cluster.NewNode("node-2", 0),
			cluster.NewNode("node-3", 0),
		}
		result []*cluster.Node
		err    error
	)

	// Add a container taking away port 80 to nodes[0].
	container := &cluster.Container{Container: dockerclient.Container{Id: "c1"}, Info: dockerclient.ContainerInfo{HostConfig: &dockerclient.HostConfig{PortBindings: makeBinding("", "80")}}}
	assert.NoError(t, nodes[0].AddContainer(container))

	// Request port 80.
	config := &dockerclient.ContainerConfig{
		HostConfig: dockerclient.HostConfig{
			PortBindings: makeBinding("", "80"),
		},
	}

	// nodes[0] should be excluded since port 80 is taken away.
	result, err = p.Filter(config, nodes)
	assert.NoError(t, err)
	assert.NotContains(t, result, nodes[0])
}

func TestPortFilterDifferentInterfaces(t *testing.T) {
	var (
		p     = PortFilter{}
		nodes = []*cluster.Node{
			cluster.NewNode("node-1", 0),
			cluster.NewNode("node-2", 0),
			cluster.NewNode("node-3", 0),
		}
		result []*cluster.Node
		err    error
	)

	// Add a container taking away port 80 on every interface to nodes[0].
	container := &cluster.Container{Container: dockerclient.Container{Id: "c1"}, Info: dockerclient.ContainerInfo{HostConfig: &dockerclient.HostConfig{PortBindings: makeBinding("", "80")}}}
	assert.NoError(t, nodes[0].AddContainer(container))

	// Request port 80 for the local interface.
	config := &dockerclient.ContainerConfig{
		HostConfig: dockerclient.HostConfig{
			PortBindings: makeBinding("127.0.0.1", "80"),
		},
	}

	// nodes[0] should be excluded since port 80 is taken away for every
	// interface.
	result, err = p.Filter(config, nodes)
	assert.NoError(t, err)
	assert.NotContains(t, result, nodes[0])

	// Add a container taking away port 4242 on the local interface of
	// nodes[1].
	container = &cluster.Container{Container: dockerclient.Container{Id: "c1"}, Info: dockerclient.ContainerInfo{HostConfig: &dockerclient.HostConfig{PortBindings: makeBinding("127.0.0.1", "4242")}}}
	assert.NoError(t, nodes[1].AddContainer(container))

	// Request port 4242 on the same interface.
	config = &dockerclient.ContainerConfig{
		HostConfig: dockerclient.HostConfig{
			PortBindings: makeBinding("127.0.0.1", "4242"),
		},
	}
	// nodes[1] should be excluded since port 4242 is already taken on that
	// interface.
	result, err = p.Filter(config, nodes)
	assert.NoError(t, err)
	assert.NotContains(t, result, nodes[1])

	// Request port 4242 on every interface.
	config = &dockerclient.ContainerConfig{
		HostConfig: dockerclient.HostConfig{
			PortBindings: makeBinding("0.0.0.0", "4242"),
		},
	}
	// nodes[1] should still be excluded since the port is not available on the same interface.
	result, err = p.Filter(config, nodes)
	assert.NoError(t, err)
	assert.NotContains(t, result, nodes[1])

	// Request port 4242 on every interface using an alternative syntax.
	config = &dockerclient.ContainerConfig{
		HostConfig: dockerclient.HostConfig{
			PortBindings: makeBinding("", "4242"),
		},
	}
	// nodes[1] should still be excluded since the port is not available on the same interface.
	result, err = p.Filter(config, nodes)
	assert.NoError(t, err)
	assert.NotContains(t, result, nodes[1])

	// Finally, request port 4242 on a different interface.
	config = &dockerclient.ContainerConfig{
		HostConfig: dockerclient.HostConfig{
			PortBindings: makeBinding("192.168.1.1", "4242"),
		},
	}
	// nodes[1] should be included this time since the port is available on the
	// other interface.
	result, err = p.Filter(config, nodes)
	assert.NoError(t, err)
	assert.Contains(t, result, nodes[1])
}

func TestPortFilterRandomAssignment(t *testing.T) {
	var (
		p     = PortFilter{}
		nodes = []*cluster.Node{
			cluster.NewNode("node-1", 0),
			cluster.NewNode("node-2", 0),
			cluster.NewNode("node-3", 0),
		}
		result []*cluster.Node
		err    error
	)

	// Simulate a container that requested to map 80 to a random port.
	// In this case, HostConfig.PortBindings should contain a binding with no
	// HostPort defined and NetworkSettings.Ports should contain the actual
	// mapped port.
	container := &cluster.Container{
		Container: dockerclient.Container{Id: "c1"},
		Info: dockerclient.ContainerInfo{
			HostConfig: &dockerclient.HostConfig{
				PortBindings: map[string][]dockerclient.PortBinding{
					"80/tcp": {
						{
							HostIp:   "",
							HostPort: "",
						},
					},
				},
			},
			NetworkSettings: struct {
				IpAddress   string
				IpPrefixLen int
				Gateway     string
				Bridge      string
				Ports       map[string][]dockerclient.PortBinding
			}{
				Ports: map[string][]dockerclient.PortBinding{
					"80/tcp": {
						{
							HostIp:   "127.0.0.1",
							HostPort: "1234",
						},
					},
				},
			},
		},
	}

	assert.NoError(t, nodes[0].AddContainer(container))

	// Request port 80.
	config := &dockerclient.ContainerConfig{
		HostConfig: dockerclient.HostConfig{
			PortBindings: makeBinding("", "80"),
		},
	}

	// Since port "80" has been mapped to "1234", we should be able to request "80".
	result, err = p.Filter(config, nodes)
	assert.NoError(t, err)
	assert.Equal(t, result, nodes)

	// However, we should not be able to request "1234" since it has been used for a random assignment.
	config = &dockerclient.ContainerConfig{
		HostConfig: dockerclient.HostConfig{
			PortBindings: makeBinding("", "1234"),
		},
	}
	result, err = p.Filter(config, nodes)
	assert.NoError(t, err)
	assert.NotContains(t, result, nodes[0])
}