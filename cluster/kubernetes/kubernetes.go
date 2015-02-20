package kubernetes

import (
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/kubernetes/pkg/api"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/client"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/kubectl"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/labels"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/watch"
	log "github.com/Sirupsen/logrus"
	"github.com/docker/swarm/cluster"
	"github.com/docker/swarm/discovery"
	"github.com/samalba/dockerclient"
)

var ErrNotImplemented = errors.New("not implemented in the kubernetes cluster")

type KubernetesCluster struct {
	sync.Mutex

	masters []*client.Client
	//TODO: list of offers
	nodes   *cluster.Nodes
	options *cluster.Options
}

func NewCluster(options *cluster.Options) cluster.Cluster {
	log.WithFields(log.Fields{"name": "kubernetes"}).Debug("Initializing cluster")
	return &KubernetesCluster{
		nodes:   cluster.NewNodes(),
		options: options,
	}
}

// TODO in Kubernetes land, a single CreateContainer can result in multiple
// containers being scheduled! Even with a simple ReplicationController, you
// get the thing you wanted and a pause container.
// TODO filters and strategies completely ignored
func (c *KubernetesCluster) CreateContainer(config *dockerclient.ContainerConfig, name string) (*cluster.Container, error) {
	c.Lock()
	defer c.Unlock()

	if len(c.masters) == 0 {
		return nil, fmt.Errorf("no Kubernetes master was found")
	}

	m := c.masters[0]

	// TODO look at not using the kubectl namespace for this stuff and sticking
	// to client. Or not, this is a spike and building those objects appears
	// pretty rough.
	generator := kubectl.BasicReplicationController{}
	rcToCreate, err := generator.Generate(map[string]string{"image": config.Image, "replicas": "1", "name": "swarmtest"})
	if err != nil {
		return nil, err
	}

	rc, err := m.ReplicationControllers("default").Create(rcToCreate.(*api.ReplicationController))
	if err != nil {
		return nil, err
	}

	w, err := m.Events("default").Watch(labels.Everything(), labels.Everything(), rc.ObjectMeta.ResourceVersion)

	eventPods := make(chan *api.Pod)
	quitEventPolling := make(chan bool)
	go func() {
		for {
			select {
			case e, ok := <-w.ResultChan():
				if !ok {
					return
				}
				if e.Type == watch.Error {
					ee, ok := e.Object.(*api.Status)
					log.Error(ee)
					if !ok {
						return
					}
				} else if e.Type == watch.Added {
					o, ok := e.Object.(*api.Event)
					if !ok {
						return
					}

					podName := o.InvolvedObject.Name
					pod, err := m.Pods("default").Get(podName)
					if err != nil {
						log.Fatal(err)
					}

					eventPods <- pod
				}
			case <-quitEventPolling:
				w.Stop()
				return
			}
		}
	}()

	pod := <-eventPods
	quitEventPolling <- true
	host := fmt.Sprintf("tcp://%s:2375", pod.Status.HostIP)

	for pod.Status.Phase != api.PodRunning {
		time.Sleep(time.Second * 2)

		pod, err = m.Pods("default").Get(pod.Name)
		if err != nil {
			panic(fmt.Sprintf("ERROR: %s", err))
		}

		log.Debugf("Pod phase: %v", pod.Status.Phase)
	}

	for name, cs := range pod.Status.Info {
		if name == "swarmtest" && cs.Image == "redis" {
			matches := regexp.MustCompile("^docker://(.+)$").FindStringSubmatch(cs.ContainerID)
			if len(matches) != 2 {
				log.WithFields(log.Fields{
					"containerWithProtocol": cs.ContainerID,
				}).Fatal("Couldn't determine container ID")
			}
			containerID := matches[1]
			docker, err := dockerclient.NewDockerClient(host, nil)
			if err != nil {
				return nil, err
			}
			containers, err := docker.ListContainers(true, false, "")
			if err != nil {
				return nil, err
			}
			for _, dc := range containers {
				if dc.Id == containerID {
					containerInfo, err := docker.InspectContainer(containerID)
					if err != nil {
						return nil, err
					}
					return &cluster.Container{
						Container: dc,
						Info:      *containerInfo,
						Node:      c.nodes.Get(pod.Status.HostIP),
					}, nil
				}
			}

			log.WithFields(log.Fields{
				"host":        host,
				"containerID": containerID,
			}).Fatal("Couldn't find the container on the host")
		}
	}

	////TODO: Store container in store ??

	return nil, errors.New("Couldn't find the expected container in the pod!")
}

func (c *KubernetesCluster) RemoveContainer(container *cluster.Container, force bool) error {
	c.Lock()
	defer c.Unlock()

	//TODO: KillTask

	//TODO: remove container from store ??

	return ErrNotImplemented
}

//TODO: We are iterating over Kubernetes masters here as described in the Mesos
//driver, but it feels weird. We aren't going to gracefully handle multiple
//Kubernetes clusters. It feels like a Kubernetes discovery thing could point
//us to the nodes, and that any cluster *could* have a master, or might not,
//depending on the architecture?
//
//Should we at least implement a "node" discovery agent to go with nodes, for
//cases like this where we really only want one?
func (c *KubernetesCluster) NewEntries(entries []*discovery.Entry) {
	for _, entry := range entries {
		config := &client.Config{Host: "http://" + entry.String()}
		client, err := client.New(config)
		if err != nil {
			//TODO handle this error!
			panic("error creating Kubernetes client")
		}
		c.masters = append(c.masters, client)

		knl, err := client.Nodes().List()
		if err != nil {
			//TODO handle this error!
			panic("error fetching Kubernetes node list")
		}

		for _, kn := range knl.Items {
			n := cluster.NewNode(kn.Status.HostIP+":2375", c.options.OvercommitRatio)
			if err := n.Connect(c.options.TLSConfig); err != nil {
				//TODO handle this error!
				panic("error connecting to Kubernetes node")
			}
			c.nodes.Add(n)
		}
	}
}

func (c *KubernetesCluster) Events(eventsHandler cluster.EventHandler) {
	c.nodes.Events(eventsHandler)
}

func (c *KubernetesCluster) Nodes() []*cluster.Node {
	return c.nodes.List()
}

func (c *KubernetesCluster) Containers() []*cluster.Container {
	return c.nodes.Containers()
}

func (c *KubernetesCluster) Container(IdOrName string) *cluster.Container {
	return c.nodes.Container(IdOrName)
}
