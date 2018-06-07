package main

import (
	"flag"
	"path/filepath"
	"os"
	"log"
	"bytes"
	"fmt"
	"time"
	"os/signal"
	"syscall"

	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"

	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"

	"k8s.io/apimachinery/pkg/util/yaml"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"io"
	"k8s.io/client-go/kubernetes/scheme"
)

func main() {
	var kubeconfig *string
	if home := os.Getenv("HOME"); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	namespace := flag.String("n", "", "k8s namespace to use")
	flag.Parse()

	var jobFile *os.File
	if len(flag.Args()) > 0 {
		var err error
		jobFile, err = os.Open(flag.Arg(0))
		if err != nil {
			log.Fatal("unable to open job file: ", err)
		}
	} else {
		jobFile = os.Stdin
	}

	var jobIn batch.Job
	err := yaml.NewYAMLOrJSONDecoder(jobFile, 1024).Decode(&jobIn)
	if err != nil {
		log.Fatal("unable to parse job spec: ", err)
	}

	cs, err := k8sClientSet(*kubeconfig)
	if err != nil {
		log.Fatal("failed to create client: ", err)
	}

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// create the job
	job, err := cs.BatchV1().Jobs(*namespace).Create(&jobIn)
	if err != nil {
		log.Fatal("failed to create job: ", err)
	}

	resultChan := make(chan bool)
	go watchJob(cs, job, resultChan)

	// wait for signal or shutdown
	select {
	case <-sigChan:
	case result := <-resultChan:
		if result {
			log.Print("job completed successfully")
		} else {
			log.Fatal("job failed")
		}
	}

	log.Print("deleting job")
	err = cs.BatchV1().Jobs(*namespace).Delete(job.Name, nil)
	if err != nil {
		log.Print("delete job: ", err)
	}
}

func watchJob(cs *kubernetes.Clientset, job *batch.Job, resultChan chan<- bool) {
	var lastPhase core.PodPhase

	for {
		time.Sleep(100 * time.Millisecond)

		podList, err := getPods(cs, job)
		if err != nil {
			log.Print("list pods failed: ", err)
			continue
		}
		if len(podList.Items) == 0 {
			continue
		}
		if len(podList.Items) > 1 {
			log.Print("unexpected number of pods: ", len(podList.Items))
			continue
		}

		pod := podList.Items[0]

		// Only act on phase transitions
		if pod.Status.Phase == lastPhase {
			continue
		}

		switch pod.Status.Phase {
		case core.PodRunning:
			go func() {
				err := streamLogs(cs, job)
				if err != nil {
					log.Print("streamLogs: ", err)
				}
			}()
		case core.PodSucceeded:
			resultChan <-true
			return
		case core.PodFailed:
			resultChan <-false
			return
		}

		lastPhase = pod.Status.Phase
	}
}

func streamLogs(cs *kubernetes.Clientset, job *batch.Job) error {
	pods, err := getPods(cs, job)
	if err != nil {
		return err
	}
	pod := pods.Items[0]

	logOpts := core.PodLogOptions{
		Follow: true,
	}

	req := cs.CoreV1().RESTClient().Get().Namespace(pod.Namespace).Resource("pods").Name(pod.Name).SubResource("log").VersionedParams(&logOpts, scheme.ParameterCodec)
	stream, err := req.Stream()
	if err != nil {
		return err
	}
	defer stream.Close()

	_, err = io.Copy(os.Stdout, stream)
	if err != nil {
		return err
	}

	return nil
}

func getPods(cs *kubernetes.Clientset, job *batch.Job) (*core.PodList, error) {
	ns := job.ObjectMeta.Namespace
	return cs.CoreV1().Pods(ns).List(meta.ListOptions{LabelSelector: labelSelector(job.Spec.Selector.MatchLabels)})
}

func labelSelector(labels map[string]string) string {
	var buf bytes.Buffer
	for k, v := range labels {
		fmt.Fprintf(&buf, "%s=%s,", k, v)
	}
	if buf.Len() > 0 {
		buf.Truncate(buf.Len() - 1)
	}
	return buf.String()
}

func k8sClientSet(kubeconfig string) (*kubernetes.Clientset, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(config)
}
