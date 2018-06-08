package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"sync"
	"time"

	"github.com/egymgmbh/go-prefix-writer/prefixer"

	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var kubeconfig *string
	if home := os.Getenv("HOME"); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	namespace := flag.String("n", "", "kubernetes namespace to use")
	timeoutStr := flag.String("t", "", "(optional) timeout in time.Duration format (eg. 10s, 1m, 1h, ...)")
	// help := flag.Bool("h", false, "(optional) print usage information")
	flag.Parse()

	if len(*namespace) == 0 {
		fmt.Fprint(os.Stderr, "Purpose: Create a Kubernetes job and watch the logs until it completes.\n")
		fmt.Fprintf(os.Stderr, "\nUsage:\n\t%s -n Namespace [-kubeconfig ConfigFile] [-t Timeout] [JobFile]\n", os.Args[0])
		fmt.Fprintln(os.Stderr, "\nExamples:")
		fmt.Fprintf(os.Stderr, "\t# Read job spec from file:\n\t%s -n test job.yaml\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\t# Read job spec from stdin:\n\tcat job.yaml | %s -n test\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		return
	}

	var timeoutChan <-chan time.Time
	if len(*timeoutStr) > 0 {
		timeout, err := time.ParseDuration(*timeoutStr)
		if err != nil {
			log.Fatal("invalid timeout (-t): ", err)
		}
		timeoutChan = time.After(timeout)
	}

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

	// make sure we clean up properly
	defer func() {
		log.Print("deleting job")
		err = cs.BatchV1().Jobs(*namespace).Delete(job.Name, nil)
		if err != nil {
			log.Print("delete job: ", err)
		}
	}()

	// wait for signal or shutdown
	select {
	case <-timeoutChan:
		log.Print("timeout")
	case <-sigChan:
		log.Print("cancelled by signal")
	case result := <-resultChan:
		if result {
			log.Print("job completed successfully")
		} else {
			log.Fatal("job failed")
		}
	}
}

func watchJob(cs *kubernetes.Clientset, job *batch.Job, resultChan chan<- bool) {
	var lastPhase core.PodPhase

	listOpts := meta.ListOptions{
		LabelSelector: labelSelector(job.Spec.Selector.MatchLabels),
		Watch:         true,
	}
	watch, err := cs.CoreV1().Pods(job.Namespace).Watch(listOpts)
	if err != nil {
		log.Print("unable to watch: ", err)
	}
	defer watch.Stop()

	logsDone := make(chan interface{})
	var result bool

	for event := range watch.ResultChan() {
		pod := event.Object.(*core.Pod)

		// Only act on phase transitions
		if pod.Status.Phase == lastPhase {
			continue
		}

		switch pod.Status.Phase {
		case core.PodRunning:
			startLogStreaming(cs, pod, logsDone)
		case core.PodSucceeded:
			result = true
			goto end
		case core.PodFailed:
			result = false
			goto end
		}

		lastPhase = pod.Status.Phase
	}

end:

	// wait for logs but no more than 10s
	select {
	case <-logsDone:
	case <-time.After(10 * time.Second):
	}

	// send the result
	resultChan <- result
}

func startLogStreaming(cs *kubernetes.Clientset, pod *core.Pod, done chan<- interface{}) {
	var wg sync.WaitGroup
	if len(pod.Spec.Containers) == 1 {
		wg.Add(1)
		go streamLogsForContainer(cs, pod, "", &wg)
	} else {
		wg.Add(len(pod.Spec.Containers))
		for _, container := range pod.Spec.Containers {
			go streamLogsForContainer(cs, pod, container.Name, &wg)
		}
	}

	go func() {
		wg.Wait()
		done <- nil
	}()
}

func streamLogsForContainer(cs *kubernetes.Clientset, pod *core.Pod, container string, wg *sync.WaitGroup) {
	defer wg.Done()

	logOpts := core.PodLogOptions{
		Follow:    true,
		Container: container,
	}

	stream, err := cs.CoreV1().RESTClient().Get().
		Namespace(pod.Namespace).
		Resource("pods").
		Name(pod.Name).
		SubResource("log").
		VersionedParams(&logOpts, scheme.ParameterCodec).Stream()
	if err != nil {
		log.Print("Log stream request failed: ", err)
		return
	}
	defer stream.Close()

	// prefix output lines with container name if set
	var prefix string
	if len(container) > 0 {
		prefix = container + ": "
	}
	out := prefixer.New(os.Stdout, func() string {
		return prefix
	})

	_, err = io.Copy(out, stream)
	if err != nil {
		log.Print("Reading logs failed: ", err)
		return
	}
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
