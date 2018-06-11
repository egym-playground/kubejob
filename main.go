package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/egymgmbh/go-prefix-writer/prefixer"
	"github.com/egymgmbh/kubejob/internal/cli"

	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/tools/clientcmd"
)

var githash string // set by linker, see '.travis.yml'

func main() {
	args, err := cli.Parse(os.Args, os.Getenv("HOME"), githash, os.Stderr)
	if err != nil {
		log.Fatal("Error: ", err)
	}

	var timeoutChan <-chan time.Time
	if args.Timeout != time.Duration(0) {
		timeoutChan = time.After(args.Timeout)
	}

	jobIn, err := parseAndValidateJob(args.JobFile)
	if err != nil {
		log.Fatal("Unable to parse job: ", err)
	}

	cs, err := k8sClientSet(args.Kubeconfig)
	if err != nil {
		log.Fatal("Failed to create client: ", err)
	}

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// create the job
	job, err := cs.BatchV1().Jobs(args.Namespace).Create(jobIn)
	if err != nil {
		log.Fatal("Failed to create job: ", err)
	}

	resultChan := make(chan bool)
	go watchJob(cs, job, resultChan)

	result := false

	// wait for signal or shutdown
	select {
	case <-timeoutChan:
		log.Print("Timeout after ", args.Timeout)
	case <-sigChan:
		log.Print("Cancelled by signal")
	case result = <-resultChan:
		if result {
			log.Print("Job completed successfully")
		} else {
			log.Print("Job failed")
		}
	}

	log.Print("Deleting job")
	err = cs.BatchV1().Jobs(args.Namespace).Delete(job.Name, nil)
	if err != nil {
		log.Print("Deleting job: ", err)
	}

	if !result {
		os.Exit(1)
	}
}

// parseAndValidateJob reads the job spec from path and returns the result if possible. If path is "-" the job spec is
// read from os.Stdin. Warnings are logged if required values are not set.
func parseAndValidateJob(path string) (*batch.Job, error) {
	f := os.Stdin
	if path != "-" {
		var err error
		f, err = os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("unable to open job file: %v", err)
		}
	}

	var job batch.Job
	err := yaml.NewYAMLOrJSONDecoder(f, 1024).Decode(&job)
	if err != nil {
		return nil, fmt.Errorf("unable to parse job spec: %v", err)
	}

	if job.Spec.Template.Spec.RestartPolicy != core.RestartPolicyNever {
		log.Print(`Warning: ".spec.template.spec.restartPolicy" should be set to "Never" in order to avoid unintended restarts`)
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		log.Print(`Warning: ".spec.backoffLimit" should be set to "0" in order to avoid unintended restarts`)
	}

	return &job, nil
}

// watchJob waits until job is done and reports the result (sucess or failure) through resultChan. The function ensures
// that all log output of the job is reported on os.Stdout and waits up to 10s for the end of the logs after the job
// finished before reporting to resultChan.
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
	oncePerPod := make(map[string]bool)

	for event := range watch.ResultChan() {
		pod := event.Object.(*core.Pod)

		// Only act on phase transitions
		if pod.Status.Phase == lastPhase {
			continue
		}

		log.Print(pod.Name, ": ", pod.Status.Phase)

		ensureLogStreaming := func() {
			// Start streaming logs once per pod
			if oncePerPod[pod.Name] {
				return
			}
			oncePerPod[pod.Name] = true
			startLogStreaming(cs, pod, logsDone)
		}

		switch pod.Status.Phase {
		case core.PodRunning:
			ensureLogStreaming()
		case core.PodSucceeded:
			ensureLogStreaming()
			result = true
			goto end
		case core.PodFailed:
			ensureLogStreaming()
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
		log.Print("timeout waiting for logs")
	}

	// send the result
	resultChan <- result
}

// startLogStreaming starts streaming the logs for all containers in the pod. The function returns immediately and
// signals the end of all logs treams by closing the done channel.
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
		close(done)
	}()
}

// streamLogsForContainer reads all the logs from the specified container which must be part of the specified pod
// and writes them to os.Stdout using the container name as a prefix. container may be empty in case pod has only
// one container. wg.Done() is called when the end of the log stream is reached.
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

// labelSelector converts a label map (as used in the job spec) into a label query as used in the API.
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

// k8sClientSet creates the Kubernetes client set from the config.
func k8sClientSet(kubeconfig string) (*kubernetes.Clientset, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(config)
}
