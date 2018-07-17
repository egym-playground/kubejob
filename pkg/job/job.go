package job

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
	"context"

	"github.com/egymgmbh/go-prefix-writer/prefixer"

	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"errors"
)

type result struct {
	success bool
	err error
}

func RunJob(ctx context.Context, cs *kubernetes.Clientset, namespace string, jobIn *batch.Job) (bool, error) {
	job, err := cs.BatchV1().Jobs(namespace).Create(jobIn)
	if err != nil {
		log.Fatal("Failed to create job: ", err)
	}

	resultChan := make(chan result)
	go watchJob(ctx, cs, job, resultChan)

	// wait for signal or shutdown
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case result := <-resultChan:
		return result.success, result.err
	}
}

// WatchJob waits until job is done and reports the result (sucess or failure) through resultChan. The function ensures
// that all log output of the job is reported on os.Stdout and waits up to 10s for the end of the logs after the job
// finished before reporting to resultChan.
func watchJob(ctx context.Context, cs *kubernetes.Clientset, job *batch.Job, resultChan chan<- result) {
	var lastPhase core.PodPhase

	listOpts := meta.ListOptions{
		LabelSelector: labelSelector(job.Spec.Selector.MatchLabels),
		Watch:         true,
	}
	watch, err := cs.CoreV1().Pods(job.Namespace).Watch(listOpts)
	if err != nil {
		resultChan <- result{false, fmt.Errorf("unable to watch: %v", err)}
		return
	}
	defer watch.Stop()

	logsDone := make(chan interface{})
	var success bool
	oncePerPod := make(map[string]bool)

	for event := range watch.ResultChan() {
		if ctx.Err() != nil {
			break
		}

		pod := event.Object.(*core.Pod)

		// Only act on phase transitions
		if pod.Status.Phase == lastPhase {
			continue
		}

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
			success = true
			break
		case core.PodFailed:
			ensureLogStreaming()
			success = false
			break
		}

		lastPhase = pod.Status.Phase
	}

	err = nil

	// wait for end of logs but no more than 10s
	select {
	case <-logsDone:
	case <-time.After(10 * time.Second):
		err = errors.New("timeout waiting for end of logs")
	case <-ctx.Done():
		err = ctx.Err()
	}

	// send the result
	resultChan <- result{success, err}
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
