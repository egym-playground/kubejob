package job

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
)

// Event is some event during job execution.
// It can be either of type error, core.PodStatus or LogLine.
type Event interface{}

// LogLine represents one line of log output from a container.
type LogLine struct {
	Container string // container name
	Line      string // one line of log output
}

type result struct {
	success bool
	err     error
}

// RunJob executes the specified jobIn and waits for its completion.
// An optional events channel can be provided to receive pod status updates, log messages and errors. The provided channel
// must be read or the operation won't make progress.
func RunJob(ctx context.Context, cs *kubernetes.Clientset, namespace string, jobIn *batch.Job, events chan<- Event) (bool, error) {
	job, err := cs.BatchV1().Jobs(namespace).Create(jobIn)
	if err != nil {
		return false, fmt.Errorf("failed to create job: %v", err)
	}

	resultChan := make(chan result)
	go watchJob(ctx, cs, job, resultChan, events)

	// wait for context or job
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
// An optional statusChan can be provided to receive pod status updates.
func watchJob(ctx context.Context, cs *kubernetes.Clientset, job *batch.Job, resultChan chan<- result, events chan<- Event) {
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

	// Start streaming logs once per pod. This is a no-op if was streaming already started.
	ensureLogStreaming := func(pod *core.Pod) {
		// no need to stream events if nobody's listening
		if events == nil {
			return
		}
		if oncePerPod[pod.Name] {
			return
		}
		oncePerPod[pod.Name] = true
		startLogStreaming(cs, pod, logsDone, events)
	}

	for event := range watch.ResultChan() {
		pod := event.Object.(*core.Pod)
		if events != nil {
			events <- pod.Status
		}

		// Quit if context is done
		if ctx.Err() != nil {
			goto end
		}

		// Only act on phase transitions
		if pod.Status.Phase == lastPhase {
			continue
		}

		switch pod.Status.Phase {
		case core.PodRunning:
			ensureLogStreaming(pod)
		case core.PodSucceeded:
			ensureLogStreaming(pod)
			success = true
			goto end
		case core.PodFailed:
			ensureLogStreaming(pod)
			success = false
			goto end
		}

		lastPhase = pod.Status.Phase
	}

end:
	err = nil
	// wait for end of logs but no more than 10s
	select {
	case <-logsDone:
	case <-time.After(10 * time.Second):
		err = errors.New("timeout waiting for end of logs")
	case <-ctx.Done():
		err = ctx.Err()
	}

	// close events channel to signal that we're done here
	if events != nil {
		close(events)
	}

	// send the result
	resultChan <- result{success, err}
}

// startLogStreaming starts streaming the logs for all containers in the pod. The function returns immediately and
// signals the end of all logs streams by closing the done channel.
func startLogStreaming(cs *kubernetes.Clientset, pod *core.Pod, done chan<- interface{}, events chan<- Event) {
	var wg sync.WaitGroup
	wg.Add(len(pod.Spec.Containers))
	for _, container := range pod.Spec.Containers {
		go streamLogsForContainer(cs, pod, container.Name, &wg, events)
	}

	go func() {
		wg.Wait()
		close(done)
	}()
}

// streamLogsForContainer reads all the logs from the specified container which must be part of the specified pod
// and writes them to os.Stdout using the container name as a prefix. container may be empty in case pod has only
// one container. wg.Done() is called when the end of the log stream is reached.
func streamLogsForContainer(cs *kubernetes.Clientset, pod *core.Pod, container string, wg *sync.WaitGroup, events chan<- Event) {
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
		events <- fmt.Errorf("streamLogsForContainer: %v", err)
		return
	}
	defer stream.Close()

	reader := bufio.NewReader(stream)
	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			events <- fmt.Errorf("streamLogsForContainer: %v", err)
			return
		}
		events <- LogLine{container, line}
		if err == io.EOF {
			return
		}
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
