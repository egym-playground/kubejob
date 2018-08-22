package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp" // enable GCP specific authentication
	"k8s.io/client-go/tools/clientcmd"

	"github.com/egymgmbh/kubejob/pkg/cli"
	"github.com/egymgmbh/kubejob/pkg/job"
)

var githash string // set by linker, see '.travis.yml'

func main() {
	args, err := cli.Parse(os.Args, os.Getenv("HOME"), githash, os.Stderr)
	if err != nil {
		log.Fatal("Error: ", err)
	}

	ctx := context.Background()
	var cancel func()
	if args.Timeout != time.Duration(0) {
		ctx, cancel = context.WithTimeout(ctx, args.Timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	go func() {
		sigChan := make(chan os.Signal)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		// wait for signal
		<-sigChan

		// cancel context
		cancel()
	}()

	jobSpec, err := parseAndValidateJob(args.JobFile)
	if err != nil {
		log.Fatal("Unable to parse job: ", err)
	}

	cs, err := k8sClientSet(args.Kubeconfig)
	if err != nil {
		log.Fatal("Failed to create client: ", err)
	}

	events := make(chan job.Event)
	go func() {
		var lastPhase core.PodPhase
		for event := range events {
			switch event := event.(type) {
			case error:
				log.Printf("Error: %v", event)
			case job.LogLine:
				log.Printf("%s: %s", event.Container, event.Line)
			case core.PodStatus:
				status := event
				if status.Phase != lastPhase {
					log.Print("Phase: ", status.Phase)
				}
				lastPhase = status.Phase

				if status.Phase == core.PodPending {
					for _, cs := range status.ContainerStatuses {
						if cs.State.Waiting != nil {
							log.Printf("Container %s is waiting: %s", cs.Name, cs.State.Waiting.Reason)
						}
					}
				}
			}
		}
	}()

	success, err := job.RunJob(ctx, cs, args.Namespace, jobSpec, events)
	if success {
		log.Print("Job completed successfully")
	} else {
		log.Print("Job failed")
	}
	if err != nil {
		log.Print("Error: ", err)
	}

	log.Print("Deleting job")
	err = cs.BatchV1().Jobs(args.Namespace).Delete(jobSpec.Name, nil)
	if err != nil {
		log.Print("Deleting job: ", err)
	}

	if !success {
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

// k8sClientSet creates the Kubernetes client set from the config.
func k8sClientSet(kubeconfig string) (*kubernetes.Clientset, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(config)
}
