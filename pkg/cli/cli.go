package cli

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"text/template"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"
)

const helpTemplate = `Purpose: Create a Kubernetes job and watch the logs until it completes.

Examples:
	# Read job spec from file:
	{{ .app }} -n test job.yaml

	# Read job spec from stdin
	cat job.yaml | {{ .app }} -n test

`

type Args struct {
	Kubeconfig string
	Namespace  string
	JobFile    string
	Timeout    time.Duration
}

func Parse(args []string, home, version string, out io.Writer) (*Args, error) {
	var help bytes.Buffer
	err := template.Must(template.New("help").Parse(helpTemplate)).Execute(&help, map[string]string{"app": args[0]})
	if err != nil {
		return nil, fmt.Errorf("unable to format help: %v", err)
	}

	app := kingpin.New(args[0], "Run Kubernetes jobs and wait for their completion.")
	app.Version(version)
	app.Help = help.String()

	jobFile := app.Arg("JOBFILE", "Job spec file, - for stdin (default)").Default("-").String()
	namespace := app.Flag("namespace", "Kubernetes namespace to use").Short('n').Required().String()
	timeout := app.Flag("timeout", "Timeout in time.Duration format (eg. 10s, 1m, 1h, ...)").Short('t').Duration()

	var kubeconfig *string
	if home != "" {
		kubeconfig = app.Flag("kubeconfig", "(optional) absolute path to the Kubeconfig file").Default(filepath.Join(home, ".kube", "config")).String()
	} else {
		kubeconfig = app.Flag("kubeconfig", "absolute path to the Kubeconfig file").Required().String()
	}

	// do not call os.Exit() on error
	app.Terminate(nil)

	// redirect output
	app.ErrorWriter(out)
	app.UsageWriter(out)

	_, err = app.Parse(args[1:])
	if err != nil {
		return nil, err
	}

	return &Args{
		Kubeconfig: *kubeconfig,
		Namespace:  *namespace,
		JobFile:    *jobFile,
		Timeout:    *timeout,
	}, nil
}
