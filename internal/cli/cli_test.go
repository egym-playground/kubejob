package cli

import (
	"testing"
	"reflect"
	"bytes"
	"strings"
)

func TestParse(t *testing.T) {
	data := []struct{
		home string
		args []string
		expectedRes Args
		expectedErr bool
		outContains string
	}{
		{
			"/foo/bar",
			[]string{"kubejob", "-n", "foo"},
			Args{
				Kubeconfig: "/foo/bar/.kube/config",
				Namespace: "foo",
				JobFile: "-",
			},
			false,
			"",
		},
		{
			"/foo/bar",
			[]string{"kubejob", "-n", "foo", "/fizz/buzz"},
			Args{
				Kubeconfig: "/foo/bar/.kube/config",
				Namespace: "foo",
				JobFile: "/fizz/buzz",
			},
			false,
			"",
		},
		{
			"/foo/bar",
			[]string{"kubejob", "--help"},
			Args{},
			true,
			"",
		},
		{
			"",
			[]string{"kubejob", "--version"},
			Args{},
			true,
			"42-23-73",
		},
	}

	for _, d := range data {
		var buf bytes.Buffer
		res, err := Parse(d.args, d.home, "42-23-73", &buf)
		if d.expectedErr {
			if err == nil {
				t.Fatalf("%v: missing expected error", d)
			}
		} else {
			if err != nil {
				t.Fatalf("%v: unexpected error: %v", d, err)
			}
		}
		if !d.expectedErr && !reflect.DeepEqual(res, &d.expectedRes) {
			t.Fatalf("%v: unexpected result: %v", d, res)
		}
		if d.outContains != "" {
			if !strings.Contains(buf.String(), d.outContains) {
				t.Log(buf.String())
				t.Fatal("output does not contain expected string: ", d.outContains)
			}
		}
	}
}
