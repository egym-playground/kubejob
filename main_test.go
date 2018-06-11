package main

import "testing"

func TestLabelSelector(t *testing.T) {
	data := []struct{
		in map[string]string
		out string
	}{
		{
			map[string]string{},
			"",
		},
		{
			map[string]string{"foo": "bar"},
			"foo=bar",
		},
		{
			map[string]string{"foo": "bar", "fizz": "buzz"},
			"foo=bar,fizz=buzz",
		},
	}

	for _, d := range data {
		out := labelSelector(d.in)
		if out != d.out {
			t.Fatalf("%v: unexpected result: %s", d, out)
		}
	}
}
