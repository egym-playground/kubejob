# kubejob
Run Kubernetes jobs and wait for their completion.

## Motivation
Sometimes you just want to run a job in a Kubernetes cluster and wait for its completion while
keeping an eye on the container output. This usecase frequently appears in deployment pipelines
eg. for running a smoke test or a database migration before the actual deployment continues. 

## How to install
```bash
go get -v -u github.com/egymgmbh/kubejob
```

## How to use
Assuming that your `$GOPATH/bin` is part of your `$PATH`, you can call it directly, eg.:
```bash
kubejob -n test demo/two-containers.job.yaml
```

### Example output
```
2018/06/08 16:18:33 demo-7zbq9: Pending
2018/06/08 16:18:36 demo-7zbq9: Running
container1: 1
container1: 2
container2: 11
container2: 12
container1: 3
container2: 13
container1: 4
container2: 14
container1: 5
container2: 15
container1: 6
container2: 16
container1: 7
container2: 17
container1: 8
container2: 18
container1: 9
container2: 19
container1: 10
container2: 20
2018/06/08 16:18:46 demo-7zbq9: Succeeded
2018/06/08 16:18:50 Job completed successfully
2018/06/08 16:18:50 Deleting job
```
