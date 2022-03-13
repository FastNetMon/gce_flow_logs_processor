# Introduction

This app works as bridge to tranform GCE VPC Flow Logs into FastNetMon's Tera flow format: https://fastnetmon.com/docs-fnm-advanced/fastnetmon-and-google-compute-gce-vpc-flow-logs/

# Build process

```go build```

# Run

```
./gce_flow_logs_processor -topic_id flow_logs_eu_west_2
```
