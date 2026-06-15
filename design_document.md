# Level 5 Job Worker Design

## Summary

Build a small Go worker that runs Linux processes. It has three parts:

- worker library
- gRPC API server
- CLI client

Target is Level 5. That means:

- mTLS
- simple authz
- output streaming
- process-tree cleanup
- per-job CPU, memory, and disk I/O limits with cgroup v2

This is a prototype. Keepin' it small. No scheduler. No database. No containers. No full platform.

## Goals

- Start a job.
- Stop a job.
- Get job status.
- Stream combined process output from the start of a job.
- Support many output clients for one job.
- Process output may be text or arbitrary binary data. Treat all output as raw bytes and preserve it byte-for-byte; never require, assume, decode, or transform it as text.
- Use gRPC over mTLS.
- Authorize clients with simple roles and job ownership.
- Kill child processes when stopping a job.
- Put each job in a cgroup and set CPU, memory, and disk I/O limits.

## Non-goals

No HA. No database. No persistence after server restart. No scheduler. No shell command strings. No stdin. No PTY. No containers. No cgroup v1. No full cert lifecycle. No full systemd installer.

Jobs run as the worker service user. Cgroups limit resources. They are not a sandbox.

## Assumptions

The worker runs on 64-bit Linux with cgroup v2.

The worker gets a writable delegated cgroup subtree at `/sys/fs/cgroup/jobworker`. The worker manages job cgroups inside that root. It does not mount cgroup filesystems, edit systemd config, or do root-level host setup.

The server owns the job working directory. Clients do not choose `cwd`.

Demo CA/server/client certs are checked into the repo for local dev only. They are not production secrets.

## Architecture

```text
jobctl CLI
  -> gRPC over mTLS
    -> jobworker server
      -> worker library
        -> Linux process + output spool + cgroup
```

Main packages:

```text
cmd/jobworker       server
cmd/jobctl          CLI
api/workerpb        protobuf/gRPC
pkg/worker          job lifecycle
pkg/cgroup          cgroup v2
pkg/output          output spool/streaming
internal/server     gRPC handlers
internal/authz      authz policy
```

The server is thin. The worker library owns process state, output state, and cgroup state.

## Job model

Each job has:

```text
id (opaque UUID string)
owner identity
executable path
args
server-owned working directory
limits
state
pid
cgroup path
exit result
output spool path
output error state
```

States:

```text
STARTING
RUNNING
STOPPING
EXITED
KILLED
FAILED
```

Job metadata is in memory. Completed jobs remain queryable until server shutdown.

## Worker API

```go
type Worker interface {
    Start(ctx context.Context, spec StartSpec) (*Job, error)
    Stop(ctx context.Context, id JobID, opts StopOptions) (*Job, error)
    Get(ctx context.Context, id JobID) (*Job, error)
    StreamOutput(ctx context.Context, id JobID, opts StreamOptions) (<-chan OutputChunk, error)
}
```

Use locks for shared state. Do not hold locks while doing process waits, file I/O, or gRPC sends.

## Start lifecycle

Start flow:

1. Validate request.
2. Require absolute executable path.
3. Reject shell strings and `PATH` lookup.
4. Validate resource limits.
5. Create an opaque UUID job ID.
6. Create output spool files.
7. Create job cgroup.
8. Write cgroup limits.
9. Open the job cgroup directory.
10. Start process directly inside the job cgroup.
11. Start output drain goroutine.
12. Start wait goroutine.
13. Mark job `RUNNING`.
14. Return job ID.

Primary implementation uses Linux `syscall.SysProcAttr`:

```go
SysProcAttr{
    UseCgroupFD:  true,
    CgroupFD:     jobCgroupFD,
}
```

This places the child in the job cgroup at process start. That avoids the fork race from starting a process first and moving its PID to `cgroup.procs` later.

The process working directory is fixed by the server.

If direct cgroup placement is not available, `Start` fails with a clear error. I will not implement PID-after-start migration as the normal path because a process can fork before migration.

### Start failure rollback

`Start` returns a job ID only after setup succeeds.

If setup fails after side effects, rollback runs in reverse order:

1. Use `cgroup.kill` if the job cgroup has live processes.
2. Close pipes and spool files.
3. Delete partial output files.
4. Remove the job cgroup.
5. Remove the job from memory.
6. Return the original error, with cleanup errors attached or logged.

This prevents orphan processes, leaked cgroups, and leaked output files on failed starts.

## Wait lifecycle

Each started process has one wait goroutine.

The wait goroutine:

1. Waits for the process to exit.
2. Records exit code or signal.
3. Marks state `EXITED`, `KILLED`, or `FAILED`.
4. Waits for the output drain goroutine to finish.
5. Closes output writers.
6. Records output drain errors, if any.
7. Sends EOF to stream clients.
8. Wakes all waiting stream clients.
9. Removes the job cgroup.
10. Cleans output resources.

Output bytes stay available while the job record is retained. Cleanup means closing handles, stopping goroutines, and deleting spool files when the job record is removed or the server stops.

## Stop lifecycle

Stop flow:

1. Check job exists.
2. Check caller is allowed.
3. If terminal, return current status.
4. Mark `STOPPING`.
5. Write `1` to `cgroup.kill`.
6. Let the wait goroutine record final state and clean up.

`cgroup.kill` is the cleanup path for job processes. Job processes should not have write access to the worker's cgroup filesystem.

Jobs that intentionally escape the cgroup are not supported. The worker does not provide container-grade isolation.

## Output streaming

Output is opaque binary data. The worker stores and streams bytes as-is.

The worker captures stdout and stderr as one combined stream:

```text
output.bin
```

The process stdout and stderr file descriptors both write to the same pipe. The drain goroutine reads that pipe as bytes and appends to the spool file. This preserves the output ordering seen by the worker instead of separating streams and reordering them later.

New output wakes waiting clients with a condition variable or broadcaster channel. No busy polling.

Multiple clients can stream the same job. Each client has its own internal read position.

Output chunks are bounded, for example 32 KiB or 64 KiB, to avoid huge gRPC messages.

If output read/spool fails, the job records `output_error`, waiting stream clients receive a gRPC error, and status remains queryable.

### EOF and ordering

Each chunk has:

```text
data = raw bytes
eof  = true when the process output pipe is closed
```

`data` is a protobuf `bytes` field, not a `string`.

`StreamOutput` replays bytes from the start, waits for new bytes while the job is running, and ends after the job exits and the output pipe is drained.

CLI writes job output bytes to local stdout with no decoration.

## Cgroups

Use cgroup v2 only.

Layout:

```text
/sys/fs/cgroup/jobworker/
  supervisor/
  job-<id>/
```

The worker keeps `/sys/fs/cgroup/jobworker` as a parent-only cgroup. During startup, the worker creates `/sys/fs/cgroup/jobworker/supervisor` and moves its own process there before enabling controllers, unless the service manager already placed it in a child cgroup.

Startup checks:

```text
cgroup v2 exists
jobworker cgroup root is writable
cpu controller exists
memory controller exists
io controller exists
cgroup.kill exists
child cgroups can be created
```

Initialization:

1. Read `/sys/fs/cgroup/jobworker/cgroup.controllers`.
2. Require `cpu`, `memory`, and `io`.
3. Move worker process into `/sys/fs/cgroup/jobworker/supervisor/cgroup.procs` if needed.
4. Write `+cpu +memory +io` to `/sys/fs/cgroup/jobworker/cgroup.subtree_control`.
5. Create one child cgroup per job.

Per-job files:

```text
cpu.max       CPU limit
memory.max    memory limit
io.max        disk I/O limit
cgroup.kill   kill remaining processes
```

Limit mapping:

```text
cpu_millicores=500       -> cpu.max = "50000 100000"
cpu unset                -> cpu.max = "max 100000"
memory_bytes=134217728   -> memory.max = "134217728"
memory unset             -> memory.max = "max"
io_read_bps/io_write_bps -> io.max = "<major>:<minor> rbps=<n> wbps=<n>"
io unset                 -> no io.max write
```

CPU is millicores. Memory is bytes. Disk I/O is read/write bytes per second. Limits are optional unsigned values. Omitted means no limit; zero is invalid when present.

### Disk I/O device resolution

Disk I/O uses cgroup v2 `io.max`.

`io.max` is device-based, not path-based. The worker resolves a device for the job:

1. Use the server-owned job working directory.
2. Read `/proc/self/mountinfo`.
3. Find the longest mount point prefix matching the directory.
4. Use that mount's major:minor device number.
5. Write `io.max` for that device.

If I/O limits are requested and the device cannot be resolved, or the `io.max` write fails, `Start` fails. If no I/O limits are requested, the worker skips `io.max`.

This is not perfect for every filesystem. `tmpfs`, network filesystems, overlay filesystems, loop devices, and device mapper can behave differently. That limitation is documented.

## gRPC API

```proto
syntax = "proto3";

package worker.v1;

service WorkerService {
  rpc StartJob(StartJobRequest) returns (StartJobResponse);
  rpc StopJob(StopJobRequest) returns (StopJobResponse);
  rpc GetJob(GetJobRequest) returns (GetJobResponse);
  rpc StreamOutput(StreamOutputRequest) returns (stream OutputChunk);
}

message StartJobRequest {
  string path = 1;
  repeated string args = 2;
  ResourceLimits limits = 3;
}

message StartJobResponse {
  Job job = 1;
}

message StopJobRequest {
  string job_id = 1;
}

message StopJobResponse {
  Job job = 1;
}

message GetJobRequest {
  string job_id = 1;
}

message GetJobResponse {
  Job job = 1;
}

message StreamOutputRequest {
  string job_id = 1;
}

message OutputChunk {
  bytes data = 1;
  bool eof = 2;
}

message Job {
  string id = 1;
  string owner = 2;
  string path = 3;
  repeated string args = 4;
  ResourceLimits limits = 5;
  JobState state = 6;
  int32 pid = 7;
  string cgroup_path = 8;
  ExitResult exit = 9;
  string output_error = 10;
}

message ResourceLimits {
  optional uint64 cpu_millicores = 1;
  optional uint64 memory_bytes = 2;
  optional uint64 io_read_bps = 3;
  optional uint64 io_write_bps = 4;
}

message ExitResult {
  int32 exit_code = 1;
  string signal = 2;
  string error = 3;
}

enum JobState {
  JOB_STATE_UNSPECIFIED = 0;
  JOB_STATE_STARTING = 1;
  JOB_STATE_RUNNING = 2;
  JOB_STATE_STOPPING = 3;
  JOB_STATE_EXITED = 4;
  JOB_STATE_KILLED = 5;
  JOB_STATE_FAILED = 6;
}

```

`job_id` is an opaque server-generated UUID string. Clients pass it back unchanged and do not parse meaning from it.

`StartJobRequest` does not include `cwd`. The server chooses the working directory.

Skip `ListJobs` for the first version.

## TLS and auth

Use mTLS only.

TLS config:

```text
MinVersion = tls.VersionTLS13
ClientAuth = tls.RequireAndVerifyClientCert
ClientCAs  = demo CA pool on server
RootCAs    = demo CA pool on CLI
```

Go does not expose manual TLS 1.3 cipher-suite selection through `tls.Config.CipherSuites`. The implementation uses TLS 1.3 only and relies on Go's TLS 1.3 cipher suites. TLS 1.2 is not enabled.

Demo cert setup:

```text
CA cert:       local demo CA
server cert:   serverAuth EKU, DNS/IP SAN for localhost dev
client certs:  clientAuth EKU, Common Name identity
key type:      Ed25519
```

No bearer token. No password. No custom auth header.

### Client identity and roles

Client identity comes from the verified client certificate Common Name.

Hardcoded role map:

```text
admin   -> admin
runner  -> runner
```

Policy:

```text
admin   all jobs, all actions
runner  start jobs, read own jobs, stop own jobs
```

Job owner is set from client identity at start time. Unknown client identities are rejected.

Authorization is implemented in the gRPC layer. A unary interceptor and stream interceptor extract the verified client certificate from the TLS connection, read its Common Name, map that identity to a role, and attach the authenticated principal to the RPC context. Each handler then checks the requested action against the role policy and, for existing jobs, the job owner before calling the worker library.

`StartJob` requires `runner` or `admin`. `GetJob`, `StopJob`, and `StreamOutput` require either `admin` or a matching job owner.

## CLI UX

Start server:

```bash
jobworker server --listen 127.0.0.1:8443 \
  --ca testdata/certs/ca.pem \
  --cert testdata/certs/server.pem \
  --key testdata/certs/server-key.pem
```

Start job:

```bash
jobctl start --addr 127.0.0.1:8443 \
  --ca testdata/certs/ca.pem \
  --cert testdata/certs/runner.pem \
  --key testdata/certs/runner-key.pem \
  --cpu 500m --memory 128Mi \
  --read-bps 10485760 --write-bps 10485760 \
  -- /usr/bin/echo hello
```

Other commands:

```bash
jobctl status <job-id>
jobctl output <job-id>
jobctl stop <job-id>
```

CLI output is raw by default. It writes combined job output bytes to local stdout.

## Security notes

No shell. Absolute executable path only. No `PATH` lookup.

mTLS authenticates. Authorization checks every RPC.

Cgroups are not isolation. Jobs still have the worker user's OS permissions. Run the worker as a dedicated low-privilege Unix user.

The CLI does not decorate output by default because output may be binary.

Demo certs are local-dev fixtures only.

## Testing

Focus tests on risky parts.

Worker/process tests: valid start, bad path, missing command, server-owned working directory, stop running job, stop exited job, child process cleanup, exit status, start rollback.

Output tests: replay from start, stream until EOF, multiple clients, combined stdout/stderr ordering, binary data, invalid UTF-8, NUL bytes, cancellation, stream error.

TLS/auth tests: valid cert, missing cert, unknown CA, missing Common Name, unknown identity, unauthorized role, owner checks, admin override.

Cgroup tests: create job cgroup, write `cpu.max`, write `memory.max`, write `io.max`, enable `cgroup.subtree_control`, direct cgroup placement, `cgroup.kill` cleanup, missing controller error.

Unit tests use temporary directories to verify cgroup file writes and error handling. A gated real cgroup v2 integration test verifies direct placement, resource-limit writes, `cgroup.kill`, and cleanup:

```bash
JOBWORKER_CGROUP_INTEGRATION=1 go test ./pkg/cgroup -run Integration
```

Run with the race detector.

## PR plan

1. Worker library with tests: job model, start, stop, status.
2. Output streaming with tests: spool, replay, terminal-like combined output, multiple clients.
3. gRPC, mTLS, authz, and CLI with tests.
4. Cgroup v2 limits and process cleanup with tests.
5. Final docs cleanup.
