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
- Stream stdout/stderr from the start of a job.
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

The worker gets a writable delegated cgroup subtree. The server receives this path with `--cgroup-root`. The worker manages job cgroups inside that root. It does not mount cgroup filesystems, edit systemd config, or do root-level host setup.

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
id
owner identity
executable path
args
cwd
limits
state
pid
process group id
cgroup path
exit result
output spool paths
output errors/truncation state
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
5. Create job ID.
6. Create output spool files.
7. Create job cgroup.
8. Write cgroup limits.
9. Open the job cgroup directory.
10. Start process in a new process group and directly inside the job cgroup.
11. Start stdout/stderr drain goroutines.
12. Start wait goroutine.
13. Mark job `RUNNING`.
14. Return job ID.

Primary implementation uses Linux `syscall.SysProcAttr`:

```go
SysProcAttr{
    Setpgid:      true,
    UseCgroupFD:  true,
    CgroupFD:     jobCgroupFD,
}
```

This places the child in the job cgroup at process start. That avoids the fork race from starting a process first and moving its PID to `cgroup.procs` later.

If direct cgroup placement is not available, `Start` fails with a clear error. I will not implement PID-after-start migration as the normal path because a process can fork before migration.

### Start failure rollback

`Start` returns a job ID only after setup succeeds.

If setup fails after side effects, rollback runs in reverse order:

1. Kill partial process/process group if it started.
2. Use `cgroup.kill` if the job cgroup has live processes.
3. Close pipes and spool files.
4. Delete partial output files.
5. Remove the job cgroup.
6. Remove the job from memory.
7. Return the original error, with cleanup errors attached or logged.

This prevents orphan processes, leaked cgroups, and leaked output files on failed starts.

## Wait lifecycle

Each started process has one wait goroutine.

The wait goroutine:

1. Waits for the process to exit.
2. Records exit code or signal.
3. Marks state `EXITED`, `KILLED`, or `FAILED`.
4. Waits for stdout/stderr drain goroutines to finish.
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
5. Send `SIGTERM` to the process group.
6. Wait a short hardcoded grace period.
7. If still running, write `1` to `cgroup.kill`.
8. If `cgroup.kill` is unavailable, fall back to `SIGKILL` on the process group.
9. Let the wait goroutine record final state and clean up.

`cgroup.kill` is the authoritative cleanup path for remaining job processes. Process group signaling is still useful for graceful stop and as a fallback.

Jobs that intentionally escape the cgroup are not supported. The worker does not provide container-grade isolation.

## Output streaming

Output is opaque binary data.

Rules:

- Do not decode UTF-8.
- Do not split by lines.
- Do not trim bytes.
- Do not add timestamps.
- Do not add stream labels to raw output.
- Do not assume newlines, printable bytes, or text encoding.

The worker captures stdout and stderr separately:

```text
stdout.bin
stderr.bin
```

Drain goroutines read process pipes as bytes and append to spool files. Stream clients read from byte offsets. Default offset is zero, so clients see output from the start.

New output wakes waiting clients with a condition variable or broadcaster channel. No busy polling.

Multiple clients can stream the same job. Each client has its own offsets.

Output chunks are bounded, for example 32 KiB or 64 KiB, to avoid huge gRPC messages.

A hardcoded max spool size prevents disk fill. If reached, the worker keeps draining pipes but stops saving extra bytes. Job status reports `output_truncated=true`.

If output read/spool fails, the job records `output_error`, waiting stream clients receive a gRPC error, and status remains queryable.

### Follow, EOF, offsets, and ordering

Offsets are per stream:

```text
stdout_offset = byte offset in stdout.bin
stderr_offset = byte offset in stderr.bin
```

Each chunk has:

```text
stream = STDOUT or STDERR
offset = starting byte offset for that stream
data   = raw bytes
eof    = true when that stream is closed
```

`data` is a protobuf `bytes` field, not a `string`.

Clients update only the offset for the stream they receive.

`follow=false` means replay available bytes from the requested offsets and return. It does not wait for future output. If the job is already done, the server also sends EOF for closed streams.

`follow=true` means replay available bytes, then wait for new bytes. The RPC ends after the job exits, both pipes are drained, and both streams have sent EOF.

Ordering is guaranteed within each stream. There is no total ordering between stdout and stderr. They are separate Linux pipes, so cross-stream ordering is best effort only.

CLI writes job stdout bytes to local stdout and job stderr bytes to local stderr. No decoration by default. This keeps binary output safe.

## Cgroups

Use cgroup v2 only.

Server flag:

```text
--cgroup-root <path>
```

Layout:

```text
<cgroup-root>/
  supervisor/
  job-<id>/
```

The worker keeps `<cgroup-root>` as a parent-only cgroup. During startup, the worker creates `<cgroup-root>/supervisor` and moves its own process there before enabling controllers, unless the service manager already placed it in a child cgroup.

Startup checks:

```text
cgroup v2 exists
root is writable
cpu controller exists
memory controller exists
io controller exists
child cgroups can be created
```

Initialization:

1. Read `<cgroup-root>/cgroup.controllers`.
2. Require `cpu`, `memory`, and `io`.
3. Move worker process into `<cgroup-root>/supervisor/cgroup.procs` if needed.
4. Write `+cpu +memory +io` to `<cgroup-root>/cgroup.subtree_control`.
5. Create one child cgroup per job.

Per-job files:

```text
cpu.max       CPU limit
memory.max    memory limit
io.max        disk I/O limit
cgroup.procs  attach process, fallback only
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

CPU is millicores. Memory is bytes. Disk I/O is read/write bytes per second.

### Disk I/O device resolution

Disk I/O uses cgroup v2 `io.max`.

`io.max` is device-based, not path-based. The worker resolves a device for the job:

1. Use the job `cwd`. If unset, use the worker's job work directory.
2. Read `/proc/self/mountinfo`.
3. Find the longest mount point prefix matching the directory.
4. Use that mount's major:minor device number.
5. Write `io.max` for that device.

If I/O limits are requested and the device cannot be resolved, or the `io.max` write fails, `Start` fails. If no I/O limits are requested, the worker skips `io.max`.

This is not perfect for every filesystem. `tmpfs`, network filesystems, overlay filesystems, loop devices, and device mapper can behave differently. That limitation is documented.

## gRPC API

```proto
service WorkerService {
  rpc StartJob(StartJobRequest) returns (StartJobResponse);
  rpc StopJob(StopJobRequest) returns (StopJobResponse);
  rpc GetJob(GetJobRequest) returns (GetJobResponse);
  rpc StreamOutput(StreamOutputRequest) returns (stream OutputChunk);
}
```

Main fields:

```text
StartJobRequest: path, args, cwd, cpu_millicores, memory_bytes, io_read_bps, io_write_bps
StopJobRequest: job_id
GetJobRequest: job_id
StreamOutputRequest: job_id, stdout_offset, stderr_offset, follow
OutputChunk: stream, offset, bytes data, truncated, eof
```

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
client certs:  clientAuth EKU, URI SAN identity
key type:      ECDSA P-256 or Ed25519
```

No bearer token. No password. No custom auth header.

### URI SAN identity and roles

Client identity comes from the verified client certificate URI SAN. Common Name is ignored.

URI SAN format:

```text
spiffe://jobworker.local/user/<name>
```

Hardcoded role map:

```text
spiffe://jobworker.local/user/admin   -> admin
spiffe://jobworker.local/user/runner  -> runner
```

Policy:

```text
admin   all jobs, all actions
runner  start jobs, read own jobs, stop own jobs
```

Job owner is set from client identity at start time. Unknown URI SANs are rejected.

## CLI UX

Start server:

```bash
jobworker server --listen 127.0.0.1:8443 \
  --cgroup-root /sys/fs/cgroup/jobworker \
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
  --cwd /tmp -- /usr/bin/echo hello
```

Other commands:

```bash
jobctl status <job-id>
jobctl output <job-id>
jobctl output --follow <job-id>
jobctl stop <job-id>
```

CLI output is raw by default. It writes job stdout bytes to local stdout and job stderr bytes to local stderr.

## Security notes

No shell. Absolute executable path only. No `PATH` lookup.

mTLS authenticates. Authorization checks every RPC.

Cgroups are not isolation. Jobs still have the worker user's OS permissions. Run the worker as a dedicated low-privilege Unix user.

The CLI does not decorate output by default because output may be binary.

Demo certs are local-dev fixtures only.

## Testing

Focus tests on risky parts.

Worker/process tests: valid start, bad path, missing command, stop running job, stop exited job, child process cleanup, exit status, start rollback.

Output tests: replay from start, follow mode, EOF, offsets, multiple clients, stdout/stderr separation, binary data, invalid UTF-8, NUL bytes, cancellation, truncation, stream error.

TLS/auth tests: valid cert, missing cert, unknown CA, missing URI SAN, unknown URI SAN, unauthorized role, owner checks, admin override.

Cgroup tests: create job cgroup, write `cpu.max`, write `memory.max`, write `io.max`, enable `cgroup.subtree_control`, direct cgroup placement, cleanup, missing controller error.

Most cgroup tests use a fake cgroup filesystem. Real cgroup tests are gated:

```bash
JOBWORKER_CGROUP_INTEGRATION=1 go test ./pkg/cgroup -run Integration
```

Run with the race detector.

## PR plan

1. Worker library: job model, start, stop, status.
2. Output streaming: spool, replay, follow, multiple clients.
3. gRPC, mTLS, authz, CLI.
4. cgroup v2 limits and process cleanup.
5. Polish: tests, docs.

