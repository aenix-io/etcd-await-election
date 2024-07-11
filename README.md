# etcd-await-election

![Latest release](https://img.shields.io/github/v/release/aenix-io/etcd-await-election)

Ensure that only a single instance of a process is running in your environment by leveraging the `etcd` built-in leader election capabilities.

`etcd-await-election` acts as a gatekeeper, only starting the command when it successfully acquires leadership in an `etcd` cluster. This is particularly useful for processes that must run exclusively or require coordination across multiple instances.

### Inspiration
This project was inspired by [k8s-await-election](https://github.com/LINBIT/k8s-await-election), and serves a similar purpose but operates directly with `etcd` without the dependency on Kubernetes. It provides a flexible, Kubernetes-independent solution for managing singleton processes in any environment where `etcd` is used.

### Why leader election?
In scenarios where applications manage critical stateful data or need to maintain singleton operations, leader election helps prevent race conditions and ensures that only one instance of the service operates as the leader at any time.

## Usage
Set `etcd-await-election` as your command's entry point in the container or execution environment. The leader election is configured via environment variables. Without these variables, `etcd-await-election` will execute the command immediately without participating in an election.

The relevant environment variables are:

 Variable                          | Description |
|-----------------------------------|-------------|
| `ETCD_AWAIT_ELECTION_LOCK_NAME`   | Name of the lock within etcd used for the election process. |
| `ETCD_AWAIT_ELECTION_IDENTITY`    | Unique identity for the instance competing for leadership. Typically set to the pod's name. |
| `ETCD_AWAIT_ELECTION_STATUS_ENDPOINT` | HTTP endpoint to report leadership status, formatted as `host:port` (e.g., `127.0.0.1:1234`). Optional: If not set, no status will be reported. |
| `ETCD_AWAIT_ELECTION_LEASE_DURATION` | Lease duration (seconds) before a leader must renew. Optional: Default is 15 seconds. |
| `ETCD_AWAIT_ELECTION_RENEW_DEADLINE` | Time limit (seconds) for the leader to renew the lease. Optional: Default is 10 seconds. |
| `ETCD_AWAIT_ELECTION_RETRY_PERIOD` | Time interval (seconds) to retry a failed election. Optional: Default is 2 seconds. |
| `ETCD_AWAIT_ELECTION_ENDPOINTS` | Comma-separated list of etcd endpoints. |
| `ETCD_AWAIT_ELECTION_CERT`, `ETCD_AWAIT_ELECTION_KEY`, `ETCD_AWAIT_ELECTION_CACERT` | TLS certificates for secure communication with etcd. |
| `ETCD_AWAIT_ELECTION_FORCE_ACQUIRE` | Set to any non-empty value to forcefully acquire leadership by deleting existing locks. Optional: Use with caution as only one instance should run with this option. |

### Installation

Binaries can be downloaded from the [releases page](https://github.com/aenix-io/etcd-await-election/releases).

### Example Configuration

To run a command under `etcd-await-election` control, configure your environment variables and command as follows:

```sh
ETCD_AWAIT_ELECTION_CACERT=/etc/etcd/pki/ca.crt \
ETCD_AWAIT_ELECTION_CERT=/etc/etcd/pki/etcdctl-etcd-client.crt \
ETCD_AWAIT_ELECTION_KEY=/etc/etcd/pki/etcdctl-etcd-client.key \
ETCD_AWAIT_ELECTION_ENDPOINTS='https://127.0.0.1:2379' \
ETCD_AWAIT_ELECTION_LOCK_NAME=test \
ETCD_AWAIT_ELECTION_IDENTITY=$HOSTNAME \
ETCD_AWAIT_ELECTION_STATUS_ENDPOINT=127.0.0.1:1234 \
etcd-await-election sleep 100m
```

This command sets up the required TLS configuration, specifies the `etcd` endpoints, and defines the lock name and identity before running a `sleep` command under the `etcd-await-election` system for 100 minutes, assuming leadership has been acquired.
