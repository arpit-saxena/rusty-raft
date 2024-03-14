# Rusty Raft

Another Raft implementation in Rust. This is a hobby project to learn about Raft and Rust (also alliteration is cool). Here I've tried to leverage Rust as best as possible to implement the distributed consensus algorithm. A few salient features:

- **Everything is async**, and due to how tokio works can be scheduled on multiple cores, improving performance and reducing wait times.
- **(Very) Lockless**, using atomics as much as possible so tasks can progress without waiting on each other. While things like writing to a file are necessarily serialized, we aim to have as few locks as possible.

## What is Raft?

[Raft](https://en.wikipedia.org/wiki/Raft_(algorithm)) is a consensus algorithm for making replicated state machines. That basically means that multiple nodes have state machines that receive inputs in the same order, even in the presence of server crashes or network issues.

Raft achieves consensus by electing a _leader_ which accepts input from the outside world and replicates it to it's _followers_. In case of a _leader_ crashing, a _follower_ will randomly timeout and start an election process. This ensures the cluster continues to work in presence of crashes.

See the [Raft Paper](https://raft.github.io/raft.pdf) for complete description of the algorithm

## Why Rust?

Rust is a modern, fast-growing systems programming language with amazing tooling, good quality third party libraries and a very helpful community. Rust has more restrictions imposed to support memory safety and also safe concurrency. Safe Rust **guarantees** an absence of data races!!! (which is honestly the coolest thing, because a data race is an Undefined Behaviour, and that should be enough to send a chill up your spine). This all makes Rust very worthwhile to learn, since those ideas to prevent entire class of bugs can be used in other places as well that don't have the required static analysis.

## How to Run

Currently there are 2 modes to run: single server (node) or a cluster of servers. The latter option is sort of the same as spawning multiple processes of the first mode

### Single node

```sh
cargo run -- node <config> <node_index>
```

The last parameter specifies the node index, which is an index into the `cluster_members` array (see below).Config is in RON (Rust Object Notation) format, find `config.ron` committed in the root directory of the repo. It has the following parameters:

- `persistent_state_file`: File to store the persistent state of the server, including log entries
- `cluster_members`: Array of IP address & port combinations on which servers of this cluster will listen. This also includes the listen address of current node so we can use 1 config for all nodes of the cluster
- `election_timeout_interval`: Inclusive range of election timeout in milliseconds
- `heartbeat_interval`: Time interval in which an empty AppendEntries RPC will be sent, to assert leadership

### Cluster

```sh
cargo run -- cluster <cluster_config> <num_nodes>
```

Cluster config is also in RON format, and is used to construct individual node configs. Find `cluster_config.ron` in root directory of the repository. Only new variable is `data_path`, which is used as a folder to place individual node's `persistent_state_file`s.

## Logs

This uses the popular [tracing](https://docs.rs/tracing/latest/tracing/) crate for logging purposes. By default, only logs of visibility INFO and above are printed to console. To view all logs produced the library, run with the environment variable `RUST_LOG=raft=trace`. Replace `trace` with `debug`/`info`/`warn`/`error` to change error logging levels.
