# autumn-harvest-sandbox-guest

The guest agent of the autumn-harvest sandbox microVMs.

The agent is process 1 in each Firecracker microVM. It mounts the basic file
systems, listens on a vsock port, and serves the host:

- A ping gets a pong.
- A run mixes the host entropy into the kernel random pool, runs the plan
  with the process executor, and returns the outcome. Then the agent reboots,
  and Firecracker exits.

Build it as a static binary and copy it to `/sbin/harvest-guest-agent` in
the root file system:

```sh
cargo build --release --target x86_64-unknown-linux-musl \
  -p autumn-harvest-sandbox-guest
```

`scripts/sandbox/build-guest-image.sh` builds a complete image. See
`docs/sandbox-activities.md`.

For a test on a host without KVM, `harvest-guest-agent --listen-unix <path>`
serves the same protocol on a Unix socket. That mode is not a sandbox.
