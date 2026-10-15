# Lance development container

The `lance-dev` service is a persistent Rust development environment for this
checkout. The host repository is mounted at `/workspace/source`. At startup the
container creates a lightweight working tree at `/workspace/lance` whose files
point back to the host checkout, while restoring the Unix symbolic links that a
Windows checkout cannot represent. Edits to existing host files are immediately
visible in the container. Restart the service after adding, deleting, or moving
files so the working tree structure is refreshed.

Cargo downloads and build output are stored in named volumes to preserve
incremental builds across container restarts and image rebuilds. Run Git commands
against the host checkout rather than the generated container working tree.

Start the development container:

```bash
docker compose up --build --detach lance-dev
```

Open a shell:

```bash
docker compose exec lance-dev bash
```

The container pins `RUSTUP_TOOLCHAIN` so normal builds do not attempt to update
the toolchain when entering the mounted repository. If lint tooling is needed
and the Rust component server is reachable, install the optional components and
rebuild the image afterward if they should be permanent:

```bash
docker compose exec lance-dev rustup component add rustfmt clippy rust-analyzer
```

Run common Rust commands:

```bash
docker compose exec lance-dev cargo check --workspace --tests --benches
docker compose exec lance-dev cargo test -p lance
docker compose exec lance-dev cargo fmt --all --check
docker compose exec lance-dev cargo clippy --all --tests --benches -- -D warnings
```

Start LocalStack when tests need AWS-compatible services:

```bash
docker compose up --detach localstack
```

Stop containers while retaining build caches:

```bash
docker compose down
```

Running `docker compose down --volumes` also deletes the Cargo and build caches.
