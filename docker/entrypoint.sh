#!/usr/bin/env bash

set -euo pipefail

source_root=/workspace/source
workspace_root=/workspace/lance

shopt -s dotglob nullglob
for source_entry in "${source_root}"/*; do
    entry_name=${source_entry##*/}
    if [[ ${entry_name} == ".git" ]]; then
        ln -s "${source_entry}" "${workspace_root}/${entry_name}"
    else
        cp -as "${source_entry}" "${workspace_root}/"
    fi
done

declare -A repository_links=(
    ["java/lance-jni/rust-toolchain.toml"]="../../rust-toolchain.toml"
    ["python/rust-toolchain.toml"]="../rust-toolchain.toml"
    ["rust/lance-datafusion/protos"]="../../protos"
    ["rust/lance-encoding/protos"]="../../protos/"
    ["rust/lance-file/protos"]="../../protos/"
    ["rust/lance-index/protos"]="../../protos"
    ["rust/lance-table/protos"]="../../protos/"
    ["rust/lance/protos"]="../../protos"
)

for link_path in "${!repository_links[@]}"; do
    rm -f "${workspace_root}/${link_path}"
    ln -s "${repository_links[${link_path}]}" "${workspace_root}/${link_path}"
done

exec "$@"
