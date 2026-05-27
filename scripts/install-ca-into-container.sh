#!/usr/bin/env sh
set -eu

CONTAINER="${CONTAINER:-cli-proxy-api}"
CA_CERT="${CA_CERT:-./certs/ca.crt}"
SSH_HOST="${SSH_HOST:-}"
ALPINE_VERSION="${ALPINE_VERSION:-v3.23}"
ALPINE_MIRROR="${ALPINE_MIRROR:-https://mirrors.tuna.tsinghua.edu.cn/alpine}"
INSTALL_NAME="${INSTALL_NAME:-codex-reset-proxy-ca.crt}"
SKIP_APK="${SKIP_APK:-0}"

usage() {
  cat <<EOF
Usage: $0 [--host USER@HOST] [--container NAME] [--ca PATH] [--alpine-version v3.23] [--mirror URL] [--skip-apk]

Installs the codex-reset-proxy MITM CA into a running Docker container.

Examples:
  $0 --container cli-proxy-api --ca ./certs/ca.crt
  $0 --host asants@10.255.200.17 --container cli-proxy-api --ca ./certs/ca.crt

Environment defaults:
  SSH_HOST=$SSH_HOST
  CONTAINER=$CONTAINER
  CA_CERT=$CA_CERT
  ALPINE_VERSION=$ALPINE_VERSION
  ALPINE_MIRROR=$ALPINE_MIRROR
  INSTALL_NAME=$INSTALL_NAME
  SKIP_APK=$SKIP_APK
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --host)
      SSH_HOST="$2"
      shift 2
      ;;
    --container)
      CONTAINER="$2"
      shift 2
      ;;
    --ca)
      CA_CERT="$2"
      shift 2
      ;;
    --alpine-version)
      ALPINE_VERSION="$2"
      shift 2
      ;;
    --mirror)
      ALPINE_MIRROR="$2"
      shift 2
      ;;
    --skip-apk)
      SKIP_APK=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [ ! -f "$CA_CERT" ]; then
  echo "CA certificate not found: $CA_CERT" >&2
  echo "Start codex-reset-proxy once in socks5_mitm mode to generate ./certs/ca.crt." >&2
  exit 1
fi

docker_inspect() {
  if [ -n "$SSH_HOST" ]; then
    ssh "$SSH_HOST" docker inspect "$CONTAINER" >/dev/null
  else
    docker inspect "$CONTAINER" >/dev/null
  fi
}

copy_ca_to_container() {
  tmp_container_path="$1"
  if [ -n "$SSH_HOST" ]; then
    remote_tmp="/tmp/$INSTALL_NAME.$$"
    scp "$CA_CERT" "$SSH_HOST:$remote_tmp" >/dev/null
    ssh "$SSH_HOST" docker cp "$remote_tmp" "$CONTAINER:$tmp_container_path"
    ssh "$SSH_HOST" rm -f "$remote_tmp"
  else
    docker cp "$CA_CERT" "$CONTAINER:$tmp_container_path"
  fi
}

run_install_in_container() {
  if [ -n "$SSH_HOST" ]; then
    ssh "$SSH_HOST" docker exec -i "$CONTAINER" sh -s -- "$@"
  else
    docker exec -i "$CONTAINER" sh -s -- "$@"
  fi
}

copy_bundle_from_container() {
  bundle_tmp="$1"
  if [ -n "$SSH_HOST" ]; then
    remote_bundle="/tmp/ca-certificates.$$.crt"
    ssh "$SSH_HOST" docker cp "$CONTAINER:/etc/ssl/certs/ca-certificates.crt" "$remote_bundle"
    scp "$SSH_HOST:$remote_bundle" "$bundle_tmp" >/dev/null
    ssh "$SSH_HOST" rm -f "$remote_bundle"
  else
    docker cp "$CONTAINER:/etc/ssl/certs/ca-certificates.crt" "$bundle_tmp"
  fi
}

if ! docker_inspect; then
  echo "Docker container not found: $CONTAINER" >&2
  exit 1
fi

tmp_container_path="/tmp/$INSTALL_NAME"
target_dir="/usr/local/share/ca-certificates"
target_path="$target_dir/$INSTALL_NAME"

copy_ca_to_container "$tmp_container_path"

run_install_in_container "$target_dir" "$target_path" "$tmp_container_path" "$ALPINE_VERSION" "$ALPINE_MIRROR" "$SKIP_APK" <<'INSTALL_SH'
set -eu

target_dir="$1"
target_path="$2"
tmp_path="$3"
alpine_version="$4"
alpine_mirror="$5"
skip_apk="$6"

mkdir -p "$target_dir"
cp "$tmp_path" "$target_path"
rm -f "$tmp_path"

if command -v apk >/dev/null 2>&1 && [ "$skip_apk" != "1" ]; then
  cp /etc/apk/repositories "/etc/apk/repositories.bak.$(date +%s)" 2>/dev/null || true
  printf "%s\n%s\n" \
    "$alpine_mirror/$alpine_version/main" \
    "$alpine_mirror/$alpine_version/community" \
    > /etc/apk/repositories
  apk update
  apk add --no-cache ca-certificates
fi

if ! command -v update-ca-certificates >/dev/null 2>&1; then
  echo "update-ca-certificates not found in container" >&2
  exit 1
fi

update-ca-certificates
test -s /etc/ssl/certs/ca-certificates.crt
ls -l "$target_path"
INSTALL_SH

bundle_tmp="$(mktemp)"
trap 'rm -f "$bundle_tmp"' EXIT
copy_bundle_from_container "$bundle_tmp"

cert_marker="$(sed -n '2p' "$CA_CERT")"
if [ -n "$cert_marker" ] && grep -Fq "$cert_marker" "$bundle_tmp"; then
  target="$CONTAINER"
  if [ -n "$SSH_HOST" ]; then
    target="$SSH_HOST:$CONTAINER"
  fi
  echo "CA installed into $target and present in /etc/ssl/certs/ca-certificates.crt"
else
  echo "CA copied, but could not verify it in the container CA bundle" >&2
  exit 1
fi
