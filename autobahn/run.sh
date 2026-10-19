#!/usr/bin/env bash
# Runs the Autobahn test suite against nwws, in Docker:
#   1. the fuzzingclient against the nwws echo server (autobahn/echoserver);
#   2. the fuzzingserver against the nwws echo client (autobahn/echoclient).
# Reports go to autobahn/reports/{server,client}. The script exits non-zero
# unless every case passes (see autobahn/checkreport).
#
# Environment:
#   AUTOBAHN_IMAGE  the suite's image (default crossbario/autobahn-testsuite:latest)
#   GOFLAGS         passed to go build, for example GOFLAGS=-race
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
root=$(cd "$here/.." && pwd)
image=${AUTOBAHN_IMAGE:-crossbario/autobahn-testsuite:latest}
server_addr=127.0.0.1:9701 # in config/fuzzingclient.json
fuzzer_port=9702           # in config/fuzzingserver.json

bin=$(mktemp -d)
pids=()
container=""
cleanup() {
	for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
	if [[ -n $container ]]; then docker rm -f "$container" >/dev/null 2>&1 || true; fi
	rm -rf "$bin"
}
trap cleanup EXIT

# wait_port waits up to 30 s for something to listen on 127.0.0.1:$1.
wait_port() {
	for _ in $(seq 300); do
		if (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; then return 0; fi
		sleep 0.1
	done
	echo "nothing listens on port $1" >&2
	return 1
}

echo "== building"
(cd "$root" && go build -o "$bin/" ./autobahn/echoserver ./autobahn/echoclient ./autobahn/checkreport)

for side in server client; do
	rm -rf "${here:?}/reports/$side"
	mkdir -p "$here/reports/$side"
done
# The suite runs on the host network, as the current user so that the
# reports it writes are ours.
docker_args=(--network host --user "$(id -u):$(id -g)"
	-v "$here/config:/config:ro" -v "$here/reports:/reports" "$image")

echo "== fuzzingclient against the nwws echo server"
"$bin/echoserver" -addr "$server_addr" &
pids+=($!)
wait_port "${server_addr##*:}"
kill -0 "${pids[0]}" # the port is the echo server's, not another listener's
docker run --rm "${docker_args[@]}" wstest -m fuzzingclient -s /config/fuzzingclient.json >"$bin/fuzzingclient.log" 2>&1 ||
	{ tail -n 50 "$bin/fuzzingclient.log" >&2; exit 1; }

echo "== fuzzingserver against the nwws echo client"
container=nwws-autobahn-$$
docker run -d --name "$container" "${docker_args[@]}" \
	wstest -m fuzzingserver -s /config/fuzzingserver.json -u 0 >/dev/null
wait_port "$fuzzer_port"
"$bin/echoclient" -url "ws://127.0.0.1:$fuzzer_port"

echo "== results"
"$bin/checkreport" -readme "$here/README.md" "$here/reports/server/index.json" "$here/reports/client/index.json"
