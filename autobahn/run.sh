#!/usr/bin/env bash
# Runs the Autobahn test suite against nwws, in Docker, in two passes:
#   plain:   every section but 12 and 13, without compression;
#   deflate: sections 12 and 13, with permessage-deflate on both echo peers.
# Each pass runs
#   1. the fuzzingclient against the nwws echo server (autobahn/echoserver);
#   2. the fuzzingserver against the nwws echo client (autobahn/echoclient).
# Reports go to autobahn/reports/{server,client} for the plain pass and
# autobahn/reports/{server,client}-deflate for the deflate pass. The script
# exits non-zero unless every pass run has all its cases (cases below) and
# every case is strictly OK (see autobahn/checkreport).
#
# Environment:
#   AUTOBAHN_PASSES the passes to run: plain, deflate or all (default all)
#   AUTOBAHN_IMAGE  the suite's image (default crossbario/autobahn-testsuite:latest)
#   GOFLAGS         passed to go build, for example GOFLAGS=-race
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
root=$(cd "$here/.." && pwd)
image=${AUTOBAHN_IMAGE:-crossbario/autobahn-testsuite:latest}
passes=${AUTOBAHN_PASSES:-all}
case $passes in
plain) sides=(server client) ;;
deflate) sides=(server-deflate client-deflate) ;;
all) sides=(server client server-deflate client-deflate) ;;
*)
	echo "AUTOBAHN_PASSES=$passes: want plain, deflate or all" >&2
	exit 2
	;;
esac

# cases is the number of cases each report must have: a run that lost a
# case fails the gate even when every case it has passed. They change only
# when the suite's image or the configurations change.
declare -A cases=([server]=301 [client]=301 [server-deflate]=216 [client-deflate]=216)

bin=$(mktemp -d)
pids=()
containers=()
cleanup() {
	for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
	for c in "${containers[@]}"; do docker rm -f "$c" >/dev/null 2>&1 || true; done
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

# fuzz_server runs the fuzzingclient with the configuration $1 against an
# echo server listening on $2, started with the remaining arguments.
fuzz_server() {
	local config=$1 addr=$2
	shift 2
	"$bin/echoserver" -addr "$addr" "$@" &
	local pid=$!
	pids+=("$pid")
	wait_port "${addr##*:}"
	kill -0 "$pid" # the port is the echo server's, not another listener's
	docker run --rm "${docker_args[@]}" wstest -m fuzzingclient -s "/config/$config" >"$bin/$config.log" 2>&1 ||
		{ tail -n 50 "$bin/$config.log" >&2; exit 1; }
	kill "$pid"
}

# fuzz_client runs the fuzzingserver with the configuration $1, listening
# on port $2, against the echo client started with the remaining arguments.
fuzz_client() {
	local config=$1 port=$2
	shift 2
	local container=nwws-autobahn-$$-$port
	containers+=("$container")
	docker run -d --name "$container" "${docker_args[@]}" \
		wstest -m fuzzingserver -s "/config/$config" -u 0 >/dev/null
	wait_port "$port"
	"$bin/echoclient" -url "ws://127.0.0.1:$port" "$@"
	docker rm -f "$container" >/dev/null
}

echo "== building"
(cd "$root" && go build -o "$bin/" ./autobahn/echoserver ./autobahn/echoclient ./autobahn/checkreport)

for side in "${sides[@]}"; do
	rm -rf "${here:?}/reports/$side"
	mkdir -p "$here/reports/$side"
done
# The suite runs on the host network, as the current user so that the
# reports it writes are ours.
docker_args=(--network host --user "$(id -u):$(id -g)"
	-v "$here/config:/config:ro" -v "$here/reports:/reports" "$image")

# The ports are also in the configurations.
if [[ $passes != deflate ]]; then
	echo "== plain: fuzzingclient against the nwws echo server"
	fuzz_server fuzzingclient.json 127.0.0.1:9701
	echo "== plain: fuzzingserver against the nwws echo client"
	fuzz_client fuzzingserver.json 9702
fi
if [[ $passes != plain ]]; then
	echo "== deflate: fuzzingclient against the nwws echo server"
	fuzz_server fuzzingclient-deflate.json 127.0.0.1:9703 -compress
	echo "== deflate: fuzzingserver against the nwws echo client"
	fuzz_client fuzzingserver-deflate.json 9704 -compress
fi

echo "== results"
reports=()
for side in "${sides[@]}"; do reports+=("$here/reports/$side/index.json:${cases[$side]}"); done
"$bin/checkreport" "${reports[@]}"
