# Autobahn gate

The [Autobahn test suite](https://github.com/crossbario/autobahn-testsuite)
checks nwws against RFC 6455, on both sides:

- the suite's **fuzzingclient** tests the nwws server, `echoserver`;
- the suite's **fuzzingserver** tests the nwws client, `echoclient`.

## Running

Docker is required, nothing else beyond Go. From the repository root:

```sh
./autobahn/run.sh
```

The script builds the commands, runs both halves of the suite with the
`crossbario/autobahn-testsuite:latest` image (`AUTOBAHN_IMAGE` overrides it)
on the host network, and checks the results with `checkreport`. It exits
non-zero unless every case passes. A full run takes about a minute. To run
the nwws side under the race detector, set `GOFLAGS=-race`.

The suite is not part of `go test ./...`, which needs no Docker.

| Path | Purpose |
| --- | --- |
| `run.sh` | Runs the gate. |
| `echoserver/` | The nwws echo server, on `127.0.0.1:9701`. |
| `echoclient/` | The nwws echo client. It follows the fuzzingserver protocol: `getCaseCount`, `runCase` for each case, then `updateReports`. |
| `checkreport/` | Checks a report summary and prints the cases per behaviour. |
| `config/` | The fuzzingclient and fuzzingserver configurations. The fuzzingserver listens on `127.0.0.1:9702`. |
| `reports/server/index.json` | The summary of the fuzzingclient run against the nwws server. |
| `reports/client/index.json` | The summary of the fuzzingserver run against the nwws client. |

The full HTML reports land next to the summaries and are not committed.

## Passing

A case passes when its behaviour and its close behaviour are `OK`,
`NON-STRICT` or `INFORMATIONAL`. Every `NON-STRICT` case must be listed
below with its reason; `checkreport` fails on any that is not.

Sections 12 and 13 (permessage-deflate) are excluded until nwws supports
compression. The echo peers raise the read limit to 32 MiB, since section 9
sends messages of up to 16 MiB.

## NON-STRICT cases

The same cases are NON-STRICT on the server and the client side.

| Cases | Reason |
| --- | --- |
| 3.2, 3.3, 4.1.3, 4.1.4, 4.2.3, 4.2.4, 5.15 | The peer sends a valid message and then an invalid frame at once. The connection's read goroutine reads ahead: it fails the connection on the invalid frame, sending the close frame, before the application has echoed the valid message. The suite expects the echo first, but also accepts this. |
| 6.4.3, 6.4.4 | The peer sends a text frame in chunks, with invalid UTF-8 in an early chunk. nwws validates UTF-8 per frame once the frame's payload has arrived, not per chunk, so it fails the connection when the frame is complete instead of at the first invalid byte. |

## INFORMATIONAL cases

These cases have no required outcome:

- 7.1.6: the peer sends a 256 KiB message, a short message and a close
  frame back to back. Whether the echoes arrive before the close is up to
  the implementation; nwws echoes both.
- 7.13.1 and 7.13.2: close codes 5000 and 65535, which nwws answers with
  1002.
