# Autobahn gate

The [Autobahn test suite](https://github.com/crossbario/autobahn-testsuite)
checks nwws against RFC 6455, on both sides:

- the suite's **fuzzingclient** tests the nwws server, `echoserver`;
- the suite's **fuzzingserver** tests the nwws client, `echoclient`.

It runs in two passes:

- **plain**: every section except 12 and 13, without compression;
- **deflate**: sections 12 and 13 (permessage-deflate), with compression
  enabled on both echo peers (`-compress`). The peers compress every
  non-empty message, so every echo goes through the compressor.

## Running

Docker is required, nothing else beyond Go. From the repository root:

```sh
./autobahn/run.sh
```

The script builds the commands, runs both halves of the suite for each
pass with the `crossbario/autobahn-testsuite:latest` image (`AUTOBAHN_IMAGE`
overrides it) on the host network, and checks the results with
`checkreport`. It exits non-zero unless every case passes. A full run takes
a few minutes, most of it in the deflate pass. To run the nwws side under
the race detector, set `GOFLAGS=-race`.

The suite is not part of `go test ./...`, which needs no Docker.

| Path | Purpose |
| --- | --- |
| `run.sh` | Runs the gate. |
| `echoserver/` | The nwws echo server: on `127.0.0.1:9701` for the plain pass, `127.0.0.1:9703` with `-compress` for the deflate pass. |
| `echoclient/` | The nwws echo client. It follows the fuzzingserver protocol: `getCaseCount`, `runCase` for each case, then `updateReports`. |
| `checkreport/` | Checks a report summary and prints the cases per behaviour. |
| `config/` | The fuzzingclient and fuzzingserver configurations of each pass, `*-deflate.json` for the deflate pass. The fuzzingserver listens on `127.0.0.1:9702` for the plain pass and `127.0.0.1:9704` for the deflate pass. |
| `reports/server/index.json` | The summary of the plain fuzzingclient run against the nwws server. |
| `reports/client/index.json` | The summary of the plain fuzzingserver run against the nwws client. |
| `reports/server-deflate/index.json` | The summary of the deflate fuzzingclient run against the nwws server. |
| `reports/client-deflate/index.json` | The summary of the deflate fuzzingserver run against the nwws client. |

The full HTML reports land next to the summaries and are not committed.

## Passing

A case passes when its behaviour and its close behaviour are `OK`,
`NON-STRICT` or `INFORMATIONAL`, or `UNIMPLEMENTED` for an extension offer
nwws declines. Every `NON-STRICT` and every `UNIMPLEMENTED` case must be
listed below with its reason; `checkreport` fails on any that is not.

The echo peers raise the read limit to 32 MiB, since section 9 sends
messages of up to 16 MiB.

## NON-STRICT cases

Only 15 cases of the suite have a `NON-STRICT` outcome at all; every other
case is either `OK` or failed. They fall into the classes below, and each
class is listed whole, on both sides.

| Cases | Reason |
| --- | --- |
| 3.2, 3.3, 3.4, 4.1.3, 4.1.4, 4.1.5, 4.2.3, 4.2.4, 4.2.5, 5.15 | Read-ahead, timing-dependent. The peer sends a valid message and then an invalid frame at once (an RSV bit, a reserved opcode, or a continuation with nothing to continue). The connection's read goroutine reads ahead: it fails the connection on the invalid frame, sending the close frame, before the application has echoed the valid message. The suite expects the echo first, but also accepts this. Whether the echo or the close goes out first depends on scheduling, so any case of this class may come out `OK` or `NON-STRICT` in a given run. |
| 6.4.3, 6.4.4 | The peer sends a text frame in chunks, with invalid UTF-8 in an early chunk. nwws validates UTF-8 per frame once the frame's payload has arrived, not per chunk, so it fails the connection when the frame is complete instead of at the first invalid byte. This does not depend on timing. The two cases of the same section that send the invalid UTF-8 in a frame of its own are always `OK` and are not listed. |

The remaining case of the 15 always ends up `INFORMATIONAL` (see below).

## UNIMPLEMENTED cases

None. `UNIMPLEMENTED` means that nwws declined the suite's permessage-deflate
offer, and nwws accepts every offer of sections 12 and 13:

- Sections 13.3 and 13.5 limit the compression window to 9 bits, below the
  32 KiB window that `compress/flate` always uses. nwws accepts the limit
  and compresses with Huffman coding only, which needs no window, so these
  cases pass instead of being declined.
- The nwws client offers `client_max_window_bits`, so the fuzzingserver's
  section 13 accepts it at every window size it asks for.

If a later change makes nwws decline an offer because of the window size,
list each such case here with its reason.

## INFORMATIONAL cases

These cases have no required outcome:

- 7.1.6: the peer sends a 256 KiB message, a short message and a close
  frame back to back. Whether the echoes arrive before the close is up to
  the implementation; nwws echoes both.
- 7.13.1 and 7.13.2: close codes 5000 and 65535, which nwws answers with
  1002.
