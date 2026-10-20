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
`checkreport`. It exits non-zero unless every report has all its cases and
every case passes. A full run takes
a few minutes, most of it in the deflate pass. To run the nwws side under
the race detector, set `GOFLAGS=-race`.

`AUTOBAHN_PASSES` selects the passes to run: `plain`, `deflate` or `all`
(the default). Only the reports of the passes run are replaced and checked:

```sh
AUTOBAHN_PASSES=plain ./autobahn/run.sh
```

The suite is not part of `go test ./...`, which needs no Docker.

| Path | Purpose |
| --- | --- |
| `run.sh` | Runs the gate. |
| `echoserver/` | The nwws echo server: on `127.0.0.1:9701` for the plain pass, `127.0.0.1:9703` with `-compress` for the deflate pass. |
| `echoclient/` | The nwws echo client. It follows the fuzzingserver protocol: `getCaseCount`, `runCase` for each case, then `updateReports`. |
| `checkreport/` | Checks report summaries against their expected case counts (`index.json:301`) and prints the cases per behaviour. |
| `config/` | The fuzzingclient and fuzzingserver configurations of each pass, `*-deflate.json` for the deflate pass. The fuzzingserver listens on `127.0.0.1:9702` for the plain pass and `127.0.0.1:9704` for the deflate pass. |
| `reports/server/index.json` | The summary of the plain fuzzingclient run against the nwws server. |
| `reports/client/index.json` | The summary of the plain fuzzingserver run against the nwws client. |
| `reports/server-deflate/index.json` | The summary of the deflate fuzzingclient run against the nwws server. |
| `reports/client-deflate/index.json` | The summary of the deflate fuzzingserver run against the nwws client. |

The full HTML reports land next to the summaries and are not committed.

## Passing

The gate is strict. A case passes only when its behaviour and its close
behaviour are both `OK`, or `INFORMATIONAL` where the suite defines no
stricter outcome. `checkreport` fails on every other outcome, including
`NON-STRICT` and `UNIMPLEMENTED`.

Every report must also have the expected number of cases: 301 for each
side of the plain pass and 216 for each side of the deflate pass. A run
that lost a case fails even when every case it has passed. `run.sh`
holds these counts and passes each with its report, as
`index.json:cases`; they change only with the suite's image or the
configurations.

The echo peers raise the read limit to 32 MiB, since section 9 sends
messages of up to 16 MiB.

Two properties of the connection keep the cases strict that would
otherwise come out `NON-STRICT`:

- **Failures in order** (3.2–3.4, 4.1.3–4.1.5, 4.2.3–4.2.5, 5.15). When the
  peer sends a valid message and then an invalid frame at once, `Read`
  reports the violation only after the message, and the close frame goes
  out after the echo.
- **UTF-8 per read** (6.4.3, 6.4.4). Text is checked as its bytes arrive,
  so invalid UTF-8 fails the connection before the rest of the frame.

`UNIMPLEMENTED` would mean that nwws declined a permessage-deflate offer of
sections 12 and 13; nwws accepts every one of them:

- Sections 13.3 and 13.5 limit the compression window to 9 bits, below the
  32 KiB window that `compress/flate` always uses. nwws accepts the limit
  and compresses with Huffman coding only, which needs no window.
- The nwws client offers `client_max_window_bits`, so the fuzzingserver's
  section 13 accepts it at every window size it asks for.

## INFORMATIONAL cases

These cases have no required outcome:

- 7.1.6: the peer sends a 256 KiB message, a short message and a close
  frame back to back. Whether the echoes arrive before the close is up to
  the implementation; nwws echoes both.
- 7.13.1 and 7.13.2: close codes 5000 and 65535, which nwws answers with
  1002.
