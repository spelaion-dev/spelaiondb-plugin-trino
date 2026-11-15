# spelaiondb-plugin-trino

Trino and Presto for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb), the desktop
database client: one driver for both engines, served to SpelaionDB as a plugin.

SpelaionDB's engines are programs it starts and talks to over their standard input and output
(its ADR-041). This one serves `trino` and `presto`, which speak the same HTTP protocol (Presto
is where Trino came from). It is built here, in its own repository, and SpelaionDB pins by hash
the exact release it was tested with (its ADR-042, ADR-055).

- **Protocol:** [SpelaionDB's plugin protocol](https://github.com/spelaion-dev/spelaiondb/blob/dev/docs/plugin-protocol.md),
  version 1.1. Experimental until SpelaionDB 1.0.
- **Engines:** `trino` and `presto`, over their HTTP client protocol (port 8080), with `hyper`
  and rustls.
- **License:** Apache-2.0.

## Working on it

```bash
ci/services.sh
```

starts the test servers (Trino 479 and Presto 0.295 in Docker), and

```bash
SPELAION_TEST_TRINO=127.0.0.1:18080 SPELAION_TEST_PRESTO=127.0.0.1:18081 cargo test
```

runs the type zoo and the protocol's conformance set against this plugin's own program, on
both. Without the variables the tests that need a server are skipped, which does not count as
tested.

To try a build with SpelaionDB, point a development build of it at the program
(`SPELAION_DEV_PLUGIN_TRINO` and `SPELAION_DEV_PLUGIN_PRESTO`, in SpelaionDB's repository; a
release build ignores those variables).

`AGENTS.md` has the rules. Reports and questions go to
[SpelaionDB's issues](https://github.com/spelaion-dev/spelaiondb/issues).
