# A plugin of SpelaionDB — rules for agents

An engine for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb): its driver, and a small
program that serves it to SpelaionDB over its plugin protocol. Apache-2.0.

**The rules of SpelaionDB's repository apply here** (its `AGENTS.md`: all the code is written
by an agent, the compiler, the tests and CI are the only reviewer; everything in English;
sign-off; one task, one short branch; commit subjects that read well in a changelog). This file
says only what is different in a plugin's repository. A plugin's own `AGENTS.md` starts from
this one and adds what is particular to its engine.

## What this is
- `src/`: the driver. It implements `Driver` and `Connection` of `spelaion-driver-api`
  (SpelaionDB's ADR-005): results in Arrow with `spelaion.logical` and `spelaion.native_type`
  metadata per column.
- `src/bin/spelaiondb-plugin-<engine>.rs`: the program. It hands the driver to
  `spelaion_plugin_sdk::run` and nothing else.
- `tests/`: the engine's own tests (its types against the engine's own text), and
  `tests/conformance.rs`: SpelaionDB's conformance set against this program, and `plugin.toml`
  checked against what the program says of itself.
- `plugin.toml`: the protocol version, the engines and the lowest SpelaionDB of this plugin.
- In this repository only: `.github/workflows/plugin-ci.yml` and `plugin-release.yml`, the
  workflows every plugin calls. A change to them reaches every plugin: it needs an owner.

## A plugin is the wire, not the rules
SpelaionDB's core splits, classifies and guards statements, keeps secrets, opens tunnels and
writes the audit (ADR-041 there). A plugin connects, runs what it is sent, cancels, introspects
and loads rows. So, and these are not negotiable (`docs/plugin-protocol.md` in SpelaionDB's
repository, "What a plugin must never do"):

- **Standard output is the protocol and nothing else.** Never `println!`. What must be said
  goes to standard error, a line at a time.
- **The password arrives in `connect` and is written nowhere**: not to a log, an error, a
  file or the environment.
- **It runs what it is sent.** It never decides whether a statement is a read, and never
  changes one.
- **Nothing is run again** after a connection is lost.
- **It ends when its standard input closes** (the SDK does this).
- It reads no configuration file, keychain or store of SpelaionDB's, and opens no tunnel: it
  connects to `connect_addr` when it is given one and verifies TLS against `host`.

## The contract comes from SpelaionDB, by tag
`spelaion-driver-api`, `spelaion-dialect`, `spelaion-plugin-sdk` and, for the tests,
`spelaion-plugin-host` are git dependencies on a tag of SpelaionDB's repository
(`plugin-sdk-vX.Y.Z`), all on the same tag. They are not on crates.io.

- **A protocol change is made there first** (the new version, with its tests), then here: move
  the tag, adapt, release. SpelaionDB then moves its pin.
- Arrow must stay the version SpelaionDB uses: batches cross between the two unchanged.
- `plugin.toml`'s `protocol` is what the SDK of that tag speaks; `tests/conformance.rs` fails
  if they differ.

## Releases
Pushing a tag `vX.Y.Z` (the version in `Cargo.toml`) runs the shared release: the four
platforms, each archive with its SHA-256, and notes that carry the pin in the form SpelaionDB's
`plugins.toml` wants. **Nothing is signed and no secret lives in a plugin's repository**: a
release reaches users only when SpelaionDB's repository pins its hashes in a pull request of
its own, whose CI runs SpelaionDB's tests through that exact build (ADR-042 there).

## Branches
`main` is protected: it changes only through pull requests whose CI is green, and cannot be
force-pushed or deleted. A pull request that touches a file in `.github/CODEOWNERS` waits for
an owner. The work is tracked in SpelaionDB's tracker (Plane, project `SDB`): write the item's
identifier in the pull request's description, never in a commit subject.

## Before calling something done
- `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`, `cargo test` (with the
  engine's server up, where it has one: a test skipped is not a test passed).
- `cargo deny check` if dependencies change.
- A change to how values are written is checked against SpelaionDB too: build the program and
  run SpelaionDB's tests through it (`SPELAION_DEV_PLUGIN_<ENGINE>=<the program>` with a
  development build there).
- If something could not be tested, say so.
