# spelaiondb-plugin-template

What a plugin of [SpelaionDB](https://github.com/spelaion-dev/spelaiondb) starts from, and the
checks and the release every plugin of ours shares.

SpelaionDB's engines are programs it starts and talks to over their standard input and output
(its ADR-041). Each of ours lives in its own repository, `spelaiondb-plugin-<engine>`, and
SpelaionDB pins by hash the exact release it was tested with (its ADR-042). This repository is
two things:

- **A plugin to copy**: a made-up engine, `example`, with everything a plugin's repository
  has: the driver (`src/lib.rs`), the program (`src/bin/`), the conformance test, `plugin.toml`,
  the rules (`AGENTS.md`).
- **The shared workflows**: `.github/workflows/plugin-ci.yml` and `plugin-release.yml`. Every
  plugin calls them by commit, so a change to how plugins are checked or released is made
  once, here.

Protocol: [SpelaionDB's plugin protocol](https://github.com/spelaion-dev/spelaiondb/blob/dev/docs/plugin-protocol.md),
version 1.0, experimental until SpelaionDB 1.0. License: Apache-2.0.

## Creating a plugin's repository

Creating the repository and protecting its branch is done by an owner of the organisation, or
with their yes each time. A plugin's repository is created when its engine's work starts, not
before.

1. **Create `spelaion-dev/spelaiondb-plugin-<engine>` from this template** (public, Apache-2.0).
2. **Rename.** `spelaiondb-plugin-example` → `spelaiondb-plugin-<engine>` in `Cargo.toml` (the
   package, the binary and its path), in `src/bin/`, in the tests and in `README.md`; the library
   (`spelaion_driver_example`) to your driver's name; `engines` in `plugin.toml`.
3. **Put your driver in `src/`**, with its own tests in `tests/`. A driver that moves out of
   SpelaionDB's repository comes whole, with its tests, and with the patched library it uses
   (`vendor/<library>`, its `PATCHED.md`, and the `[patch.crates-io]` entry), which is then under
   this repository's `CODEOWNERS`.
4. **Point the two workflows at the shared ones**, by commit:

   ```yaml
   # .github/workflows/ci.yml
   jobs:
     ci:
       uses: spelaion-dev/spelaiondb-plugin-template/.github/workflows/plugin-ci.yml@<commit>
       with:
         test-env: |
           SPELAION_TEST_PG=127.0.0.1:15432

   # .github/workflows/release.yml
   jobs:
     release:
       permissions:
         contents: write
       uses: spelaion-dev/spelaiondb-plugin-template/.github/workflows/plugin-release.yml@<commit>
   ```

   and delete `plugin-ci.yml` and `plugin-release.yml` from the new repository: they live here.
5. **If the tests need a server**, add an executable `ci/services.sh` that starts it (Docker
   Compose, waiting until it is healthy) and name where it is in `test-env`. A test that needs
   a server and does not find its variable is skipped and says so, as in SpelaionDB.
   A patched library under `vendor/` is a crate of its own, which `cargo test` does not reach:
   run its tests from an executable `ci/more-tests.sh`.
6. **Protect `main`**: pull requests only, the checks of `CI` required, no force-push, no
   deletion, owners' review for the files in `.github/CODEOWNERS`.
7. **No secret, ever.** Nothing here signs anything: a release reaches users only when
   SpelaionDB's repository pins its hashes.

## Releasing a plugin

Push a tag `vX.Y.Z` that is the version in `Cargo.toml`. The release builds the four platforms
and its notes carry the pin for SpelaionDB's `plugins.toml`. Moving that pin is a pull request
in SpelaionDB's repository, whose CI runs SpelaionDB's tests through that exact build.

## Working on it

```bash
cargo test
```

runs the protocol's conformance set against the example plugin's own program. To try a plugin
with SpelaionDB, point a development build of it at the program
(`SPELAION_DEV_PLUGIN_<ENGINE>=/path/to/the/program`, in SpelaionDB's repository; a release
build ignores that variable).

`AGENTS.md` has the rules. Reports and questions go to
[SpelaionDB's issues](https://github.com/spelaion-dev/spelaiondb/issues).
