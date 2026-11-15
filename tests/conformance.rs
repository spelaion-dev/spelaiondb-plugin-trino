//! The conformance set of SpelaionDB's plugin protocol, run against this plugin's own program
//! (ADR-042), once per engine it serves: it starts, speaks the protocol's version, opens
//! sessions, returns results in the order the contract says, fails without hanging, obeys a
//! cancel while a statement runs and ends when its input closes. What the engines' types look
//! like is `tests/trino.rs`.
//!
//! It needs the test servers (`ci/services.sh`, `SPELAION_TEST_TRINO=127.0.0.1:18080`,
//! `SPELAION_TEST_PRESTO=127.0.0.1:18081`). Without a variable its engine is skipped, which
//! does not count as tested.

use spelaion_driver_api::{ConnectParams, TlsMode};
use spelaion_plugin_host::conformance::{Target, assert_conforms};

const PROGRAM: &str = env!("CARGO_BIN_EXE_spelaiondb-plugin-trino");

#[tokio::test]
async fn the_plugin_conforms() {
    for (variable, engine) in [
        ("SPELAION_TEST_TRINO", "trino"),
        ("SPELAION_TEST_PRESTO", "presto"),
    ] {
        let Ok(addr) = std::env::var(variable) else {
            eprintln!("{variable} not set: {engine} skipped");
            continue;
        };
        let (host, port) = addr.rsplit_once(':').expect("host:port");
        let target = Target {
            program: PROGRAM.into(),
            engine: engine.into(),
            params: ConnectParams {
                host: host.into(),
                port: port.parse().expect("a port"),
                user: "spelaion".into(),
                database: "memory.default".into(),
                tls: TlsMode::Disable,
                ..Default::default()
            },
            one_row: "select 1".into(),
            refused: "select * from a_table_that_is_not_there".into(),
            // A trillion rows to count, unless it is cancelled.
            slow: Some(
                "select count(*) from unnest(sequence(1, 10000)) a(x) \
                 cross join unnest(sequence(1, 10000)) b(y) cross join unnest(sequence(1, 10000)) c(z)"
                    .into(),
            ),
        };
        assert_conforms(&target).await;
    }
}

/// `plugin.toml` is what SpelaionDB's pin says of this plugin: it must be what the program
/// says of itself. The program says it without a server.
#[tokio::test]
async fn the_manifest_is_what_the_program_says() {
    let manifest = include_str!("../plugin.toml");
    let value = |key: &str| -> String {
        manifest
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{key} = ")))
            .unwrap_or_else(|| panic!("plugin.toml has no `{key}`"))
            .trim()
            .to_string()
    };
    let plugin = spelaion_plugin_host::Plugin::new(PROGRAM);
    let info = plugin.describe().await.expect("the program starts");

    assert_eq!(value("protocol"), format!("\"{}\"", info.protocol));
    let engines: Vec<String> = info.engines.iter().map(|e| format!("\"{e}\"")).collect();
    assert_eq!(value("engines"), format!("[{}]", engines.join(", ")));
    assert_eq!(info.plugin.name, env!("CARGO_PKG_NAME"));
    assert_eq!(info.plugin.version, env!("CARGO_PKG_VERSION"));
    let min_app = value("min_app");
    let numbers = min_app.trim_matches('"').split('.').count();
    assert_eq!(numbers, 3, "min_app = {min_app}");
}
