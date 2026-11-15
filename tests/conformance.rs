//! The conformance set of SpelaionDB's plugin protocol, run against this plugin's own program
//! (ADR-042): it starts, speaks the protocol's version, opens sessions, returns results in the
//! order the contract says, fails without hanging, obeys a cancel while a statement runs and
//! ends when its input closes. What the engine's types look like is the engine's own tests.

use spelaion_driver_api::ConnectParams;
use spelaion_plugin_host::conformance::{Target, assert_conforms};

const PROGRAM: &str = env!("CARGO_BIN_EXE_spelaiondb-plugin-example");

#[tokio::test]
async fn the_plugin_conforms() {
    let target = Target {
        program: PROGRAM.into(),
        engine: "example".into(),
        // A plugin of a server reads where it is from the environment, as SpelaionDB's own
        // tests do, and skips this test (saying so) when it is not set.
        params: ConnectParams {
            database: "main".into(),
            ..Default::default()
        },
        one_row: "select 1".into(),
        refused: "select * from a_table_that_is_not_there".into(),
        // A statement that runs until it is cancelled; `None` for an engine that cannot cancel.
        slow: Some("wait".into()),
    };
    assert_conforms(&target).await;
}

/// `plugin.toml` is what SpelaionDB's pin says of this plugin: it must be what the program
/// says of itself.
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
    // Three numbers, as SpelaionDB's `plugins.toml` wants.
    let min_app = value("min_app");
    let numbers = min_app.trim_matches('"').split('.').count();
    assert_eq!(numbers, 3, "min_app = {min_app}");
}
