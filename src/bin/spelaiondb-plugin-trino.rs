//! The plugin's program: Trino's and Presto's driver, served to SpelaionDB over its plugin
//! protocol on the program's standard input and output (SpelaionDB's ADR-041).
//!
//! It does nothing else. Standard output is the protocol: nothing here, and nothing in the
//! driver, ever prints to it.

use std::sync::Arc;

use spelaion_driver_trino::TrinoDriver;
use spelaion_plugin_sdk::{Plugin, run};

#[tokio::main]
async fn main() {
    run(Plugin::new(
        "spelaiondb-plugin-trino",
        env!("CARGO_PKG_VERSION"),
        vec![
            Arc::new(TrinoDriver::trino()),
            Arc::new(TrinoDriver::presto()),
        ],
    ))
    .await
}
