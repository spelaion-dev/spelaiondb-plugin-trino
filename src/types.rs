//! Trino's and Presto's types as SpelaionDB's logical types, and a result's rows built from the
//! values the protocol sends (JSON, typed by the column).
//!
//! - `boolean` is `Bool`; `tinyint` to `bigint` are `Int`; `real` and `double` are `Float`
//!   (the protocol sends a number, or `"NaN"`, `"Infinity"`, `"-Infinity"`);
//! - `decimal(p, s)` is `Decimal`, kept as the server's text;
//! - `varchar` and `char` are `Text`; `varbinary` is `Bytes` (sent in base64);
//! - `date` is `Date`; `uuid` is `Uuid`; `json` is `Json` (the protocol sends its text);
//! - `array(…)` is `Array`; everything else (`time`, `timestamp`, intervals, `map`, `row`,
//!   `ipaddress`…) is `Native`. A value sent as a string is that string; anything else
//!   (arrays, maps, rows) is written as the protocol's JSON.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder,
    RecordBatch, StringBuilder,
};
use serde_json::Value as Json;
use spelaion_driver_api::{ColumnMeta, DriverError, LogicalType, Result, ResultSchema};

/// The base name of a type: `varchar(25)` → `varchar`, `timestamp(3) with time zone` →
/// `timestamp with time zone`.
fn base(native: &str) -> String {
    let lower = native.trim().to_ascii_lowercase();
    match lower.find('(') {
        Some(open) => {
            let close = lower.rfind(')').unwrap_or(lower.len() - 1);
            let rest = lower.get(close + 1..).unwrap_or("").trim();
            let head = lower[..open].trim();
            if rest.is_empty() {
                head.to_string()
            } else {
                format!("{head} {rest}")
            }
        }
        None => lower,
    }
}

/// The logical type of a type, as the protocol names it.
pub fn logical(native: &str) -> LogicalType {
    match base(native).as_str() {
        "boolean" => LogicalType::Bool,
        "tinyint" | "smallint" | "integer" | "bigint" => LogicalType::Int,
        "real" | "double" => LogicalType::Float,
        "decimal" => LogicalType::Decimal,
        "varchar" | "char" => LogicalType::Text,
        "varbinary" => LogicalType::Bytes,
        "date" => LogicalType::Date,
        "uuid" => LogicalType::Uuid,
        "json" => LogicalType::Json,
        "array" => LogicalType::Array,
        _ => LogicalType::Native,
    }
}

pub fn column(name: String, native_type: String) -> ColumnMeta {
    ColumnMeta {
        name,
        logical: logical(&native_type),
        native_type,
        native_id: None,
        origin: None,
    }
}

enum Builder {
    Bool(BooleanBuilder),
    Int(Int64Builder),
    Float(Float64Builder),
    Date(Date32Builder),
    Bytes(BinaryBuilder),
    Text(StringBuilder),
}

/// The rows of one result, a batch at a time.
pub struct Batches {
    schema: Arc<ResultSchema>,
    builders: Vec<Builder>,
    rows: usize,
}

impl Batches {
    pub fn new(schema: Arc<ResultSchema>) -> Self {
        let builders = fresh(&schema);
        Self {
            schema,
            builders,
            rows: 0,
        }
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    /// One row as the protocol sends it.
    pub fn push(&mut self, row: &[Json]) -> Result<()> {
        if row.len() != self.builders.len() {
            return Err(DriverError::Other(format!(
                "a row of {} values in a result of {} columns",
                row.len(),
                self.builders.len()
            )));
        }
        for ((builder, value), column) in
            self.builders.iter_mut().zip(row).zip(&self.schema.columns)
        {
            let bad = || DriverError::Decode {
                type_name: column.native_type.clone(),
                message: format!("{value} is not a value of this type"),
            };
            if value.is_null() {
                match builder {
                    Builder::Bool(b) => b.append_null(),
                    Builder::Int(b) => b.append_null(),
                    Builder::Float(b) => b.append_null(),
                    Builder::Date(b) => b.append_null(),
                    Builder::Bytes(b) => b.append_null(),
                    Builder::Text(b) => b.append_null(),
                }
                continue;
            }
            match builder {
                Builder::Bool(b) => b.append_value(value.as_bool().ok_or_else(bad)?),
                Builder::Int(b) => b.append_value(value.as_i64().ok_or_else(bad)?),
                Builder::Float(b) => b.append_value(match value {
                    Json::Number(n) => n.as_f64().ok_or_else(bad)?,
                    Json::String(s) => match s.as_str() {
                        "NaN" => f64::NAN,
                        "Infinity" => f64::INFINITY,
                        "-Infinity" => f64::NEG_INFINITY,
                        _ => return Err(bad()),
                    },
                    _ => return Err(bad()),
                }),
                Builder::Date(b) => {
                    b.append_value(days(value.as_str().ok_or_else(bad)?).ok_or_else(bad)?)
                }
                Builder::Bytes(b) => b.append_value(
                    crate::http::unbase64(value.as_str().ok_or_else(bad)?).ok_or_else(bad)?,
                ),
                Builder::Text(b) => match value {
                    Json::String(s) => b.append_value(s),
                    other => b.append_value(other.to_string()),
                },
            }
        }
        self.rows += 1;
        Ok(())
    }

    /// The rows pushed since the last batch, as one batch; `None` when there are none.
    pub fn take(&mut self) -> Result<Option<RecordBatch>> {
        if self.rows == 0 {
            return Ok(None);
        }
        let builders = std::mem::replace(&mut self.builders, fresh(&self.schema));
        self.rows = 0;
        let arrays: Vec<ArrayRef> = builders
            .into_iter()
            .map(|builder| -> ArrayRef {
                match builder {
                    Builder::Bool(mut b) => Arc::new(b.finish()),
                    Builder::Int(mut b) => Arc::new(b.finish()),
                    Builder::Float(mut b) => Arc::new(b.finish()),
                    Builder::Date(mut b) => Arc::new(b.finish()),
                    Builder::Bytes(mut b) => Arc::new(b.finish()),
                    Builder::Text(mut b) => Arc::new(b.finish()),
                }
            })
            .collect();
        RecordBatch::try_new(self.schema.arrow.clone(), arrays)
            .map(Some)
            .map_err(|e| DriverError::Other(format!("building a batch: {e}")))
    }
}

fn fresh(schema: &ResultSchema) -> Vec<Builder> {
    schema
        .columns
        .iter()
        .map(|column| match column.logical {
            LogicalType::Bool => Builder::Bool(BooleanBuilder::new()),
            LogicalType::Int => Builder::Int(Int64Builder::new()),
            LogicalType::Float => Builder::Float(Float64Builder::new()),
            LogicalType::Date => Builder::Date(Date32Builder::new()),
            LogicalType::Bytes => Builder::Bytes(BinaryBuilder::new()),
            LogicalType::Decimal
            | LogicalType::Text
            | LogicalType::Time
            | LogicalType::Timestamp
            | LogicalType::TimestampTz
            | LogicalType::Uuid
            | LogicalType::Json
            | LogicalType::Interval
            | LogicalType::Array
            | LogicalType::Native => Builder::Text(StringBuilder::new()),
        })
        .collect()
}

/// Days since 1970-01-01 of `YYYY-MM-DD` (a year may have more digits or a sign).
fn days(text: &str) -> Option<i32> {
    let (sign, rest) = match text.strip_prefix('-') {
        Some(rest) => (-1, rest),
        None => (1, text),
    };
    let mut parts = rest.splitn(3, '-');
    let y: i64 = sign * parts.next()?.parse::<i64>().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let d: i64 = parts.next()?.parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    i32::try_from(era * 146_097 + doe - 719_468).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn types_map_to_logical_types() {
        for (native, logical_type) in [
            ("boolean", LogicalType::Bool),
            ("bigint", LogicalType::Int),
            ("real", LogicalType::Float),
            ("decimal(10,2)", LogicalType::Decimal),
            ("varchar(25)", LogicalType::Text),
            ("varchar", LogicalType::Text),
            ("char(3)", LogicalType::Text),
            ("varbinary", LogicalType::Bytes),
            ("date", LogicalType::Date),
            ("uuid", LogicalType::Uuid),
            ("json", LogicalType::Json),
            ("array(integer)", LogicalType::Array),
            ("timestamp(3) with time zone", LogicalType::Native),
            ("map(varchar, integer)", LogicalType::Native),
            ("row(a integer, b varchar)", LogicalType::Native),
            ("interval day to second", LogicalType::Native),
        ] {
            assert_eq!(logical(native), logical_type, "{native}");
        }
    }

    #[test]
    fn dates_count_days_from_1970() {
        assert_eq!(days("1970-01-01"), Some(0));
        assert_eq!(days("2000-03-01"), Some(11_017));
        assert_eq!(days("1969-12-31"), Some(-1));
        assert_eq!(days("x"), None);
    }

    #[test]
    fn rows_build_from_json() {
        let schema = Arc::new(ResultSchema::new(vec![
            column("b".into(), "boolean".into()),
            column("n".into(), "bigint".into()),
            column("f".into(), "double".into()),
            column("d".into(), "date".into()),
            column("v".into(), "varbinary".into()),
            column("a".into(), "array(integer)".into()),
        ]));
        let mut batches = Batches::new(schema);
        let row: Vec<Json> =
            serde_json::from_str(r#"[true, 7, "NaN", "2024-02-29", "AAE=", [1, null]]"#).unwrap();
        batches.push(&row).unwrap();
        let nulls: Vec<Json> =
            serde_json::from_str("[null, null, null, null, null, null]").unwrap();
        batches.push(&nulls).unwrap();
        let batch = batches.take().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let bad: Vec<Json> =
            serde_json::from_str(r#"[1, 7, 1.5, "2024-02-29", "AAE=", []]"#).unwrap();
        assert!(batches.push(&bad).is_err());
    }
}
