//! What the server holds, from each catalog's `information_schema`: catalogs are the explorer's
//! databases, then schemas, then tables. Names are written as quoted identifiers and string
//! literals (the protocol binds no values outside `EXECUTE`).
//!
//! Neither engine has primary keys or foreign keys: none is reported, so nothing pages or edits
//! rows by one.

use serde_json::Value as Json;
use spelaion_driver_api::{
    ColumnInfo, DriverError, ObjectInfo, ObjectKind, Result, SchemaColumn, SchemaInfo, TableInfo,
};

use crate::{TrinoConnection, literal, quote};

fn text(row: &[Json], i: usize) -> String {
    match row.get(i) {
        Some(Json::String(s)) => s.clone(),
        Some(Json::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

fn optional(row: &[Json], i: usize) -> Option<String> {
    Some(text(row, i)).filter(|s| !s.is_empty())
}

fn catalog_of(catalog: Option<&str>) -> Result<&str> {
    catalog.ok_or_else(|| DriverError::Other("no catalog is selected".into()))
}

pub async fn list_catalogs(conn: &TrinoConnection) -> Result<Vec<String>> {
    Ok(conn
        .rows("SHOW CATALOGS")
        .await?
        .iter()
        .map(|row| text(row, 0))
        .collect())
}

pub async fn list_schemas(
    conn: &TrinoConnection,
    catalog: Option<&str>,
) -> Result<Vec<SchemaInfo>> {
    let Some(catalog) = catalog else {
        return Ok(Vec::new());
    };
    let rows = conn
        .rows(&format!(
            "SELECT schema_name FROM {}.information_schema.schemata ORDER BY schema_name",
            quote(catalog)
        ))
        .await?;
    Ok(rows
        .iter()
        .map(|row| {
            let name = text(row, 0);
            SchemaInfo {
                system: name == "information_schema",
                name,
            }
        })
        .collect())
}

pub async fn list_objects(
    conn: &TrinoConnection,
    catalog: Option<&str>,
    schema: &str,
) -> Result<Vec<ObjectInfo>> {
    let catalog = catalog_of(catalog)?;
    let rows = conn
        .rows(&format!(
            "SELECT table_name, table_type FROM {}.information_schema.tables \
             WHERE table_schema = {} ORDER BY table_name",
            quote(catalog),
            literal(schema)
        ))
        .await?;
    Ok(rows
        .iter()
        .map(|row| ObjectInfo {
            schema: schema.to_string(),
            name: text(row, 0),
            kind: if text(row, 1) == "VIEW" {
                ObjectKind::View
            } else {
                ObjectKind::Table
            },
            estimated_rows: None,
            comment: None,
        })
        .collect())
}

pub async fn describe_table(
    conn: &TrinoConnection,
    catalog: Option<&str>,
    schema: &str,
    name: &str,
) -> Result<TableInfo> {
    let catalog = catalog_of(catalog)?;
    let objects = conn
        .rows(&format!(
            "SELECT table_type FROM {}.information_schema.tables \
             WHERE table_schema = {} AND table_name = {}",
            quote(catalog),
            literal(schema),
            literal(name)
        ))
        .await?;
    let Some(object) = objects.first() else {
        return Err(DriverError::Other(format!(
            "there is no table {catalog}.{schema}.{name}"
        )));
    };
    let columns = conn
        .rows(&format!(
            "SELECT column_name, data_type, is_nullable, column_default \
             FROM {}.information_schema.columns \
             WHERE table_schema = {} AND table_name = {} ORDER BY ordinal_position",
            quote(catalog),
            literal(schema),
            literal(name)
        ))
        .await?;
    Ok(TableInfo {
        object: ObjectInfo {
            schema: schema.to_string(),
            name: name.to_string(),
            kind: if text(object, 0) == "VIEW" {
                ObjectKind::View
            } else {
                ObjectKind::Table
            },
            estimated_rows: None,
            comment: None,
        },
        columns: columns
            .iter()
            .map(|row| ColumnInfo {
                name: text(row, 0),
                native_type: text(row, 1),
                nullable: text(row, 2) == "YES",
                default: optional(row, 3),
                comment: None,
            })
            .collect(),
        primary_key: Vec::new(),
        indexes: Vec::new(),
        foreign_keys: Vec::new(),
        constraints: Vec::new(),
    })
}

pub async fn list_columns(
    conn: &TrinoConnection,
    catalog: Option<&str>,
    schema: &str,
) -> Result<Vec<SchemaColumn>> {
    let catalog = catalog_of(catalog)?;
    let rows = conn
        .rows(&format!(
            "SELECT table_name, column_name, data_type FROM {}.information_schema.columns \
             WHERE table_schema = {} ORDER BY table_name, ordinal_position",
            quote(catalog),
            literal(schema)
        ))
        .await?;
    Ok(rows
        .iter()
        .map(|row| SchemaColumn {
            table: text(row, 0),
            name: text(row, 1),
            native_type: text(row, 2),
        })
        .collect())
}

pub async fn relation_definition(
    conn: &TrinoConnection,
    catalog: Option<&str>,
    schema: &str,
    name: &str,
) -> Result<String> {
    let catalog = catalog_of(catalog)?;
    let table = describe_table(conn, Some(catalog), schema, name).await?;
    let what = match table.object.kind {
        ObjectKind::View => "VIEW",
        ObjectKind::Table
        | ObjectKind::PartitionedTable
        | ObjectKind::MaterializedView
        | ObjectKind::ForeignTable
        | ObjectKind::Collection
        | ObjectKind::NodeLabel
        | ObjectKind::RelationshipType => "TABLE",
    };
    let rows = conn
        .rows(&format!(
            "SHOW CREATE {what} {}.{}.{}",
            quote(catalog),
            quote(schema),
            quote(name)
        ))
        .await?;
    rows.first()
        .and_then(|row| optional(row, 0))
        .map(|sql| spelaion_driver_api::terminated(&sql))
        .ok_or_else(|| DriverError::Other(format!("no definition for {catalog}.{schema}.{name}")))
}
