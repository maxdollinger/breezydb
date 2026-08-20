use std::fmt;

use serde_json::Value;

pub const NAME_LEN: usize = 64;
/// Byte length of the null flag each nullable field gets.
pub const NULL_FLAG_SIZE: u32 = 1;
/// Byte length of the actual-size prefix a String/Blob field gets on write.
pub const LEN_PREFIX_SIZE: u32 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Int,
    String,
    Bool,
    Float,
    Blob,
}

impl DataType {
    fn parse(decl: &str) -> Result<(Self, u32, bool), SchemaError> {
        let mut parts = decl.split_whitespace();
        let ty = parts.next().unwrap_or_default();

        let (base, nullable) = match ty.strip_suffix('?') {
            Some(t) => (t, true),
            None => (ty, false),
        };

        let base = match base.to_ascii_lowercase().as_str() {
            "int" => Self::Int,
            "string" => Self::String,
            "bool" => Self::Bool,
            "float" => Self::Float,
            "blob" => Self::Blob,
            _ => return Err(SchemaError::UnknownType(decl.to_string())),
        };

        match base {
            Self::Int | Self::Float | Self::Bool => {
                if parts.next().is_some() {
                    return Err(SchemaError::UnexpectedSize(decl.to_string()));
                }
                let size = if base == Self::Bool { 1 } else { 8 };
                Ok((base, size, nullable))
            }
            Self::String | Self::Blob => {
                let Some(n) = parts.next() else {
                    return Err(SchemaError::MissingSize(decl.to_string()));
                };
                if parts.next().is_some() {
                    return Err(SchemaError::InvalidSize(decl.to_string()));
                }
                let size: u32 = n
                    .parse()
                    .map_err(|_| SchemaError::InvalidSize(decl.to_string()))?;
                if size == 0 {
                    return Err(SchemaError::InvalidSize(decl.to_string()));
                }
                Ok((base, size, nullable))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: [u8; NAME_LEN],
    /// Byte offset of the field's first byte in a record. For String/Blob that
    /// is the 5-byte `[null, length]` prefix; for a nullable fixed-size field
    /// it is the 1-byte null flag.
    pub offset: u32,
    /// Content size in bytes, excluding any null flag or length prefix.
    pub size: u32,
    pub ty: DataType,
    pub nullable: bool,
}

impl Field {
    /// On-disk width of the field.
    ///
    /// String/Blob always carry a 5-byte `[null, length]` prefix regardless of
    /// nullability. Fixed-size fields only add the 1-byte null flag when
    /// nullable.
    pub fn width(&self) -> u32 {
        match self.ty {
            DataType::String | DataType::Blob => NULL_FLAG_SIZE + LEN_PREFIX_SIZE + self.size,
            _ => self.size + if self.nullable { NULL_FLAG_SIZE } else { 0 },
        }
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub name: [u8; NAME_LEN],
    pub version: u16,
    pub fields: Vec<Field>,
}

#[derive(Debug)]
pub enum SchemaError {
    NotAnObject,
    UnknownType(String),
    MissingSize(String),
    InvalidSize(String),
    UnexpectedSize(String),
    NameTooLong(String),
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAnObject => write!(f, "schema must be a JSON object"),
            Self::UnknownType(t) => write!(f, "unknown field type: {t}"),
            Self::MissingSize(t) => write!(f, "String/Blob need a length: {t}"),
            Self::InvalidSize(t) => write!(f, "invalid field length: {t}"),
            Self::UnexpectedSize(t) => write!(f, "fixed-size type takes no length: {t}"),
            Self::NameTooLong(n) => write!(f, "field name longer than {NAME_LEN} bytes: {n}"),
        }
    }
}

impl std::error::Error for SchemaError {}

/// Build a [`Schema`] from a JSON object.
///
/// Values are `"Type"` (Int/Float/Bool), `"Type N"` (String/Blob), and a
/// trailing `?` marks a field nullable (`"Int?"`, `"String 64?"`). Nested
/// objects flatten into dotted field names:
///
/// ```json
/// { "name": { "first": "String 64", "last": "String 64" }, "id": "Int" }
/// ```
///
/// yields fields `name.first`, `name.last`, `id`.
pub fn schema_from_json(name: &str, value: &Value) -> Result<Schema, SchemaError> {
    let mut fields = Vec::new();
    collect_fields("", value, &mut fields)?;

    Ok(Schema {
        name: encode_name(name)?,
        version: 0,
        fields,
    })
}

fn collect_fields(prefix: &str, value: &Value, fields: &mut Vec<Field>) -> Result<(), SchemaError> {
    let obj = value.as_object().ok_or(SchemaError::NotAnObject)?;

    let mut offset = 0u32;

    for (key, val) in obj {
        let full = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };

        match val {
            Value::Object(_) => collect_fields(&full, val, fields)?,
            Value::String(decl) => {
                let (ty, size, nullable) = DataType::parse(decl)?;
                fields.push(Field {
                    name: encode_name(&full)?,
                    offset,
                    size,
                    ty,
                    nullable,
                });
                offset += fields.last().unwrap().width();
            }
            _ => return Err(SchemaError::NotAnObject),
        }
    }

    Ok(())
}

fn encode_name(s: &str) -> Result<[u8; NAME_LEN], SchemaError> {
    let b = s.as_bytes();
    if b.len() > NAME_LEN {
        return Err(SchemaError::NameTooLong(s.to_string()));
    }
    let mut out = [0u8; NAME_LEN];
    out[..b.len()].copy_from_slice(b);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flattens_nested() {
        let v: Value = serde_json::from_str(
            r#"{ "name": { "first": "String 64", "last": "String 64" }, "id": "Int" }"#,
        )
        .unwrap();

        let s = schema_from_json("user", &v).unwrap();

        let names: Vec<String> = s
            .fields
            .iter()
            .map(|f| {
                String::from_utf8_lossy(&f.name)
                    .trim_end_matches('\0')
                    .to_string()
            })
            .collect();

        assert_eq!(names, ["id", "name.first", "name.last"]);
    }

    #[test]
    fn sized_types() {
        let v: Value = serde_json::from_str(
            r#"{ "firstName": "String 50", "userId": "Int", "ok": "Bool", "ratio": "Float", "data": "Blob 12" }"#,
        )
        .unwrap();

        let s = schema_from_json("user", &v).unwrap();

        let by_name = |n: &str| {
            s.fields
                .iter()
                .find(|f| String::from_utf8_lossy(&f.name).trim_end_matches('\0') == n)
                .unwrap()
        };

        let first = by_name("firstName");
        assert_eq!(first.ty, DataType::String);
        assert_eq!(first.size, 50);
        assert_eq!(first.width(), NULL_FLAG_SIZE + LEN_PREFIX_SIZE + 50);

        assert_eq!(by_name("userId").size, 8);
        assert_eq!(by_name("ok").size, 1);
        assert_eq!(by_name("ratio").size, 8);
        assert_eq!(by_name("data").size, 12);
    }

    #[test]
    fn nullable_types() {
        let v: Value = serde_json::from_str(
            r#"{ "a": "Int?", "b": "Bool?", "c": "String? 50", "d": "String 50" }"#,
        )
        .unwrap();

        let s = schema_from_json("user", &v).unwrap();

        let by_name = |n: &str| {
            s.fields
                .iter()
                .find(|f| String::from_utf8_lossy(&f.name).trim_end_matches('\0') == n)
                .unwrap()
        };

        let a = by_name("a");
        assert!(a.nullable);
        assert_eq!(a.width(), 1 + 8);

        let b = by_name("b");
        assert!(b.nullable);
        assert_eq!(b.width(), 1 + 1);

        let c = by_name("c");
        assert!(c.nullable);
        assert_eq!(c.width(), NULL_FLAG_SIZE + LEN_PREFIX_SIZE + 50);

        let d = by_name("d");
        assert!(!d.nullable);
        assert_eq!(d.width(), NULL_FLAG_SIZE + LEN_PREFIX_SIZE + 50);
    }

    #[test]
    fn string_requires_size() {
        let v: Value = serde_json::from_str(r#"{ "name": "String" }"#).unwrap();
        assert!(matches!(
            schema_from_json("user", &v),
            Err(SchemaError::MissingSize(_))
        ));
    }

    #[test]
    fn nullable_string_requires_size() {
        let v: Value = serde_json::from_str(r#"{ "name": "String?" }"#).unwrap();
        assert!(matches!(
            schema_from_json("user", &v),
            Err(SchemaError::MissingSize(_))
        ));
    }

    #[test]
    fn string_rejects_zero() {
        let v: Value = serde_json::from_str(r#"{ "name": "String 0" }"#).unwrap();
        assert!(matches!(
            schema_from_json("user", &v),
            Err(SchemaError::InvalidSize(_))
        ));
    }

    #[test]
    fn fixed_types_reject_suffix() {
        for decl in ["Int 64", "Float 32", "Bool 1"] {
            let v: Value = serde_json::from_str(&format!(r#"{{ "f": "{decl}" }}"#)).unwrap();
            assert!(matches!(
                schema_from_json("user", &v),
                Err(SchemaError::UnexpectedSize(_))
            ));
        }
    }

    #[test]
    fn rejects_timestamp() {
        let v: Value = serde_json::from_str(r#"{ "t": "TimeStamp" }"#).unwrap();
        assert!(matches!(
            schema_from_json("user", &v),
            Err(SchemaError::UnknownType(_))
        ));
    }

    #[test]
    fn offsets_account_for_prefix() {
        let v: Value =
            serde_json::from_str(r#"{ "id": "Int", "name": "String 50", "flag": "Bool" }"#)
                .unwrap();

        let s = schema_from_json("user", &v).unwrap();

        let by_name = |n: &str| {
            s.fields
                .iter()
                .find(|f| String::from_utf8_lossy(&f.name).trim_end_matches('\0') == n)
                .unwrap()
        };

        assert_eq!(by_name("flag").offset, 0);
        assert_eq!(by_name("id").offset, 1);
        assert_eq!(by_name("name").offset, 1 + 8);
        assert_eq!(
            by_name("name").width(),
            NULL_FLAG_SIZE + LEN_PREFIX_SIZE + 50
        );
    }
}
