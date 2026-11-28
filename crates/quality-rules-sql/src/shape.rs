//! What a field's profile takes its shape from, by the field's type, the
//! same on DuckDB and Trino (decision 0090, rulings 26 and 27): a text, a
//! date or a boolean by its text cast; an integer or a decimal by its text
//! cast, a `DECIMAL(p, p)` widened to `DECIMAL(p + 1, p)` first, since the
//! engines write its leading zero differently; a floating-point number by
//! nothing, as no text cast writes a double alike on both engines.

/// The widest decimal both engines take.
const MAX_PRECISION: u8 = 38;

/// A field's type, as far as its shape goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShapeKind {
    /// Texts, dates and booleans: their text cast.
    Text,
    Integer,
    Decimal {
        precision: u8,
        scale: u8,
    },
    /// `double` and `real`: no shape.
    Float,
}

impl ShapeKind {
    /// The kind of a column of type `ty`, as DuckDB or Trino name it
    /// (`BIGINT`, `bigint`, `DECIMAL(9,2)`, `decimal(9, 2)`, `DOUBLE`,
    /// `real`, `FLOAT`); any other type is text.
    pub fn of_type(ty: &str) -> Self {
        let t = ty.trim().to_ascii_lowercase();
        match t.as_str() {
            "tinyint" | "smallint" | "integer" | "int" | "bigint" | "hugeint" | "utinyint"
            | "usmallint" | "uinteger" | "ubigint" | "uhugeint" => Self::Integer,
            "double" | "real" | "float" => Self::Float,
            _ => decimal(&t).unwrap_or(Self::Text),
        }
    }

    /// The text `value` (an SQL expression) gives its shape from, or
    /// `None` when the field has no shape.
    pub fn text(self, value: &str) -> Option<String> {
        match self {
            Self::Float => None,
            Self::Decimal { precision, scale }
                if precision == scale && precision < MAX_PRECISION =>
            {
                Some(format!(
                    "CAST(CAST({value} AS DECIMAL({}, {scale})) AS VARCHAR)",
                    precision + 1
                ))
            }
            // A `DECIMAL(38, 38)` cannot widen: no shape, as a float
            // (decision 0090, ruling 33).
            Self::Decimal { precision, scale } if precision == scale => None,
            _ => Some(format!("CAST({value} AS VARCHAR)")),
        }
    }
}

/// `decimal(p, s)` or `numeric(p, s)`, as both engines describe a column.
fn decimal(t: &str) -> Option<ShapeKind> {
    let rest = t
        .strip_prefix("decimal")
        .or_else(|| t.strip_prefix("numeric"))?
        .trim();
    let inner = rest.strip_prefix('(')?.strip_suffix(')')?;
    let (p, s) = inner.split_once(',').unwrap_or((inner, "0"));
    Some(ShapeKind::Decimal {
        precision: p.trim().parse().ok()?,
        scale: s.trim().parse().ok()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn types_read_as_both_engines_name_them() {
        assert_eq!(ShapeKind::of_type("BIGINT"), ShapeKind::Integer);
        assert_eq!(ShapeKind::of_type("integer"), ShapeKind::Integer);
        assert_eq!(ShapeKind::of_type("DOUBLE"), ShapeKind::Float);
        assert_eq!(ShapeKind::of_type("real"), ShapeKind::Float);
        assert_eq!(ShapeKind::of_type("FLOAT"), ShapeKind::Float);
        let d = |precision, scale| ShapeKind::Decimal { precision, scale };
        assert_eq!(ShapeKind::of_type("DECIMAL(9,2)"), d(9, 2));
        assert_eq!(ShapeKind::of_type("decimal(4, 4)"), d(4, 4));
        assert_eq!(ShapeKind::of_type("decimal(5)"), d(5, 0));
        for t in ["VARCHAR", "varchar(20)", "DATE", "boolean", "decimalish"] {
            assert_eq!(ShapeKind::of_type(t), ShapeKind::Text, "{t}");
        }
    }

    #[test]
    fn a_decimal_without_integer_digits_widens_and_a_float_has_no_text() {
        let d = |precision, scale| ShapeKind::Decimal { precision, scale };
        assert_eq!(
            d(4, 4).text("x").unwrap(),
            "CAST(CAST(x AS DECIMAL(5, 4)) AS VARCHAR)"
        );
        assert_eq!(d(9, 2).text("x").unwrap(), "CAST(x AS VARCHAR)");
        assert_eq!(ShapeKind::Integer.text("x").unwrap(), "CAST(x AS VARCHAR)");
        assert_eq!(ShapeKind::Float.text("x"), None);
        assert_eq!(d(38, 38).text("x"), None);
    }
}
