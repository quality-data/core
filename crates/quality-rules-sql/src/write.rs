//! Writes the tree node by node. Where the engines differ (verified
//! 2026-10-30, DuckDB 2.0.0 and Trino 484) the writer says so: DuckDB has
//! no `regexp_like` and replaces only the first match unless told `'g'`;
//! both take `trim(BOTH … FROM …)` (each names its set: their one-argument
//! `trim` differ, decision 0090 T1), `INTERVAL '18' YEAR`, `TRY_CAST`,
//! `date_diff`, windows and `?` parameters the same way.

use quality_rules_tree::{BinaryOp, Expr, Function, IntervalUnit, Literal, Type};

use super::{formats, quote_ident, quote_text, Cut, Engine, Error, Sql, Tables};

/// The characters `trim` and `is_empty` remove: the space and U+00A0.
const BLANKS: &str = "concat(' ', chr(160))";

pub(super) struct Writer<'a> {
    engine: Engine,
    tables: &'a Tables,
    /// Written in as a literal when given; else each `:cut` is a `?`.
    cut: Option<&'a Cut>,
    out: String,
    cut_params: usize,
}

impl<'a> Writer<'a> {
    pub(super) fn new(engine: Engine, tables: &'a Tables, cut: Option<&'a Cut>) -> Self {
        Self {
            engine,
            tables,
            cut,
            out: String::new(),
            cut_params: 0,
        }
    }

    pub(super) fn finish(self) -> Sql {
        Sql {
            text: self.out,
            cut_params: self.cut_params,
        }
    }

    fn push(&mut self, s: &str) {
        self.out.push_str(s);
    }

    fn list(&mut self, items: &[Expr]) -> Result<(), Error> {
        for (i, e) in items.iter().enumerate() {
            if i > 0 {
                self.push(", ");
            }
            self.expr(e)?;
        }
        Ok(())
    }

    /// `x` as text, for the functions on text.
    fn text_of(&mut self, e: &Expr) -> Result<(), Error> {
        self.push("CAST(");
        self.expr(e)?;
        self.push(" AS VARCHAR)");
        Ok(())
    }

    pub(super) fn expr(&mut self, e: &Expr) -> Result<(), Error> {
        match e {
            Expr::Field(name) => self.push(&quote_ident(name)?),
            Expr::Literal(l) => self.literal(l)?,
            Expr::Cut => match self.cut {
                Some(cut) => self.push(&cut.literal()),
                None => {
                    self.cut_params += 1;
                    self.push("CAST(? AS DATE)");
                }
            },
            Expr::Not(x) => self.wrap("(NOT ", x, ")")?,
            Expr::Negate(x) => self.wrap("(-", x, ")")?,
            Expr::Binary { op, left, right } => self.binary(*op, left, right)?,
            Expr::IsNull { expr, negated } => {
                self.wrap(
                    "(",
                    expr,
                    if *negated {
                        " IS NOT NULL)"
                    } else {
                        " IS NULL)"
                    },
                )?;
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => self.in_list(expr, list, *negated)?,
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                self.wrap(
                    "(",
                    expr,
                    if *negated {
                        " NOT BETWEEN "
                    } else {
                        " BETWEEN "
                    },
                )?;
                self.wrap("", low, " AND ")?;
                self.wrap("", high, ")")?;
            }
            Expr::Like {
                expr,
                pattern,
                negated,
            } => {
                self.wrap("(", expr, if *negated { " NOT LIKE " } else { " LIKE " })?;
                let pattern = quote_text(pattern)?;
                self.push(&format!("{pattern} ESCAPE '\\')"));
            }
            Expr::Case {
                operand,
                branches,
                otherwise,
            } => self.case(operand.as_deref(), branches, otherwise.as_deref())?,
            Expr::Cast { expr, to } => {
                self.wrap("TRY_CAST(", expr, &format!(" AS {})", type_name(*to)))?;
            }
            Expr::Interval { amount, unit } => {
                self.push(&format!("INTERVAL '{amount}' {}", unit_name(*unit)))
            }
            Expr::Call { function, args } => self.call(*function, args)?,
            Expr::Matches { expr, format } => self.regex(expr, formats::regex(*format))?,
            Expr::Normalized(x) => {
                self.push("upper(regexp_replace(");
                self.text_of(x)?;
                self.push(match self.engine {
                    Engine::DuckDb => r", '[^\p{L}\p{N}]', '', 'g'))",
                    Engine::Trino => r", '[^\p{L}\p{N}]', ''))",
                });
            }
            Expr::Duplicated(key) => {
                self.push("(count(*) OVER (PARTITION BY ");
                self.list(key)?;
                self.push(") > 1)");
            }
        }
        Ok(())
    }

    fn wrap(&mut self, before: &str, e: &Expr, after: &str) -> Result<(), Error> {
        self.push(before);
        self.expr(e)?;
        self.push(after);
        Ok(())
    }

    fn literal(&mut self, l: &Literal) -> Result<(), Error> {
        let s = match l {
            Literal::Text(t) => quote_text(t)?,
            Literal::Number(n) if is_number(n) => n.clone(),
            Literal::Number(n) => return Err(Error::BadNumber(n.clone())),
            Literal::Boolean(b) => (if *b { "TRUE" } else { "FALSE" }).to_string(),
            Literal::Null => "NULL".to_string(),
            Literal::Date(d) if is_date(d) => format!("DATE '{d}'"),
            Literal::Date(d) => return Err(Error::BadDate(d.clone())),
        };
        self.push(&s);
        Ok(())
    }

    fn binary(&mut self, op: BinaryOp, left: &Expr, right: &Expr) -> Result<(), Error> {
        if op == BinaryOp::Divide {
            // Trino divides integers as integers; both divide doubles alike.
            self.wrap("(CAST(", left, " AS DOUBLE) / ")?;
            return self.wrap("", right, ")");
        }
        self.wrap("(", left, &format!(" {} ", op_sql(op)))?;
        self.wrap("", right, ")")
    }

    fn in_list(&mut self, expr: &Expr, list: &[Expr], negated: bool) -> Result<(), Error> {
        if list.is_empty() {
            self.push(if negated { "TRUE" } else { "FALSE" });
            return Ok(());
        }
        self.wrap("(", expr, if negated { " NOT IN (" } else { " IN (" })?;
        self.list(list)?;
        self.push("))");
        Ok(())
    }

    fn case(
        &mut self,
        operand: Option<&Expr>,
        branches: &[(Expr, Expr)],
        otherwise: Option<&Expr>,
    ) -> Result<(), Error> {
        self.push("CASE");
        if let Some(o) = operand {
            self.wrap(" ", o, "")?;
        }
        for (when, then) in branches {
            self.wrap(" WHEN ", when, " THEN ")?;
            self.expr(then)?;
        }
        if let Some(o) = otherwise {
            self.wrap(" ELSE ", o, "")?;
        }
        self.push(" END");
        Ok(())
    }

    /// The value matches the whole expression.
    fn regex(&mut self, e: &Expr, pattern: &str) -> Result<(), Error> {
        self.push(match self.engine {
            Engine::DuckDb => "regexp_matches(",
            Engine::Trino => "regexp_like(",
        });
        self.text_of(e)?;
        self.push(&format!(", {})", quote_text(&format!("^(?:{pattern})$"))?));
        Ok(())
    }

    fn call(&mut self, function: Function, args: &[Expr]) -> Result<(), Error> {
        match (function, args) {
            (Function::IsEmpty, [x]) => {
                self.wrap("(", x, &format!(" IS NULL OR trim(BOTH {BLANKS} FROM "))?;
                self.text_of(x)?;
                self.push(") = '')");
            }
            (Function::Trim, [x]) => {
                self.push(&format!("trim(BOTH {BLANKS} FROM "));
                self.text_of(x)?;
                self.push(")");
            }
            (Function::Length, [x]) => {
                self.push("length(");
                self.text_of(x)?;
                self.push(")");
            }
            (Function::RegexpLike, [x, Expr::Literal(Literal::Text(p))]) => self.regex(x, p)?,
            (Function::DaysSince, [x]) => {
                self.wrap("date_diff('day', ", x, ", ")?;
                self.expr(&Expr::Cut)?;
                self.push(")");
            }
            (Function::DaysBetween, [a, b]) => {
                self.wrap("date_diff('day', ", a, ", ")?;
                self.wrap("", b, ")")?;
            }
            (Function::YearsBetween, [a, b]) => self.years_between(a, b)?,
            (Function::InReference, [x, Expr::Literal(Literal::Text(r))]) => {
                self.in_reference(x, r)?
            }
            (Function::InList, _) => return Err(Error::NotExpanded("in_list")),
            (f, args) => {
                self.push(plain_name(f));
                self.push("(");
                self.list(args)?;
                self.push(")");
            }
        }
        Ok(())
    }

    /// Whole years from `a` to `b`, as an age: the year's difference, less
    /// one if `b`'s month and day come before `a`'s.
    fn years_between(&mut self, a: &Expr, b: &Expr) -> Result<(), Error> {
        let part = |w: &mut Self, f: &str, e: &Expr| w.wrap(&format!("{f}("), e, ")");
        self.push("(");
        part(self, "year", b)?;
        self.push(" - ");
        part(self, "year", a)?;
        self.push(" - CASE WHEN ");
        part(self, "month", b)?;
        self.push(" < ");
        part(self, "month", a)?;
        self.push(" OR (");
        part(self, "month", b)?;
        self.push(" = ");
        part(self, "month", a)?;
        self.push(" AND ");
        part(self, "day", b)?;
        self.push(" < ");
        part(self, "day", a)?;
        self.push(") THEN 1 ELSE 0 END)");
        Ok(())
    }

    /// `x` among a reference's column, in the table the platform resolved.
    fn in_reference(&mut self, x: &Expr, reference: &str) -> Result<(), Error> {
        let unknown = || Error::UnknownReference(reference.to_string());
        let (name, column) = reference.split_once('.').ok_or_else(unknown)?;
        let table = self.tables.get(name).ok_or_else(unknown)?;
        let path = match &table.pinned {
            Some(relation) => relation.clone(),
            None => table
                .path
                .iter()
                .map(|p| quote_ident(p))
                .collect::<Result<Vec<_>, _>>()?
                .join("."),
        };
        // A NULL key would make `IN` give NULL, not FALSE, for a value it
        // does not find: the rule would skip what it must fail.
        let key = quote_ident(column)?;
        self.wrap("(", x, " IN (SELECT ")?;
        self.push(&format!("{key} FROM {path} WHERE {key} IS NOT NULL"));
        if let Some(filter) = &table.filter {
            self.wrap(" AND ", filter, "")?;
        }
        self.push("))");
        Ok(())
    }
}

fn op_sql(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::And => "AND",
        BinaryOp::Or => "OR",
        BinaryOp::Eq => "=",
        BinaryOp::NotEq => "<>",
        BinaryOp::Lt => "<",
        BinaryOp::LtEq => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::GtEq => ">=",
        BinaryOp::Add => "+",
        BinaryOp::Subtract => "-",
        BinaryOp::Multiply => "*",
        BinaryOp::Divide => "/",
        BinaryOp::Modulo => "%",
        BinaryOp::Concat => "||",
    }
}

/// The functions both engines spell alike.
fn plain_name(f: Function) -> &'static str {
    match f {
        Function::Upper => "upper",
        Function::Lower => "lower",
        Function::Coalesce => "coalesce",
        Function::Abs => "abs",
        Function::Round => "round",
        _ => unreachable!("written by the writer itself"),
    }
}

fn type_name(t: Type) -> &'static str {
    match t {
        Type::Text => "VARCHAR",
        Type::Integer => "BIGINT",
        Type::Decimal => "DOUBLE",
        Type::Date => "DATE",
        Type::Timestamp => "TIMESTAMP",
        Type::Boolean => "BOOLEAN",
    }
}

fn unit_name(u: IntervalUnit) -> &'static str {
    match u {
        IntervalUnit::Year => "YEAR",
        IntervalUnit::Month => "MONTH",
        IntervalUnit::Day => "DAY",
    }
}

/// Digits, one optional point and exponent, an optional sign: nothing that
/// could close the number and start something else.
fn is_number(n: &str) -> bool {
    let body = n.strip_prefix('-').unwrap_or(n);
    let (mantissa, exponent) = match body.split_once(['e', 'E']) {
        Some((m, e)) => (m, Some(e.strip_prefix(['+', '-']).unwrap_or(e))),
        None => (body, None),
    };
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    let mantissa_ok = match mantissa.split_once('.') {
        Some((i, f)) => digits(i) && (f.is_empty() || digits(f)),
        None => digits(mantissa),
    };
    mantissa_ok && exponent.is_none_or(digits)
}

fn is_date(d: &str) -> bool {
    let b = d.as_bytes();
    b.len() == 10
        && b.iter().enumerate().all(|(i, c)| {
            if i == 4 || i == 7 {
                *c == b'-'
            } else {
                c.is_ascii_digit()
            }
        })
}
