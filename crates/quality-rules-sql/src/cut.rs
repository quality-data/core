//! The cut's date as a literal (decision 0090, ruling 52): a job's bundle
//! is complete SQL with nothing bound, so the cut is written into it as
//! `DATE 'YYYY-MM-DD'`, only after it is checked to be a real calendar day.

use super::Error;

/// A checked cut date: `YYYY-MM-DD`, a day that exists (leap years
/// included), from year 0001.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cut(String);

impl Cut {
    /// `text` as a cut, or [`Error::BadDate`].
    pub fn parse(text: &str) -> Result<Self, Error> {
        if is_day(text) {
            Ok(Self(text.to_owned()))
        } else {
            Err(Error::BadDate(text.to_owned()))
        }
    }

    /// The date as `YYYY-MM-DD`.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The date as a SQL literal, the same text on both engines.
    pub fn literal(&self) -> String {
        format!("DATE '{}'", self.0)
    }
}

fn digits(b: &[u8]) -> Option<u32> {
    b.iter().try_fold(0u32, |n, c| {
        c.is_ascii_digit().then(|| n * 10 + u32::from(c - b'0'))
    })
}

fn is_day(text: &str) -> bool {
    let b = text.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return false;
    }
    let (Some(y), Some(m), Some(d)) = (digits(&b[..4]), digits(&b[5..7]), digits(&b[8..])) else {
        return false;
    };
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let days = match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return false,
    };
    y >= 1 && (1..=days).contains(&d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_real_day_is_a_date_literal() {
        let c = Cut::parse("2026-10-31").unwrap();
        assert_eq!(c.literal(), "DATE '2026-10-31'");
        assert_eq!(c.as_str(), "2026-10-31");
        assert!(Cut::parse("2024-02-29").is_ok());
        assert!(Cut::parse("2000-02-29").is_ok());
    }

    #[test]
    fn anything_else_is_refused() {
        for bad in [
            "2026-02-29",
            "1900-02-29",
            "2026-13-01",
            "2026-00-10",
            "2026-04-31",
            "2026-10-00",
            "0000-01-01",
            "2026-1-31",
            "2026/10/31",
            "2026-10-31 ",
            "2026-10-3'",
            "+026-10-31",
            "",
        ] {
            assert_eq!(
                Cut::parse(bad),
                Err(Error::BadDate(bad.to_owned())),
                "{bad:?}"
            );
        }
    }
}
