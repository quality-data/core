//! The named formats (decision 0033, point 1): maintained by the platform,
//! each a regular expression of the syntax RE2 and RE2J share, anchored to
//! the whole value by the writer and tested on both engines.

use quality_rules_model::NamedFormat;

/// The expression a named format matches, before anchoring.
pub fn regex(format: NamedFormat) -> &'static str {
    match format {
        // The address syntax: a local part, `@`, a domain with a dot.
        NamedFormat::Email => {
            r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)+"
        }
        // A Colombian mobile: 3 and nine digits.
        NamedFormat::MobileCo => r"3\d{9}",
        // A DIVIPOLA code: five digits (municipality) or eight (place).
        NamedFormat::Divipola => r"\d{5}(?:\d{3})?",
        // A street nomenclature: a road type, then a number.
        NamedFormat::AddressCo => {
            r"(?i:(?:CALLE|CL|CLL|CARRERA|CRA|CR|KR|KRA|AVENIDA|AV|AK|AC|DIAGONAL|DG|TRANSVERSAL|TV|TR|CIRCULAR|CQ|AUTOPISTA|AUT))\.?\s+\d.*"
        }
        // Letters (accents included), with single spaces, apostrophes or
        // hyphens between words.
        NamedFormat::PersonName => r"\p{L}+(?:[ '\-]\p{L}+)*",
        // At least one letter or digit.
        NamedFormat::NotOnlySymbols => r".*[\p{L}\p{N}].*",
        // Starts with an upper-case letter.
        NamedFormat::Capitalized => r"\p{Lu}.*",
        // No space at either end and none doubled.
        NamedFormat::NormalizedSpaces => r"\S+(?: \S+)*",
    }
}
