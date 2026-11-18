//! The error catalog. Every business error is declared once as a `static ErrorSpec`:
//!
//! ```
//! use ladepa_core::factory::{conflict, ErrorSpec};
//! static SYSTEM_ROLE: ErrorSpec = conflict("system_role", "system role '{code}' cannot be deleted");
//! let e = SYSTEM_ROLE.build(&[("code", &"ORG_ADMIN")]);
//! assert_eq!(e.message, "system role 'ORG_ADMIN' cannot be deleted");
//! ```
//!
//! Codes may contain placeholders too (`"{field}_required"`). A missing parameter is a programming error and
//! panics (the catalog tests would catch it).
use std::fmt::Display;

use crate::exception::{DomainError, Kind};

#[derive(Debug, Clone, Copy)]
pub struct ErrorSpec {
    pub kind: Kind,
    pub code: &'static str,
    pub template: &'static str,
}

impl ErrorSpec {
    pub const fn new(kind: Kind, code: &'static str, template: &'static str) -> ErrorSpec {
        ErrorSpec { kind, code, template }
    }

    pub fn build(&self, params: &[(&str, &dyn Display)]) -> DomainError {
        let params: Vec<(String, String)> = params.iter().map(|(k, v)| ((*k).to_string(), v.to_string())).collect();
        DomainError {
            kind: self.kind,
            code: fill(self.code, &params),
            message: fill(self.template, &params),
            template: self.template.to_string(),
            params,
        }
    }

    pub fn fail<T>(&self, params: &[(&str, &dyn Display)]) -> Result<T, DomainError> {
        Err(self.build(params))
    }
}

pub const fn validation(code: &'static str, template: &'static str) -> ErrorSpec {
    ErrorSpec::new(Kind::Validation, code, template)
}
pub const fn unauthorized(code: &'static str, template: &'static str) -> ErrorSpec {
    ErrorSpec::new(Kind::Unauthorized, code, template)
}
pub const fn forbidden(code: &'static str, template: &'static str) -> ErrorSpec {
    ErrorSpec::new(Kind::Forbidden, code, template)
}
pub const fn not_found(code: &'static str, template: &'static str) -> ErrorSpec {
    ErrorSpec::new(Kind::NotFound, code, template)
}
pub const fn conflict(code: &'static str, template: &'static str) -> ErrorSpec {
    ErrorSpec::new(Kind::Conflict, code, template)
}

/// Replaces `{name}` placeholders. Panics when a placeholder has no parameter.
pub fn fill(template: &str, params: &[(String, String)]) -> String {
    let mut out = String::with_capacity(template.len() + 16);
    let mut rest = template;
    while let Some(start) = rest.find('{') {
        out.push_str(&rest[..start]);
        let after = &rest[start + 1..];
        match after.find('}') {
            Some(end) if is_name(&after[..end]) => {
                let name = &after[..end];
                match params.iter().find(|(k, _)| k == name) {
                    Some((_, v)) => out.push_str(v),
                    None => panic!("error template {template:?}: missing parameter {name:?}"),
                }
                rest = &after[end + 1..];
            }
            _ => {
                out.push('{');
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

fn is_name(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    static DUP: ErrorSpec = conflict("dup_{what}", "{what} exists");

    #[test]
    fn fills_code_and_message() {
        let e = DUP.build(&[("what", &"role")]);
        assert_eq!((e.code.as_str(), e.message.as_str(), e.kind), ("dup_role", "role exists", Kind::Conflict));
        assert_eq!(e.params, vec![("what".to_string(), "role".to_string())]);
    }

    #[test]
    #[should_panic(expected = "missing parameter")]
    fn missing_parameter_is_a_bug() {
        DUP.build(&[]);
    }

    #[test]
    fn braces_that_are_not_placeholders_survive() {
        assert_eq!(fill("a {1b c} {x}", &[("x".into(), "y".into())]), "a {1b c} y");
        assert_eq!(fill("regex [a-z]{2,3}", &[]), "regex [a-z]{2,3}");
    }

    #[test]
    fn not_found_has_a_stable_code() {
        let e = DomainError::not_found("Role", "r1");
        assert_eq!((e.code.as_str(), e.message.as_str()), ("role_not_found", "Role 'r1' not found"));
    }
}
