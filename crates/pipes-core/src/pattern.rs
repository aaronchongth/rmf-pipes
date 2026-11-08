//! Topic patterns for standing subscriptions.
//!
//! A pattern uses the `*` / `**` subset of zenoh key-expression syntax, applied
//! to ROS topic names without their leading `/`:
//!
//! - `*` matches exactly one non-empty segment: `fleet/*/state` matches
//!   `/fleet/tinyRobot1/state` but not `/fleet/a/b/state`.
//! - `**` matches zero or more segments: `fleet/**` matches `/fleet`,
//!   `/fleet/a` and `/fleet/a/b/state`.
//!
//! Sub-segment wildcards (`$*`) and other zenoh operators are rejected so that
//! the behaviour stays obvious. Matching lives here, not in zenoh, so that it is
//! unit-testable without a session.

use std::fmt;

/// A validated topic pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPattern {
    source: String,
    segments: Vec<Segment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Literal(String),
    Star,
    DoubleStar,
}

/// Why a pattern was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PatternError {
    /// The pattern is empty.
    #[error("pattern is empty")]
    Empty,
    /// The pattern has an empty segment.
    #[error("pattern `{0}` has an empty segment (`//`, or a leading/trailing `/` after the first)")]
    EmptySegment(String),
    /// The pattern uses syntax other than `*` and `**`.
    #[error(
        "pattern `{0}` uses unsupported syntax `{1}`; only `*` and `**` wildcards are supported"
    )]
    Unsupported(String, String),
}

impl TopicPattern {
    /// Parse a pattern. A single leading `/` is accepted and ignored, so
    /// `/fleet/*/state` and `fleet/*/state` are the same pattern.
    pub fn parse(input: &str) -> Result<Self, PatternError> {
        let trimmed = input.strip_prefix('/').unwrap_or(input);
        if trimmed.is_empty() {
            return Err(PatternError::Empty);
        }
        let mut segments = Vec::new();
        for raw in trimmed.split('/') {
            let seg = match raw {
                "" => return Err(PatternError::EmptySegment(input.to_owned())),
                "*" => Segment::Star,
                "**" => {
                    // `**/**` is the same as `**`; keep one.
                    if segments.last() == Some(&Segment::DoubleStar) {
                        continue;
                    }
                    Segment::DoubleStar
                }
                lit if lit.contains(['*', '$', '?', '#']) => {
                    return Err(PatternError::Unsupported(input.to_owned(), lit.to_owned()));
                }
                lit => Segment::Literal(lit.to_owned()),
            };
            segments.push(seg);
        }
        Ok(Self {
            source: input.to_owned(),
            segments,
        })
    }

    /// Whether the ROS topic `topic` (e.g. `/fleet/a/state`) matches.
    pub fn matches(&self, topic: &str) -> bool {
        let topic = topic.strip_prefix('/').unwrap_or(topic);
        let parts: Vec<&str> = if topic.is_empty() {
            Vec::new()
        } else {
            topic.split('/').collect()
        };
        matches_from(&self.segments, &parts)
    }

    /// The pattern as written.
    pub fn as_str(&self) -> &str {
        &self.source
    }
}

fn matches_from(pattern: &[Segment], parts: &[&str]) -> bool {
    match pattern.split_first() {
        None => parts.is_empty(),
        Some((Segment::DoubleStar, rest)) => {
            (0..=parts.len()).any(|skip| matches_from(rest, &parts[skip..]))
        }
        Some((seg, rest)) => match parts.split_first() {
            None => false,
            Some((part, more)) => {
                let ok = match seg {
                    Segment::Star => !part.is_empty(),
                    Segment::Literal(lit) => lit == part,
                    Segment::DoubleStar => unreachable!("handled above"),
                };
                ok && matches_from(rest, more)
            }
        },
    }
}

impl fmt::Display for TopicPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.source)
    }
}

impl std::str::FromStr for TopicPattern {
    type Err = PatternError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// True if any of `patterns` matches `topic`.
pub fn any_match(patterns: &[TopicPattern], topic: &str) -> bool {
    patterns.iter().any(|p| p.matches(topic))
}

#[cfg(test)]
mod tests {
    use super::{PatternError, TopicPattern};

    fn m(p: &str, t: &str) -> bool {
        TopicPattern::parse(p).unwrap().matches(t)
    }

    #[test]
    fn literal() {
        assert!(m("chatter", "/chatter"));
        assert!(m("/chatter", "/chatter"));
        assert!(!m("chatter", "/chatter2"));
        assert!(!m("chatter", "/ns/chatter"));
    }

    #[test]
    fn star_is_exactly_one_segment() {
        assert!(m("fleet/*/state", "/fleet/tinyRobot1/state"));
        assert!(!m("fleet/*/state", "/fleet/state"));
        assert!(!m("fleet/*/state", "/fleet/a/b/state"));
        assert!(m("*", "/chatter"));
        assert!(!m("*", "/a/b"));
    }

    #[test]
    fn double_star_is_zero_or_more_segments() {
        assert!(m("fleet/**", "/fleet"));
        assert!(m("fleet/**", "/fleet/a"));
        assert!(m("fleet/**", "/fleet/a/b/state"));
        assert!(m("**/state", "/state"));
        assert!(m("**/state", "/fleet/a/state"));
        assert!(!m("**/state", "/fleet/a/state/x"));
        assert!(m("**", "/anything/at/all"));
        assert!(m("a/**/b/**/c", "/a/x/b/y/z/c"));
        assert!(!m("a/**/b/**/c", "/a/x/y/c"));
    }

    #[test]
    fn repeated_double_star_collapses() {
        assert_eq!(
            TopicPattern::parse("a/**/**/b").unwrap().segments,
            TopicPattern::parse("a/**/b").unwrap().segments
        );
    }

    #[test]
    fn rejects_bad_patterns() {
        assert_eq!(TopicPattern::parse(""), Err(PatternError::Empty));
        assert_eq!(TopicPattern::parse("/"), Err(PatternError::Empty));
        assert!(matches!(
            TopicPattern::parse("a//b"),
            Err(PatternError::EmptySegment(_))
        ));
        assert!(matches!(
            TopicPattern::parse("a/"),
            Err(PatternError::EmptySegment(_))
        ));
        assert!(matches!(
            TopicPattern::parse("a/b$*"),
            Err(PatternError::Unsupported(..))
        ));
        assert!(matches!(
            TopicPattern::parse("a/b*"),
            Err(PatternError::Unsupported(..))
        ));
        assert!(matches!(
            TopicPattern::parse("a/***"),
            Err(PatternError::Unsupported(..))
        ));
    }
}
