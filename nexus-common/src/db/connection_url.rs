//! Connection URL newtype whose [`Display`] and [`Debug`] forms never yield secrets.
//!
//! The raw URL remains available via [`ConnectionUrl::as_str`] for opening clients.
//! Every log, panic, or diagnostic that formats the config field goes through the
//! redactor automatically.

use serde::{Deserialize, Serialize};
use std::fmt;

/// A database connection URL that redacts userinfo and query-string secrets on
/// [`Display`] and [`Debug`].
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConnectionUrl(String);

impl ConnectionUrl {
    /// The raw connection URL, for opening a client only.
    ///
    /// [`Display`] and [`Debug`] never yield this value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for ConnectionUrl {
    fn from(raw: String) -> Self {
        Self(raw)
    }
}

impl From<&str> for ConnectionUrl {
    fn from(raw: &str) -> Self {
        Self(raw.to_owned())
    }
}

impl PartialEq<str> for ConnectionUrl {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for ConnectionUrl {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl fmt::Display for ConnectionUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&redact_connection_url(&self.0))
    }
}

impl fmt::Debug for ConnectionUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ConnectionUrl")
            .field(&redact_connection_url(&self.0))
            .finish()
    }
}

/// Redact URL userinfo and query-string secrets in a diagnostic string.
///
/// Connector errors may embed a connection URL inside surrounding prose. This
/// preserves scheme, host, port, path, non-secret query, and fragment while
/// replacing every `username[:password]@` authority prefix and every query
/// parameter whose name is a secret (`password`, `pwd`, `sslkey`, and any
/// `*key*` / `*secret*` / `*token*` name) even when no userinfo is present.
pub fn redact_connection_url(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0;

    while let Some(relative_scheme_end) = input[cursor..].find("://") {
        let scheme_end = cursor + relative_scheme_end;
        let scheme_start = scheme_start(input, cursor, scheme_end);
        let authority_start = scheme_end + 3;
        let url_end = match input[authority_start..].find(is_url_terminator) {
            Some(offset) => authority_start + offset,
            None => input.len(),
        };

        output.push_str(&input[cursor..scheme_start]);
        output.push_str(&redact_one_url(&input[scheme_start..url_end]));
        cursor = url_end;
    }

    output.push_str(&input[cursor..]);
    output
}

fn scheme_start(input: &str, search_from: usize, scheme_end: usize) -> usize {
    let prefix = &input[search_from..scheme_end];
    let bytes = prefix.as_bytes();
    let mut index = bytes.len();
    while index > 0 {
        let character = bytes[index - 1];
        if character.is_ascii_alphanumeric()
            || character == b'+'
            || character == b'-'
            || character == b'.'
        {
            index -= 1;
        } else {
            break;
        }
    }
    search_from + index
}

fn is_url_terminator(character: char) -> bool {
    character.is_whitespace() || matches!(character, '"' | '\'' | '<' | '>' | ')')
}

fn redact_one_url(url: &str) -> String {
    let Some(scheme_sep) = url.find("://") else {
        return url.to_string();
    };
    let rest = &url[scheme_sep + 3..];
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = redact_userinfo(&rest[..authority_end]);
    let (path, query, fragment) = split_after_authority(&rest[authority_end..]);

    let mut redacted = String::with_capacity(url.len());
    redacted.push_str(&url[..scheme_sep]);
    redacted.push_str("://");
    redacted.push_str(&authority);
    redacted.push_str(path);
    if let Some(query) = query {
        redacted.push('?');
        redacted.push_str(&redact_query(query));
    }
    if let Some(fragment) = fragment {
        redacted.push('#');
        redacted.push_str(fragment);
    }
    redacted
}

fn redact_userinfo(authority: &str) -> String {
    match authority.rfind('@') {
        Some(at) => {
            let mut redacted = String::from("[redacted]@");
            redacted.push_str(&authority[at + 1..]);
            redacted
        }
        None => authority.to_string(),
    }
}

fn split_after_authority(rest: &str) -> (&str, Option<&str>, Option<&str>) {
    let (without_fragment, fragment) = match rest.find('#') {
        Some(index) => (&rest[..index], Some(&rest[index + 1..])),
        None => (rest, None),
    };
    let (path, query) = match without_fragment.find('?') {
        Some(index) => (
            &without_fragment[..index],
            Some(&without_fragment[index + 1..]),
        ),
        None => (without_fragment, None),
    };
    (path, query, fragment)
}

fn redact_query(query: &str) -> String {
    let mut redacted = String::with_capacity(query.len());
    for (index, pair) in query.split('&').enumerate() {
        if index > 0 {
            redacted.push('&');
        }
        redacted.push_str(&redact_query_pair(pair));
    }
    redacted
}

fn redact_query_pair(pair: &str) -> String {
    if pair.is_empty() {
        return String::new();
    }
    let (key, _value) = match pair.split_once('=') {
        Some((key, value)) => (key, Some(value)),
        None => (pair, None),
    };
    if is_secret_query_param(key) {
        let mut redacted = String::with_capacity(key.len() + 11);
        redacted.push_str(key);
        redacted.push_str("=[redacted]");
        redacted
    } else {
        pair.to_string()
    }
}

fn is_secret_query_param(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.contains("password")
        || name.contains("passwd")
        || name.contains("pwd")
        || name.contains("secret")
        || name.contains("token")
        || name.contains("key")
        || name == "auth"
}

#[cfg(test)]
mod tests {
    use super::{redact_connection_url, ConnectionUrl};

    #[test]
    fn redacts_redis_connection_url_userinfo() {
        assert_eq!(
            redact_connection_url("redis://default:redis-password@redis.internal:6379/0"),
            "redis://[redacted]@redis.internal:6379/0"
        );
        assert_eq!(
            redact_connection_url("Error connecting to redis://default@cache:6379/0"),
            "Error connecting to redis://[redacted]@cache:6379/0"
        );
    }

    #[test]
    fn redacts_neo4j_connection_url_userinfo() {
        assert_eq!(
            redact_connection_url("neo4j://neo4j:graph-password@graph.internal:7687"),
            "neo4j://[redacted]@graph.internal:7687"
        );
        assert_eq!(
            redact_connection_url("bolt://neo4j:graph-password@neo4j.railway.internal:7687"),
            "bolt://[redacted]@neo4j.railway.internal:7687"
        );
    }

    #[test]
    fn redacts_postgres_connection_url_userinfo() {
        assert_eq!(
            redact_connection_url(
                "postgres://marketplace:db-password@postgres.internal:5432/nexus?sslmode=require"
            ),
            "postgres://[redacted]@postgres.internal:5432/nexus?sslmode=require"
        );
        assert_eq!(
            redact_connection_url("postgresql://marketplace:db-password@[::1]:5432/nexus"),
            "postgresql://[redacted]@[::1]:5432/nexus"
        );
    }

    #[test]
    fn redacts_url_encoded_userinfo_password() {
        assert_eq!(
            redact_connection_url("redis://default:p%40ss%3Aword%2Fhash@redis.internal:6379/0"),
            "redis://[redacted]@redis.internal:6379/0"
        );
        let redacted = redact_connection_url("redis://user:p%40ss@host:6379");
        assert!(!redacted.contains("p%40ss"));
        assert!(!redacted.contains("user:"));
    }

    #[test]
    fn redacts_ipv6_host_with_userinfo_and_query() {
        assert_eq!(
            redact_connection_url("redis://default:p%40ss@[2001:db8::1]:6379/0?sslmode=require"),
            "redis://[redacted]@[2001:db8::1]:6379/0?sslmode=require"
        );
        assert_eq!(
            redact_connection_url("redis://[::1]:6379/0?password=s3cret"),
            "redis://[::1]:6379/0?password=[redacted]"
        );
    }

    #[test]
    fn redacts_query_string_secrets_without_userinfo() {
        assert_eq!(
            redact_connection_url("redis://host:6379/0?password=s3cret"),
            "redis://host:6379/0?password=[redacted]"
        );
        assert_eq!(
            redact_connection_url(
                "postgres://localhost:5432/nexus?sslmode=require&sslkey=/tmp/key.pem&password=x"
            ),
            "postgres://localhost:5432/nexus?sslmode=require&sslkey=[redacted]&password=[redacted]"
        );
        assert_eq!(
            redact_connection_url("redis://localhost:6379?token=abc&secret=def&auth=ghi"),
            "redis://localhost:6379?token=[redacted]&secret=[redacted]&auth=[redacted]"
        );
        let redacted = redact_connection_url("redis://host:6379/0?password=s3cret");
        assert!(!redacted.contains("s3cret"));
        assert!(redacted.contains("host:6379"));
    }

    #[test]
    fn connection_url_display_and_debug_never_yield_secrets() {
        let url = ConnectionUrl::from(
            "redis://default:p%40ss@host:6379/0?password=s3cret&sslmode=require",
        );
        let display = url.to_string();
        let debug = format!("{url:?}");

        assert_eq!(
            display,
            "redis://[redacted]@host:6379/0?password=[redacted]&sslmode=require"
        );
        assert_eq!(
            debug,
            "ConnectionUrl(\"redis://[redacted]@host:6379/0?password=[redacted]&sslmode=require\")"
        );
        assert!(!display.contains("p%40ss"));
        assert!(!debug.contains("p%40ss"));
        assert!(!display.contains("s3cret"));
        assert!(!debug.contains("s3cret"));
        assert_eq!(
            url.as_str(),
            "redis://default:p%40ss@host:6379/0?password=s3cret&sslmode=require"
        );
    }

    #[test]
    fn redacts_every_embedded_url_without_changing_safe_urls() {
        let diagnostic = "redis redis://u:p@cache:6379, graph neo4j://g:p@graph:7687, bolt bolt://n:s@graph:7687, postgres postgres://m:d@db:5432/nexus; docs https://example.com/help";
        let redacted = redact_connection_url(diagnostic);

        assert_eq!(
            redacted,
            "redis redis://[redacted]@cache:6379, graph neo4j://[redacted]@graph:7687, bolt bolt://[redacted]@graph:7687, postgres postgres://[redacted]@db:5432/nexus; docs https://example.com/help"
        );
        assert!(!redacted.contains("u:p"));
        assert!(!redacted.contains("g:p"));
        assert!(!redacted.contains("n:s"));
        assert!(!redacted.contains("m:d"));
    }
}
