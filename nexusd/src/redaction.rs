/// Redact URL userinfo before a CLI error is formatted for stderr.
///
/// This deliberately operates on the final diagnostic string because connector
/// errors may embed a connection URL inside surrounding prose. It preserves the
/// scheme, host, port, path, query, and fragment while replacing every
/// `username[:password]@` authority prefix.
pub fn redact_connection_url_userinfo(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0;

    while let Some(relative_scheme_end) = input[cursor..].find("://") {
        let scheme_end = cursor + relative_scheme_end;
        let authority_start = scheme_end + 3;
        let authority_end = input[authority_start..]
            .find(|character: char| {
                character.is_whitespace()
                    || matches!(
                        character,
                        '/' | '?' | '#' | '"' | '\'' | '<' | '>' | ')' | ']'
                    )
            })
            .map_or(input.len(), |offset| authority_start + offset);

        let at = input[authority_start..authority_end]
            .rfind('@')
            .map(|offset| authority_start + offset);

        output.push_str(&input[cursor..authority_start]);
        if let Some(at) = at {
            output.push_str("[redacted]@");
            cursor = at + 1;
        } else {
            cursor = authority_start;
        }

        if cursor >= bytes.len() {
            break;
        }
    }

    output.push_str(&input[cursor..]);
    output
}

#[cfg(test)]
mod tests {
    use super::redact_connection_url_userinfo;

    #[test]
    fn redacts_redis_connection_url_userinfo() {
        assert_eq!(
            redact_connection_url_userinfo("redis://default:redis-password@redis.internal:6379/0"),
            "redis://[redacted]@redis.internal:6379/0"
        );
        assert_eq!(
            redact_connection_url_userinfo("Error connecting to redis://default@cache:6379/0"),
            "Error connecting to redis://[redacted]@cache:6379/0"
        );
    }

    #[test]
    fn redacts_neo4j_connection_url_userinfo() {
        assert_eq!(
            redact_connection_url_userinfo("neo4j://neo4j:graph-password@graph.internal:7687"),
            "neo4j://[redacted]@graph.internal:7687"
        );
        assert_eq!(
            redact_connection_url_userinfo(
                "bolt://neo4j:graph-password@neo4j.railway.internal:7687"
            ),
            "bolt://[redacted]@neo4j.railway.internal:7687"
        );
    }

    #[test]
    fn redacts_postgres_connection_url_userinfo() {
        assert_eq!(
            redact_connection_url_userinfo(
                "postgres://marketplace:db-password@postgres.internal:5432/nexus?sslmode=require"
            ),
            "postgres://[redacted]@postgres.internal:5432/nexus?sslmode=require"
        );
        assert_eq!(
            redact_connection_url_userinfo("postgresql://marketplace:db-password@[::1]:5432/nexus"),
            "postgresql://[redacted]@[::1]:5432/nexus"
        );
    }

    #[test]
    fn redacts_every_embedded_url_without_changing_safe_urls() {
        let diagnostic = "redis redis://u:p@cache:6379, graph neo4j://g:p@graph:7687, bolt bolt://n:s@graph:7687, postgres postgres://m:d@db:5432/nexus; docs https://example.com/help";
        let redacted = redact_connection_url_userinfo(diagnostic);

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
