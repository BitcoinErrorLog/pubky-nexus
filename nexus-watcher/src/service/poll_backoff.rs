use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

/// Bounded, per-homeserver eligibility state for the serial watcher runner.
///
/// A rate-limited feed is skipped until its deadline; it never sleeps in the
/// runner and therefore cannot delay healthy feeds later in the same pass.
#[derive(Default)]
pub struct HomeserverPollBackoff {
    next_allowed: Mutex<HashMap<String, Instant>>,
}

impl HomeserverPollBackoff {
    pub fn defer(&self, homeserver_id: &str, delay: Duration) {
        let deadline = Instant::now() + delay;
        let mut next_allowed = self.next_allowed.lock().expect("poll backoff lock");
        next_allowed.retain(|_, deadline| *deadline > Instant::now());
        next_allowed.insert(homeserver_id.to_string(), deadline);
    }

    pub fn is_allowed(&self, homeserver_id: &str) -> bool {
        let now = Instant::now();
        let mut next_allowed = self.next_allowed.lock().expect("poll backoff lock");
        next_allowed.retain(|_, deadline| *deadline > now);
        next_allowed
            .get(homeserver_id)
            .is_none_or(|deadline| *deadline <= now)
    }

    pub fn eligible<'a>(
        &self,
        homeserver_ids: impl IntoIterator<Item = &'a String>,
        limit: usize,
    ) -> Vec<String> {
        homeserver_ids
            .into_iter()
            .filter(|homeserver_id| self.is_allowed(homeserver_id))
            .take(limit)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::HomeserverPollBackoff;
    use std::time::Duration;

    #[test]
    fn rate_limited_feed_does_not_block_another_feed() {
        let backoff = HomeserverPollBackoff::default();
        backoff.defer("rate-limited", Duration::from_secs(5));

        assert!(!backoff.is_allowed("rate-limited"));
        assert!(backoff.is_allowed("healthy"));
    }

    #[test]
    fn skipped_rate_limited_feed_does_not_consume_runner_limit() {
        let backoff = HomeserverPollBackoff::default();
        let homeservers = vec!["rate-limited".to_string(), "healthy".to_string()];
        backoff.defer("rate-limited", Duration::from_secs(5));

        assert_eq!(backoff.eligible(&homeservers, 1), vec!["healthy"]);
    }
}
