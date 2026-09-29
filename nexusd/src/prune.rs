use nexus_common::types::DynError;
use nexus_watcher::events::handlers::listing::StaleListingPrune;

/// The report `db prune-stale-listings` prints.
pub fn render_prune_summary(summary: &StaleListingPrune, apply: bool) -> String {
    let mut lines = vec![format!(
        "Checked {} listing(s): {} on their homeserver, {} gone, {} could not be checked",
        summary.scanned,
        summary.present,
        summary.stale.len(),
        summary.failed
    )];
    for (owner_id, listing_id) in &summary.stale {
        lines.push(format!("gone from homeserver: {owner_id}/{listing_id}"));
    }
    for (owner_id, listing_id) in &summary.to_restore {
        lines.push(format!(
            "record back after an interrupted run: {owner_id}/{listing_id}"
        ));
    }
    if apply {
        lines.push(format!("Deleted {} stale listing(s)", summary.pruned));
        if summary.restored > 0 {
            lines.push(format!(
                "Re-indexed {} listing(s) whose record came back",
                summary.restored
            ));
        }
    } else if !summary.stale.is_empty() || !summary.to_restore.is_empty() {
        lines.push(
            "Dry run: nothing changed. Re-run with --apply to delete the listings above"
                .to_string(),
        );
    }
    lines.join("\n")
}

/// The command fails when any listing could not be checked or deleted, so an
/// operator or script never reads a partial run as complete.
pub fn prune_outcome(summary: &StaleListingPrune) -> Result<(), DynError> {
    if summary.failed > 0 {
        return Err(format!(
            "{} listing(s) could not be checked or deleted; re-run to retry them",
            summary.failed
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(stale: usize, pruned: usize, failed: usize) -> StaleListingPrune {
        StaleListingPrune {
            scanned: 5,
            present: 5 - stale - failed,
            stale: (0..stale)
                .map(|i| ("owner".to_string(), format!("listing{i}")))
                .collect(),
            to_restore: Vec::new(),
            pruned,
            restored: 0,
            failed,
        }
    }

    #[test]
    fn a_dry_run_lists_the_stale_rows_and_says_nothing_was_deleted() {
        let report = render_prune_summary(&summary(2, 0, 0), false);
        assert!(report.contains("2 gone"));
        assert!(report.contains("gone from homeserver: owner/listing0"));
        assert!(report.contains("gone from homeserver: owner/listing1"));
        assert!(report.contains("Dry run: nothing changed"));
        assert!(!report.contains("Deleted"));
        assert!(prune_outcome(&summary(2, 0, 0)).is_ok());
    }

    #[test]
    fn a_real_run_reports_what_it_deleted() {
        let report = render_prune_summary(&summary(2, 2, 0), true);
        assert!(report.contains("Deleted 2 stale listing(s)"));
        assert!(!report.contains("Dry run"));
    }

    #[test]
    fn a_resumed_run_reports_what_it_restored() {
        let mut resumed = summary(0, 0, 0);
        resumed.to_restore = vec![("owner".to_string(), "back".to_string())];
        resumed.restored = 1;
        let report = render_prune_summary(&resumed, true);
        assert!(report.contains("record back after an interrupted run: owner/back"));
        assert!(report.contains("Re-indexed 1 listing(s) whose record came back"));
        let dry = render_prune_summary(&resumed, false);
        assert!(dry.contains("Dry run: nothing changed"));
        assert!(!dry.contains("Re-indexed"));
    }

    #[test]
    fn a_clean_second_run_deletes_nothing_and_succeeds() {
        let report = render_prune_summary(&summary(0, 0, 0), true);
        assert!(report.contains("0 gone"));
        assert!(report.contains("Deleted 0 stale listing(s)"));
        assert!(prune_outcome(&summary(0, 0, 0)).is_ok());
    }

    #[test]
    fn any_failure_makes_the_command_fail() {
        assert!(prune_outcome(&summary(0, 0, 1)).is_err());
        assert!(prune_outcome(&summary(1, 0, 1)).is_err());
    }
}
