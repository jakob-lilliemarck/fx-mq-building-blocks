/// Filter criteria for poll queries.
///
/// Used to exclude specific message names from poll results at the
/// database query level. Default (empty) applies no filtering.
///
/// # Example
///
/// ```ignore
/// let filter = Filter::default()
///     .with_exclude_names(vec!["heavy-job".into()]);
/// let msg = get_next_unattempted_with_filter(
///     &mut tx, now, host_id, hold_for, filter
/// ).await?;
/// ```
#[derive(Debug, Default, Clone)]
pub struct Filter {
    /// Message names to exclude from poll results.
    /// When empty, no filtering is applied.
    pub exclude_names: Vec<String>,
}

impl Filter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_exclude_names(mut self, names: Vec<String>) -> Self {
        self.exclude_names = names;
        self
    }
}
