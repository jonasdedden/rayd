//! Lightweight error info attached to failed `ObjectRef`s.
//!
//! Designed to be cheap to materialize: no pickled exception payload here.
//! The full Python exception (with cause and traceback) is recovered from
//! the data buffer only on demand by the binding layer.

use crate::metadata::ErrorCategory;

/// Sentinel `raw_code` for "no granular code provided".
pub const RAW_CODE_UNSPECIFIED: u16 = 0;

/// Information about a failed `ObjectRef` that can be produced without
/// unpickling the user-supplied exception.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ErrorInfo {
    /// Coarse user-facing category.
    pub category: ErrorCategory,
    /// Human-readable message (always present).
    pub message: String,
    /// Formatted traceback, present only for `TaskException`.
    pub traceback: Option<String>,
    /// Granular code mirroring a Ray-style `ErrorType`; for observability and
    /// finer pattern matching by callers who want it.
    pub raw_code: u16,
}

impl ErrorInfo {
    /// Construct an `ErrorInfo` with no traceback.
    #[must_use]
    pub fn new(category: ErrorCategory, message: impl Into<String>) -> Self {
        Self {
            category,
            message: message.into(),
            traceback: None,
            raw_code: RAW_CODE_UNSPECIFIED,
        }
    }

    /// Add a traceback string. Intended for `TaskException` payloads.
    #[must_use]
    pub fn with_traceback(mut self, traceback: impl Into<String>) -> Self {
        self.traceback = Some(traceback.into());
        self
    }

    /// Set the granular `raw_code`.
    #[must_use]
    pub const fn with_raw_code(mut self, raw_code: u16) -> Self {
        self.raw_code = raw_code;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_round_trip() {
        let info = ErrorInfo::new(ErrorCategory::TaskException, "boom")
            .with_traceback("Traceback (most recent call last)\n  ...")
            .with_raw_code(3);
        assert_eq!(info.category, ErrorCategory::TaskException);
        assert_eq!(info.message, "boom");
        assert!(info.traceback.is_some());
        assert_eq!(info.raw_code, 3);
    }
}
