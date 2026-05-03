//! Process-wide tracing-subscriber initialization.
//!
//! `init_default_subscriber` installs a layered subscriber:
//!
//! - **Always:** a `tracing_subscriber::fmt` layer that writes
//!   structured events to stderr. Filter is read from the
//!   `RAYD_LOG` env var (same syntax as `RUST_LOG`); when unset,
//!   defaults to `rayd=info,warn`.
//! - **With the `otlp` feature AND `OTEL_EXPORTER_OTLP_ENDPOINT`
//!   set:** a `tracing-opentelemetry` layer that exports spans to
//!   an OTLP/gRPC collector. Service name from `OTEL_SERVICE_NAME`
//!   (default `rayd`).
//!
//! Idempotent. Re-calling — including from worker subprocesses that
//! also run `rayd.init()` — is a clean no-op rather than a panic.

use std::sync::{Arc, OnceLock};

use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt as _};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::{fmt, EnvFilter, Registry};

/// Pluggable per-event sink, optionally registered before init.
///
/// `rayd-py` registers a Python `logging` bridge here when
/// `RAYD_LOG_FORWARD=1`. The internal `DispatchLayer` always sees
/// events; it forwards them to the registered handler if any.
///
/// `handle` runs synchronously inside the tracing dispatcher — keep
/// it cheap and non-blocking. Acquiring locks or the GIL is fine
/// (every rayd call site doing so already does it sparingly), but
/// expensive serialisation should happen in the caller's
/// formatting helpers, not here.
pub trait EventHandler: Send + Sync + 'static {
    /// Receive one tracing event. Runs synchronously inside the
    /// dispatcher; keep it cheap.
    fn handle(&self, level: Level, target: &str, message: &str);
}

static EVENT_HANDLER: OnceLock<Arc<dyn EventHandler>> = OnceLock::new();

/// Register a global event handler.
///
/// First call wins; later calls are silent no-ops (we use `OnceLock`
/// so the contract is easy to reason about even if init runs from
/// multiple call sites — workers, drivers, …).
pub fn set_event_handler(handler: Arc<dyn EventHandler>) {
    let _ = EVENT_HANDLER.set(handler);
}

/// Env var that overrides the default filter directive. Same syntax
/// as `RUST_LOG` (`tracing_subscriber::EnvFilter::try_new`).
pub const LOG_FILTER_ENV: &str = "RAYD_LOG";

/// Filter applied when `RAYD_LOG` is unset.
pub const DEFAULT_LOG_FILTER: &str = "rayd=info,warn";

/// Env var that, when set, switches on the OTLP span exporter
/// (requires the `otlp` Cargo feature). Value is the collector
/// endpoint URL, e.g. `http://localhost:4317`.
pub const OTLP_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";

/// Env var that overrides the OTLP service name. When unset,
/// defaults to `rayd`.
pub const OTLP_SERVICE_NAME_ENV: &str = "OTEL_SERVICE_NAME";

/// Default OTLP service name when `OTEL_SERVICE_NAME` is unset.
pub const DEFAULT_OTLP_SERVICE_NAME: &str = "rayd";

static SUBSCRIBER_INIT: OnceLock<()> = OnceLock::new();

/// Install the default tracing subscriber.
///
/// First call wins; later calls are silent no-ops. Returns `true`
/// if this call performed the installation, `false` if a subscriber
/// was already in place (either by us or by the host process — e.g.
/// a test harness).
pub fn init_default_subscriber() -> bool {
    let mut installed = false;
    SUBSCRIBER_INIT.get_or_init(|| {
        let filter = EnvFilter::try_from_env(LOG_FILTER_ENV)
            .unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER));
        let fmt_layer = fmt::layer()
            .with_writer(std::io::stderr)
            .with_target(true)
            .compact();

        // `DispatchLayer` always sees events. It forwards them to
        // whatever `EventHandler` was registered before init (if any)
        // so callers like rayd-py can plug in a Python `logging`
        // bridge without participating in the tracing-subscriber
        // type-system gymnastics that follow `Layered::with`.
        let registry = Registry::default()
            .with(filter)
            .with(fmt_layer)
            .with(DispatchLayer);

        // The OTLP layer only attaches when the feature is compiled
        // in AND the env var is set. Otherwise we install the
        // registry as-is — `try_init` so a *different* subscriber
        // already installed by the host (e.g. a test harness) wins
        // instead of panicking.
        #[cfg(feature = "otlp")]
        {
            if let Some(otlp_layer) = otlp::build_otlp_layer() {
                installed = registry.with(otlp_layer).try_init().is_ok();
                return;
            }
        }
        installed = registry.try_init().is_ok();
    });
    installed
}

/// Layer that forwards each event to the registered `EventHandler`.
/// Cheap when no handler is set: just an `OnceLock::get` per event.
struct DispatchLayer;

impl<S> Layer<S> for DispatchLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let Some(handler) = EVENT_HANDLER.get() else {
            return;
        };
        let metadata = event.metadata();
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        handler.handle(*metadata.level(), metadata.target(), &visitor.message);
    }
}

#[derive(Default)]
struct MessageVisitor {
    message: String,
}

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write as _;
        if field.name() == "message" {
            // tracing's default formatting emits the message field as
            // a plain string-like Debug; strip the surrounding quotes
            // a `String` Debug impl would add by writing through the
            // formatter directly.
            let _ = write!(self.message, "{value:?}");
        } else {
            if !self.message.is_empty() {
                self.message.push(' ');
            }
            let _ = write!(self.message, "{}={value:?}", field.name());
        }
    }
}

#[cfg(feature = "otlp")]
mod otlp {
    use opentelemetry::global;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::{SpanExporter, WithExportConfig as _};
    use opentelemetry_sdk::trace::TracerProvider;
    use opentelemetry_sdk::Resource;
    use tracing::Subscriber;
    use tracing_opentelemetry::OpenTelemetryLayer;
    use tracing_subscriber::registry::LookupSpan;

    /// Build the `tracing-opentelemetry` layer if `OTEL_EXPORTER_OTLP_ENDPOINT`
    /// is set in the environment. Returns `None` otherwise so the caller
    /// can install the registry without the OTLP layer.
    ///
    /// Errors during exporter construction are logged and treated as
    /// "no OTLP" — the subscriber still comes up with stderr fmt
    /// instead of failing init outright.
    pub(super) fn build_otlp_layer<S>(
    ) -> Option<OpenTelemetryLayer<S, opentelemetry_sdk::trace::Tracer>>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let endpoint = std::env::var(super::OTLP_ENDPOINT_ENV).ok()?;
        let service_name = std::env::var(super::OTLP_SERVICE_NAME_ENV)
            .unwrap_or_else(|_| super::DEFAULT_OTLP_SERVICE_NAME.to_owned());

        // The tonic-based span exporter spawns a hyper client that
        // requires a current tokio runtime. If the caller invoked
        // `init_default_subscriber` before bringing up a runtime
        // (the common case in rayd-cli/rayd-py — init runs before
        // any `block_on`), we'd panic deep inside hyper. Detect the
        // missing runtime up front and bail with a clear warning so
        // the rest of the subscriber still comes up.
        if tokio::runtime::Handle::try_current().is_err() {
            eprintln!(
                "rayd: {} is set but no tokio runtime is available at init time; \
                 OTLP exporter disabled (move `rayd.init()`/CLI startup inside a \
                 tokio runtime context to enable)",
                super::OTLP_ENDPOINT_ENV
            );
            return None;
        }

        let exporter = match SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&endpoint)
            .build()
        {
            Ok(e) => e,
            Err(err) => {
                eprintln!(
                    "rayd: OTLP exporter setup failed at {endpoint}: {err}; continuing without OTLP"
                );
                return None;
            }
        };

        let resource = Resource::new(vec![KeyValue::new("service.name", service_name)]);
        let provider = TracerProvider::builder()
            .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
            .with_resource(resource)
            .build();

        // Install as the global tracer provider so library code
        // (and our own crates) can also create spans via
        // `opentelemetry::global::tracer(...)`. Returns the previous
        // provider; we discard it.
        let tracer = provider.tracer(super::DEFAULT_OTLP_SERVICE_NAME);
        global::set_tracer_provider(provider);
        Some(tracing_opentelemetry::layer().with_tracer(tracer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// First call must install; second call must be a clean no-op.
    /// We can't easily assert on the global default subscriber state
    /// across tests without leaking process-wide state, so the assertion
    /// is just "`init_default_subscriber` doesn't panic on re-entry".
    #[test]
    fn init_is_idempotent() {
        let _ = init_default_subscriber();
        let second = init_default_subscriber();
        assert!(!second, "second init must report no installation work");
    }
}
