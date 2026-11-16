//! Auto-mode safety guard: an approval path independent of the agent model.
//!
//! The guard decides state-changing tool calls in `ApprovalMode::Auto`. It
//! layers a deterministic policy (`policy`) over an independent LLM
//! classifier (`classifier`) that sees only the user's own requests and the
//! proposed action -- never assistant text or tool output -- so content the
//! agent read cannot talk the guard into approving something.

mod classifier;
mod decision;
mod policy;

use std::collections::HashMap;

use tokio_util::sync::CancellationToken;

use crate::agent::tools::Invocation;
use crate::agent::workspace::Workspace;

/// What the guard decided for one call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Run without asking; the reason is shown to the user.
    Allow(String),
    /// Refuse; the reason is returned to the model as the tool result.
    Deny(String),
    /// Not decidable automatically; ask the user (refused when headless).
    Ask(String),
}

/// API used by the independent safety reviewer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SafetyApi {
    #[default]
    Auto,
    Chat,
    Decision,
}

impl SafetyApi {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "auto" => Some(Self::Auto),
            "chat" => Some(Self::Chat),
            "decision" | "systemone" => Some(Self::Decision),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Chat => "chat",
            Self::Decision => "decision",
        }
    }
}

/// Everything the guard may look at for one call.
pub struct ReviewRequest<'a> {
    pub inv: &'a Invocation,
    /// `tools::title(inv)` (already redacted).
    pub title: &'a str,
    /// Texts the user typed this session (oldest first), excluding tool
    /// results and `!` shell output.
    pub user_requests: &'a [String],
    pub workspace: &'a Workspace,
}

pub struct Guard {
    classifier: classifier::Classifier,
    decision: decision::Decision,
    api: SafetyApi,
    backends: HashMap<(String, String), SafetyApi>,
    consecutive_denials: u8,
    total_denials: u8,
    paused: bool,
}

impl Guard {
    /// `model`: classifier model id; empty lets mistl pick `ai.default_ref`.
    pub fn new(model: String, reasoning_effort: Option<String>) -> Self {
        Self {
            classifier: classifier::Classifier::new(model.clone(), reasoning_effort),
            decision: decision::Decision::new(model),
            api: SafetyApi::Auto,
            backends: HashMap::new(),
            consecutive_denials: 0,
            total_denials: 0,
            paused: false,
        }
    }

    /// Base URL of mistl's local OpenAI-compatible API (`http://host:port/v1`).
    pub fn set_base_url(&mut self, url: String) {
        self.decision.set_base_url(url.clone());
        self.classifier.set_base_url(url);
    }

    pub fn set_model(&mut self, model: String) {
        self.decision.set_model(model.clone());
        self.classifier.set_model(model);
    }

    pub fn set_api(&mut self, api: SafetyApi) {
        self.api = api;
    }

    /// Selected backend; auto mode is unknown until this target is probed.
    pub fn backend(&self) -> Option<&'static str> {
        self.selected_api()
            .filter(|api| *api != SafetyApi::Auto)
            .map(SafetyApi::as_str)
    }

    fn selected_api(&self) -> Option<SafetyApi> {
        match self.api {
            SafetyApi::Auto => self.backends.get(&self.decision.key()).copied(),
            api => Some(api),
        }
    }

    pub fn set_reasoning_effort(&mut self, effort: Option<String>) {
        self.classifier.set_reasoning_effort(effort);
    }

    /// Forget denial counters (conversation cleared or mode re-entered).
    pub fn reset(&mut self) {
        self.consecutive_denials = 0;
        self.total_denials = 0;
        self.paused = false;
    }

    /// True once repeated denials paused auto mode; the session then falls
    /// back to asking until `reset`.
    pub fn paused(&self) -> bool {
        self.paused
    }

    pub async fn review(&mut self, req: ReviewRequest<'_>, cancel: &CancellationToken) -> Verdict {
        // The title is display-only; it must not become classifier context.
        let _ = req.title;
        let verdict = match policy::evaluate(req.inv, req.workspace) {
            Some(verdict) => verdict,
            None if self.paused() => {
                Verdict::Ask("auto mode is paused after repeated denials".into())
            }
            None => self.classify(&req, cancel).await,
        };
        self.record(&verdict);
        verdict
    }

    async fn classify(&mut self, req: &ReviewRequest<'_>, cancel: &CancellationToken) -> Verdict {
        let api = match self.selected_api() {
            Some(api) => api,
            None => {
                // An oversized action must not trigger even a probe request.
                if let Err(verdict) = decision::state(req, decision::STATE_LIMIT) {
                    return verdict;
                }
                let api = match self.decision.probe(cancel).await {
                    Ok(()) => SafetyApi::Decision,
                    Err(decision::Failure::Cancelled) => {
                        return Verdict::Ask("safety check was cancelled".into());
                    }
                    Err(_) => SafetyApi::Chat,
                };
                if cancel.is_cancelled() {
                    return Verdict::Ask("safety check was cancelled".into());
                }
                self.backends.insert(self.decision.key(), api);
                api
            }
        };
        match api {
            SafetyApi::Decision => self.decision.review(req, cancel).await,
            _ => self.classifier.review(req, cancel).await,
        }
    }

    fn record(&mut self, verdict: &Verdict) {
        match verdict {
            Verdict::Allow(_) => self.consecutive_denials = 0,
            Verdict::Deny(_) => {
                self.consecutive_denials = self.consecutive_denials.saturating_add(1);
                self.total_denials = self.total_denials.saturating_add(1);
            }
            Verdict::Ask(_) => {}
        }
        self.paused |= self.consecutive_denials >= 3 || self.total_denials >= 20;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safety_api_parsing_and_names() {
        for (text, api) in [
            ("auto", SafetyApi::Auto),
            ("chat", SafetyApi::Chat),
            ("decision", SafetyApi::Decision),
            ("systemone", SafetyApi::Decision),
        ] {
            assert_eq!(SafetyApi::parse(text), Some(api));
            assert_eq!(SafetyApi::parse(api.as_str()), Some(api));
        }
        for text in ["", "unknown", "AUTO", " chat "] {
            assert_eq!(SafetyApi::parse(text), None);
        }
        assert_eq!(SafetyApi::default(), SafetyApi::Auto);
    }

    #[test]
    fn consecutive_denials_pause_until_reset() {
        let mut guard = Guard::new("safety-model".into(), None);
        let deny = Verdict::Deny("outside the request".into());
        guard.record(&deny);
        guard.record(&deny);
        guard.record(&Verdict::Ask("explicit approval required".into()));
        assert!(!guard.paused());
        guard.record(&Verdict::Allow("routine work".into()));
        assert_eq!(guard.consecutive_denials, 0);
        assert_eq!(guard.total_denials, 2);
        for _ in 0..3 {
            guard.record(&deny);
        }
        assert!(guard.paused());
        guard.reset();
        assert!(!guard.paused());
        assert_eq!(guard.total_denials, 0);
    }

    #[test]
    fn total_denials_pause_despite_intervening_allows() {
        let mut guard = Guard::new(String::new(), None);
        for i in 0..20 {
            guard.record(&Verdict::Allow("routine work".into()));
            guard.record(&Verdict::Deny("unrelated action".into()));
            assert_eq!(guard.paused(), i == 19);
        }
        guard.reset();
        assert!(!guard.paused());
    }

    #[tokio::test]
    async fn policy_precedes_uninitialized_classifier_and_asks_do_not_count() {
        let workspace = policy::tests::workspace();
        let mut guard = Guard::new(String::new(), None);
        for command in ["git push", "rm -rf build", "sudo echo hi"] {
            let inv = Invocation::Shell {
                command: command.into(),
                background: false,
            };
            let verdict = guard
                .review(
                    ReviewRequest {
                        inv: &inv,
                        title: "action",
                        user_requests: &[],
                        workspace: &workspace,
                    },
                    &CancellationToken::new(),
                )
                .await;
            assert!(matches!(verdict, Verdict::Ask(_)));
        }
        assert_eq!(guard.total_denials, 0);
        let inv = Invocation::ProcessStop { id: 1 };
        let req = || ReviewRequest {
            inv: &inv,
            title: "stop",
            user_requests: &[],
            workspace: &workspace,
        };
        assert!(matches!(
            guard.review(req(), &CancellationToken::new()).await,
            Verdict::Allow(_)
        ));
        for _ in 0..3 {
            guard.record(&Verdict::Deny("denied".into()));
        }
        assert!(matches!(
            guard.review(req(), &CancellationToken::new()).await,
            Verdict::Allow(_)
        ));
        assert!(guard.paused());
    }
}
