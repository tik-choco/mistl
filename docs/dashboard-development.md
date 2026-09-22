# Dashboard development

The dashboard is a modular monolith: one Rust daemon and one embedded web
application, with independently owned feature views. Cargo embeds every asset;
Node and npm are only needed for UI tests. No sibling checkout is needed to
build or run the dashboard.

The reference designs are `../mistlib-dev/tests/misteval/src/webui/assets/topology.html`
(graph overview, fit control, separate details), `../tc-storage/src/components/`
(sidebar and file browser), and `../tc-chat/src/components/` with its scoped
styles (room list and conversation). These are design references, not runtime
imports. Chat remains a preview of the latest 100 relay events: sending and
resolving edited or unavailable content require capabilities outside the
current relay API.

## Boundaries

`src/web/assets/index.html` is the composition root. It owns navigation,
translations, polling, application state, and adapters to the existing daemon
API. `src/web/assets/features/` owns presentation and interaction:

| Feature | Entry point | Responsibility |
| --- | --- | --- |
| Topology | `MistlTopology.create(options)` | Hover/focus preview and node detail dialog |
| Storage | `MistlStorage.create(options)` | File filtering, sorting, list/grid views and file details |
| Chat | `MistlChat.create(options)` | Room navigation, searchable conversation history and event details |

Features receive data and callbacks. They must not read shell state, call daemon
APIs directly, import another feature, or depend on the source/reference apps.
Keep transport and protocol semantics in the adapter. The topology renderer
continues to own graph layout and live activity; it passes display models to
the detail component. Avoid extracting a shared framework until multiple real
use cases require it.

Use feature-scoped CSS and shared theme variables. Render untrusted filenames,
message bodies and IDs as text, never HTML. Display concise summaries with the
full value available by hover/focus or an explicit details action. Every mouse
action needs a keyboard equivalent; focus and selection must survive polling.
Keep UI wording about the user's task. Put internal identifiers in details and
show an Apply action only when the backend reports pending changes.

## Parallel changes

Assign one feature directory and its adapter to each developer or agent. Agree
on the callback/data contract before implementation. A separate integration
owner changes shell composition, asset routes, and cross-feature tests. Use
small patches for adapters in the shared HTML; do not replace a stale copy of
the entire file. Carry the same ownership into future extraction of other
screens rather than adding more cross-feature globals.

Before growing a feature, split pure view-model transformations from DOM
rendering and effects. Keep user interactions close to the feature that owns
them. Do not copy a sibling app's network client or add a second state store to
reproduce its appearance. Extend the daemon contract explicitly if a requested
interaction needs new capabilities; never show a nonfunctional control.

## Verification

```sh
npm ci --ignore-scripts
npm test
cargo test --locked web::
```

The Node suite checks feature interactions and the full assembled dashboard
with deterministic daemon responses. It includes long names/IDs, literal HTML
in content, language changes, polling stability and keyboard actions. The Rust
suite checks the embedded asset allowlist and HTTP transport. These checks do
not replace visual browser review: inspect light/dark themes and narrow/wide
layouts when a browser is available.

Run the existing isolated development instance with `node scripts/dev.mjs`
when testing against real services. Do not use the personal daemon as fixture
data or alter its rooms/files merely to exercise UI states.
