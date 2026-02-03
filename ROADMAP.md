# Roadmap

## Google Analytics — Custom Event Tracking

Page-view tracking is live (PR #18). The next step is custom events to get visibility into how learners actually use the koans.

### Proposed events

| Event | Fires when | Params |
|---|---|---|
| `koan_started` | User opens a koan page | `koan_id`, `category`, `difficulty` |
| `code_run` | User clicks Run | `koan_id`, `success` (bool) |
| `koan_completed` | Assertions pass | `koan_id`, `category`, `difficulty` |
| `hint_viewed` | User reveals a hint | `koan_id`, `hint_index` |
| `solution_viewed` | User reveals the solution | `koan_id` |

### Implementation approach

1. Add a small utility (`src/utils/analytics.js`) that wraps `gtag('event', ...)` — keeps the global off the rest of the codebase and makes it easy to swap out later.
2. Call it from `pages/koans/[id].js` and whichever components own the Run button, hint, and solution reveal.
3. No new dependencies or config changes needed beyond what PR #18 already added.
