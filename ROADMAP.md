# Roadmap

## Custom Event Tracking

Page views are tracked via Vercel Analytics and Cloudflare Web Analytics (both cookie-free). The next step is custom events to get visibility into how learners actually use the koans.

### Proposed events

| Event | Fires when | Params |
|---|---|---|
| `koan_started` | User opens a koan page | `koan_id`, `category`, `difficulty` |
| `code_run` | User clicks Run | `koan_id`, `success` (bool) |
| `koan_completed` | Assertions pass | `koan_id`, `category`, `difficulty` |
| `hint_viewed` | User reveals a hint | `koan_id`, `hint_index` |
| `solution_viewed` | User reveals the solution | `koan_id` |
