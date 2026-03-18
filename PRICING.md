# Ceaser — Pricing Strategy

## Plans

### Free ($0)
- 1 user
- 20 queries/day
- 1 database connection
- 5 file uploads/month (5 MB max)
- 2 notebooks
- 5 reports/month
- 5 saved metrics
- 7-day chat history
- 3 Excel sheets max
- No analyst agent
- No cross-DB queries

### Starter ($79/month | $758/year — 20% annual discount)
- 3 users
- 100 queries/day
- 3 database connections
- 50 file uploads/month (50 MB max)
- 10 notebooks
- 30 reports/month
- 20 saved metrics
- 90-day chat history
- 10 Excel sheets max
- Basic analyst agent
- No cross-DB queries
- Email support

### Business ($249/month | $2,390/year — 20% annual discount)
- 10 users
- 500 queries/day
- 10 database connections
- Unlimited file uploads (200 MB max)
- Unlimited notebooks
- Unlimited reports
- Unlimited saved metrics
- 1-year chat history
- Unlimited Excel sheets
- Full analyst agent
- Cross-DB queries
- Audit logs
- API access
- Email + chat support

### Enterprise ($599-999/month | Custom annual pricing)
- Unlimited users
- Unlimited queries
- Unlimited database connections
- Unlimited file uploads (1 GB max)
- Unlimited everything
- Full analyst agent
- Cross-DB queries
- SSO/SAML
- Audit logs
- API access
- Custom agents
- White-label option
- Dedicated support
- SLA guarantee
- Custom integrations

---

## Cost Structure

### Fixed Costs (~$250-425/month)

| Item | Cost/Month |
|---|---|
| Backend server (AWS EC2 t3.large) | $60-100 |
| PostgreSQL (AWS RDS db.t3.medium) | $30-50 |
| Redis (cache/sessions) | $10-25 |
| File storage (S3) | $5-20 |
| Frontend hosting (Vercel) | $20 |
| Domain + SSL | $10 |
| Monitoring (Sentry) | $0-30 |
| Gemini API (LLM) | $50-100 |
| Clerk (auth) | $25 |
| Claude API (backup) | $20-50 |

### Variable Costs (Per Customer/Month)

| Item | Cost |
|---|---|
| LLM calls (50 queries/day) | $2-5 |
| DB storage | $1-2 |
| File processing | $0.50-1 |
| Bandwidth | $0.50 |
| **Total per customer** | **$4-8** |

---

## Unit Economics

| Plan | Revenue | Cost/Customer | Gross Margin |
|---|---|---|---|
| Free | $0 | $4 | -100% (loss leader) |
| Starter ($79) | $79 | $8 | **90%** |
| Business ($249) | $249 | $20 | **92%** |
| Enterprise ($799) | $799 | $40 | **95%** |

---

## Revenue Projections

### Month 1-3 (Launch)
- 50 free, 5 starter, 1 business
- Revenue: $494/month
- Cost: ~$400/month
- Net: +$94

### Month 6
- 200 free, 20 starter, 5 business, 1 enterprise
- Revenue: $3,624/month
- Cost: ~$600/month
- Net: +$3,024

### Month 12
- 500 free, 50 starter, 15 business, 3 enterprise
- Revenue: $10,082/month → **$121K ARR**
- Cost: ~$1,200/month
- Net: +$8,882/month

---

## Competitive Positioning

| Competitor | Price | What Ceaser Offers More |
|---|---|---|
| Julius.ai Pro | $37/month | Semantic layer, analyst agent, notebooks |
| Julius.ai Business | $375/month | Cross-DB, reports, notebooks — at $249 |
| Tableau | $70-150/seat | Natural language, no training needed |
| Looker | $100+/seat | Self-service, no SQL knowledge needed |
| Hire data analyst | $5-15K/month | Instant answers 24/7, consistent |

---

## Implementation Notes

### Plan enforcement uses `OrganizationPlan` table:
```sql
organization_plans (
    organization_id, plan_name, max_seats, max_connections,
    max_queries_per_day, max_reports, features, is_active, trial_ends_at
)
```

### Feature gating checkpoints:
- Chat endpoint → check `max_queries_per_day`
- Connection create → check `max_connections`
- File upload → check file size + monthly upload count
- Report generate → check `max_reports`
- User invite → check `max_seats`
- Cross-DB query → check plan features
- Analyst agent → check plan features

### LLM cost per action:
- Simple query: 4-5 calls (~$0.001)
- Analyst query: 5 calls (~$0.003)
- Multi-query: 8-10 calls (~$0.002)
- Report generation: 6-8 calls (~$0.005)
- File upload insight: 1 call (~$0.0003)

### Total LLM call sites in codebase: 22
- 12 core agents (router, sql, python, repair, verifier, decomposer, analyst, respond, suggestions)
- 4 report agents (planner, writer ×3, enricher)
- 2 cross-DB agents (planner, response)
- 2 notebook agents (extractor, templates)
- 1 Excel agent (insight)
- 1 edge case logger (fire-and-forget)

---

## Pricing Page Copy

### Headline
"Stop waiting for data analysts. Get instant answers from your data."

### Value proposition per tier
- **Free**: "Try Ceaser with your own data. No credit card required."
- **Starter**: "For analysts who want AI-powered insights without the complexity."
- **Business**: "For teams replacing their BI stack with natural language analytics."
- **Enterprise**: "For organizations with multiple databases, strict security, and custom needs."

---

*Last updated: March 2026*
