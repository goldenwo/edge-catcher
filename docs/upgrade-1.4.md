# Upgrading to v1.4.0

No DB schema migration. v1.4.0 adds the **Reports page** (UI surface for
`python -m edge_catcher.reporting`) and a new **"Operations"** sidebar
section. No breaking changes; no Pi cutover required (Reports is UI-only —
the existing CLI / cron P&L delivery path on the Pi continues to work
unchanged).

## What changed

- New page at `/reports` — pick a DB from `data/*.db`, pick a date, see today's
  P&L plus all-time stats and the raw JSON
- New API endpoints `GET /api/reporting/dbs` and `GET /api/reporting/run`
- New "Operations" sidebar block between "Research" and "Settings" (one nav
  link today; anchors home for upcoming v1.4 follow-ups)

## Steps

```bash
git pull
# Restart the backend (or systemd unit) to pick up the new routes:
uvicorn api.main:app --reload
# Rebuild the UI to pick up the new page + route:
cd ui && npm run build  # or `npm run dev` for development
```

That's it. Visit `/reports` in the browser to confirm the new page.

## Notes

- The Reports page reads from `data/*.db`. If you don't have a paper trader
  populating any DBs in `data/`, the page renders an empty-state pointing
  you to run the paper trader first.
- The dropdown auto-discovers all `*.db` files in `data/` with a
  `paper_trades` table; non-paper-trades sqlite DBs are silently skipped.
- Read coexistence with a live paper trader is supported — the listing
  uses SQLite's read-only URI mode and tolerates a writer holding a
  transaction.
