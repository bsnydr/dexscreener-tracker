# Sonar CRM

A Google Sheets-based CRM for cold outreach to newly launched crypto tokens.
It finds promising tokens on its own, fills in the background info we care
about, suggests which message template to use, and lets us track the
conversation from there.

## What it does, at a glance

**Every 6 hours, automatically:**
A tracker scans DexScreener across Solana, Ethereum, BSC, and Base. It keeps
the tokens that pass our screener (liquidity ≥ $25k, market cap $100k–$15M,
24h volume ≥ $100k) and drops them into the sheet at the top of the data
section. Anything already in the sheet is skipped.

**As soon as a new row lands (or you paste one manually):**
The sheet fills in the rest — project name, X profile, Telegram, X community,
market cap, 24h volume, 24h change, age, DexScreener link — by calling
DexScreener, Birdeye, and GeckoTerminal. This runs on a short delay (~2 min)
so mass-paste sessions don't fire off hundreds of API calls mid-edit.

**Every enrichment pass also picks an outreach template:**
Based on the token's age, market cap, volume trajectory, and price action,
the sheet tags the row with a template (A / B / C) and renders the
message body into the **Outreach template** column, with the project's
ticker filled in. If you've already written custom copy there, it leaves
yours alone.

## The columns

| Group | Columns |
|---|---|
| **Identity** | Project name, Token CA, Chain, Status, X, TG, Community, Mod, Owner |
| **Market data** (auto-filled) | DS, Market Cap, 24h Volume, 24h Change %, Age (days), VOL_AT_INGEST, PRICE_MAX_SEEN, Last auto enrich |
| **Messaging** | Channel contacted, Date added, Outreach template, Template, Last msg (us), Last msg (them), Message sent, Message Replied |
| **Notes** | Notes |

**You edit:** Status, Mod, Owner, Channel contacted, the message-tracking
columns, Notes, and (optionally) Outreach template if you want to hand-write
instead of using the rendered template.

**The sheet fills:** everything in Market data, the social links, project
name, and Template. It never overwrites Status, Message sent, Message
Replied, Last msg, or a custom-written Outreach template.

## Outreach templates

The `Template` column shows which rule matched:

- **A — "Volume revival"** — Older project (30+ days), volume recently 2x+ above where it was when we first saw it
- **B — "Held the floor"** — Dumped from ATH 50%+, still above $70k mcap, and stabilized (not actively dumping)
- **C — "Up only"** — New project (<30 days), above $70k mcap, 24h change positive
- **Fallback** — Doesn't match any rule cleanly; defaults to A with a `[FALLBACK_NO_MATCH]` flag so you know to check it manually

The message body appears in the **Outreach template** column. Template
content is kept in `templates.json` at the repo root — edit there, commit,
and both the tracker and the sheet will pick up the new copy within an hour
(sheet) or next run (tracker).

## How to use it day-to-day

1. **Open the sheet.** New tokens will already be at the top of the data
   section with everything auto-filled.
2. **Scan the Template column.** A/B/C tells you the pitch angle; the
   Outreach template column has the message copy with the ticker
   substituted.
3. **Reach out.** Paste the message, update **Channel contacted**, tick
   **Message sent**.
4. **Track replies.** When they respond, fill **Last msg (them)** and tick
   **Message Replied**. Move **Status** accordingly.
5. **Manual prospecting.** If you find a token outside the tracker's net,
   paste the CA into the top empty row in column B. The sheet detects the
   chain, stamps the date, assigns you as owner, and enriches the row.

## CRM menu (in the sheet)

- **Enrich All Rows** — Re-run enrichment across every row.
- **Enrich Current Row** — Force re-enrichment on whichever row you're on.
- **Enable/Disable Auto-Enrich** — 6-hourly background refresh.
- **Enable/Disable Pending Enrich** — Catches newly pasted CAs within ~2 min.
- **Run Diagnostic** — Drops a coverage + health report into the
  `_diagnostic` tab (row counts, API call counts, triggers, test calls).
- **Refresh Templates from GitHub** — Pulls the latest `templates.json`
  immediately instead of waiting for the 1h cache to expire.
- **Re-decide Templates (all rows)** — Re-runs template logic across every
  row with their current market data.

## What lives where

- `apps-script/` — Google Apps Script code bound to the sheet (enrichment,
  menu, template engine, migration scripts). Deployed with `clasp`.
- `dexscreener-tracker/` — Python tracker that runs in GitHub Actions every
  6 hours, discovers new tokens, applies the screener, evaluates the
  template at ingest, and POSTs to the sheet.
- `templates.json` — The single source of truth for template copy and rule
  conditions. Used by both the tracker (at ingest) and Apps Script (at
  every re-enrichment).
