# Contango — Project Folder Instructions

## What This Project Is

You are working on **Contango** — a commodity news headline aggregator web app for traders, analysts, and market professionals. Every file, decision, and output in this folder serves that product.

This is not a general news site. It is not a financial terminal. It is not a portfolio tool. It is a fast, clean, signal-focused headline feed covering three commodity categories: **Energy, Metals, and Agriculture.**

When in doubt about scope: if it isn't directly related to displaying commodity headlines quickly and credibly, it does not belong in V1.

---

## The Product in One Sentence

> A fast, signal-focused commodity news headline feed for traders, analysts, and market professionals who need to know what moved — organized by category, stripped of noise, built for people who already know what they're looking at.

---

## Brand Name & Domain

- **Brand name:** Contango
- **Primary domain:** contango.news
- **Backup domain:** contango.io
- "Contango" is a real futures market term (when future prices exceed spot). Every commodity professional recognizes it immediately. Do not explain it in the UI.

---

## Target Users

**Primary:** Commodity traders, market analysts, corporate procurement and supply chain professionals with commodity exposure.

**Secondary:** Financial journalists, Gulf/MENA institutional professionals, sophisticated retail investors with commodity exposure.

**Not the audience:** Crypto users, general retail finance app users, long-form readers.

Design, copy, and UX decisions should always optimize for the primary audience. These users are expert. Never explain basic market terminology in the interface.

---

## Brand Voice & Tone

- Confident, not arrogant
- Functional first — labels and UI copy are self-explanatory
- Respect user expertise — never explain what "WTI" or "spot price" means
- Precise — "12 minutes ago" not "recently"
- No hype — never use "powerful," "seamless," "game-changing"
- Economy of words — if a label can be one word, it is one word
- Active voice in headlines

**Tagline:** *"News at spot price."*
**Homepage headline:** *"Every commodity headline. Nothing else."*
**Homepage subhead:** *"No commentary. No filler. Just the commodity headlines that matter."*

---

## Brand Identity

### Color Palette

```
--color-bg:        #F0EFE9   Warm off-white page background
--color-surface:   #FFFFFF   Card backgrounds
--color-primary:   #0F1923   Near-black — headlines, nav, body text
--color-amber:     #E8A020   Brand accent — the signature color
--color-subtext:   #6B7280   Source labels, timestamps, secondary text
--color-border:    #D1CEC8   Dividers, card outlines, subtle rules
--color-up:        #2ECC71   Price-up indicator (use sparingly)
--color-down:      #E74C3C   Price-down indicator (use sparingly)
```

Category tag colors:
```
ENERGY:      #E8A020  (amber)
METALS:      #4A90D9  (steel blue)
AGRICULTURE: #5BA85C  (soft green)
```

### Typography

- **Headlines:** IBM Plex Serif, weight 600
- **UI / Body:** IBM Plex Sans, weight 400/500
- **Timestamps / Tickers:** IBM Plex Mono, weight 400

The amber (`#E8A020`) is Contango's signature. It should appear on: the logo accent, active filter states, category tags for Energy, headline hover states, and the "Live" badge. Do not introduce additional accent colors.

---

## Product Structure

### Site Map (MVP)

```
/                → Homepage — main headline feed (all categories)
/energy          → Energy headlines only
/metals          → Metals headlines only
/agriculture     → Agriculture headlines only
/about           → What Contango is, sourcing philosophy
```

### Homepage Layout Order

1. Sticky top nav (logo + category links + Live badge)
2. Hero value statement (minimal — not a marketing hero section)
3. Sticky category filter pills (All / Energy / Metals / Agriculture)
4. Headline feed (main content)
5. Load more
6. Footer

### Headline Card Structure

Every card contains exactly:
1. Category tag — colored dot + uppercase label (● ENERGY)
2. Headline text — IBM Plex Serif, dominant, links to source
3. Source label + timestamp — IBM Plex Mono, muted, bottom of card

No images. No excerpts. No author bylines in V1.

---

## MVP Feature Scope

### Build These (V1)

- Real-time headline feed across all commodity categories
- Category filter pills (All / Energy / Metals / Agriculture)
- Source label + timestamp on every card
- Mobile-responsive single-column layout
- Individual category pages
- External links to source articles (open in new tab)

### Do Not Build in V1

| Feature | Reason |
|---|---|
| User accounts / login | Premature — no V1 payoff |
| Watchlists | Scope creep |
| Price charts | This is a headline product |
| Push notifications | Trust must be built first |
| AI summaries | Adds latency, undermines "raw signal" positioning |
| Comment sections | Wrong product entirely |
| Sponsored content | Destroys early trust |
| Dark mode toggle | Ship one great theme first |
| Search | V1.5 feature |
| Sentiment tags | V2 feature |

---

## Technical Defaults

Unless instructed otherwise:

- **Frontend demos:** Single-file HTML (no frameworks, no build tools)
- **Fonts:** IBM Plex Serif + IBM Plex Sans + IBM Plex Mono via Google Fonts
- **CSS:** Custom properties (variables), no Tailwind, no Bootstrap
- **JS:** Vanilla — no React, no Vue, no jQuery
- **No external API calls** in demos
- **Semantic HTML:** `<nav>`, `<main>`, `<article>`, `<footer>`
- **Mobile-first** — single column, horizontal pill scroll, bottom tab bar on mobile

---

## What Good Work Looks Like Here

- Opens fast, scans in under 60 seconds
- Looks like a real, shippable product — not a prototype
- Editorial restraint: no decoration for its own sake
- The amber accent feels like a signature, not a highlight
- A commodity trader opening it immediately understands it was built for them
- Reference aesthetic: *Financial Times front page rebuilt as a mobile web app*

---

## What to Avoid

- Generic "finance app" blue color schemes
- Images or photography in the headline feed
- Dense, anxious card grids — use generous whitespace
- Hiding timestamps or source labels
- Sensationalist copy or marketing hype
- Any feature that adds noise rather than reducing it
- Explaining the product to users who already understand commodity markets