"""HTML and PDF rendering for MGA proposals.

Converts the JSONB proposal_data into a self-contained HTML document
(with embedded CSS) and optionally renders it to PDF via Playwright.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from markdown_it import MarkdownIt

# ---------------------------------------------------------------------------
# Markdown renderer (with tables)
# ---------------------------------------------------------------------------

_md = MarkdownIt("commonmark").enable("table")

# ---------------------------------------------------------------------------
# Section ordering & display names
# ---------------------------------------------------------------------------

SECTION_ORDER = [
    "executive_summary",
    "market_analysis",
    "pricing_analysis",
    "underwriting_strategy",
    "carrier_strategy",
    "reinsurance_strategy",
    "claims_philosophy",
    "tinman_advantage",
    "distribution_strategy",
    "product_market_fit",
    "financial_projections",
    "go_to_market",
    "risk_factors",
]

# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

_CSS = """\
:root {
  --primary: #1a365d;
  --primary-light: #2c5282;
  --accent: #2b6cb0;
  --bg: #ffffff;
  --bg-alt: #f7fafc;
  --text: #1a202c;
  --text-muted: #718096;
  --border: #e2e8f0;
  --success: #276749;
}

* { box-sizing: border-box; margin: 0; padding: 0; }

body {
  font-family: "Segoe UI", system-ui, -apple-system, sans-serif;
  color: var(--text);
  background: var(--bg);
  line-height: 1.7;
  font-size: 15px;
}

.container {
  max-width: 960px;
  margin: 0 auto;
  padding: 40px 48px;
}

/* ---- Cover ---- */
.cover {
  text-align: center;
  padding: 80px 40px 60px;
  border-bottom: 3px solid var(--primary);
  margin-bottom: 48px;
}
.cover h1 {
  font-family: Georgia, "Times New Roman", serif;
  font-size: 2.4rem;
  color: var(--primary);
  margin-bottom: 16px;
  line-height: 1.25;
}
.cover .meta {
  color: var(--text-muted);
  font-size: 0.95rem;
}
.cover .meta span { margin: 0 12px; }
.cover .status {
  display: inline-block;
  margin-top: 16px;
  padding: 4px 16px;
  border-radius: 20px;
  background: var(--primary);
  color: #fff;
  font-size: 0.8rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

/* ---- Download button ---- */
.download-btn {
  position: fixed;
  top: 20px;
  right: 24px;
  z-index: 100;
  background: var(--primary);
  color: #fff;
  text-decoration: none;
  padding: 10px 22px;
  border-radius: 6px;
  font-size: 0.85rem;
  font-weight: 600;
  box-shadow: 0 2px 8px rgba(0,0,0,.15);
  transition: background 0.2s;
}
.download-btn:hover { background: var(--primary-light); }

/* ---- Sections ---- */
.section {
  margin-bottom: 40px;
  page-break-inside: avoid;
}
.section h2 {
  font-family: Georgia, "Times New Roman", serif;
  font-size: 1.55rem;
  color: var(--primary);
  border-bottom: 2px solid var(--border);
  padding-bottom: 8px;
  margin-bottom: 20px;
}
.section h3 {
  font-size: 1.15rem;
  color: var(--primary-light);
  margin: 24px 0 10px;
}
.section h4 {
  font-size: 1rem;
  color: var(--accent);
  margin: 18px 0 8px;
}
.section p {
  margin-bottom: 12px;
}
.section ul, .section ol {
  margin: 8px 0 16px 24px;
}
.section li {
  margin-bottom: 4px;
}
.section strong {
  color: var(--primary);
}

/* ---- Tables ---- */
table {
  width: 100%;
  border-collapse: collapse;
  margin: 16px 0 24px;
  font-size: 0.9rem;
}
thead th {
  background: #2d3748;
  color: #fff;
  font-weight: 600;
  text-align: center;
  padding: 10px 12px;
}
thead th:first-child {
  text-align: left;
}
tbody td {
  padding: 8px 12px;
  border-bottom: 1px solid var(--border);
}
tbody tr:nth-child(even) {
  background: var(--bg-alt);
}
tbody tr:hover {
  background: #edf2f7;
}

/* ---- Financial projections table ---- */
.financial-table {
  margin: 24px 0;
}
.financial-table th {
  background: #1a365d;
}
.financial-table .currency {
  text-align: right;
  font-variant-numeric: tabular-nums;
}
.financial-table .pct {
  text-align: center;
}

/* ---- Blockquotes (callouts) ---- */
blockquote {
  border-left: 4px solid var(--accent);
  background: var(--bg-alt);
  padding: 12px 20px;
  margin: 16px 0;
  border-radius: 0 6px 6px 0;
}
blockquote p { margin: 0; }

/* ---- Code ---- */
code {
  background: #edf2f7;
  padding: 2px 6px;
  border-radius: 3px;
  font-size: 0.88em;
}
pre {
  background: #2d3748;
  color: #e2e8f0;
  padding: 16px;
  border-radius: 6px;
  overflow-x: auto;
  margin: 12px 0 20px;
}
pre code {
  background: none;
  padding: 0;
  color: inherit;
}

/* ---- Horizontal rule ---- */
hr {
  border: none;
  border-top: 1px solid var(--border);
  margin: 32px 0;
}

/* ---- Footer ---- */
.footer {
  margin-top: 60px;
  padding-top: 24px;
  border-top: 2px solid var(--border);
  color: var(--text-muted);
  font-size: 0.82rem;
}
.footer .token-usage {
  display: flex;
  gap: 24px;
  flex-wrap: wrap;
  margin-top: 8px;
}
.footer .token-usage span {
  background: var(--bg-alt);
  padding: 4px 12px;
  border-radius: 4px;
}

/* ---- Print ---- */
@media print {
  .download-btn { display: none !important; }
  body { font-size: 12pt; }
  .container { max-width: 100%; padding: 0; }
  .cover { padding: 40px 20px 30px; }
  .section { page-break-inside: avoid; }
  table { page-break-inside: avoid; }
}
"""


# ---------------------------------------------------------------------------
# Financial projections → HTML table
# ---------------------------------------------------------------------------


def _fmt_currency(val: float) -> str:
    """Format a number as $X,XXX,XXX (no decimals)."""
    if abs(val) >= 1:
        return f"${val:,.0f}"
    return f"${val:,.2f}"


def _fmt_pct(val: float) -> str:
    """Format a ratio (0.35) as '35.0%'."""
    return f"{val * 100:.1f}%"


def _render_financial_table(projections: list[dict[str, Any]]) -> str:
    """Build an HTML table for the 5-year financial projections."""
    if not projections:
        return ""

    rows = ""
    for p in projections:
        rows += f"""\
        <tr>
          <td style="text-align:center;font-weight:600;">Year {p['year']}</td>
          <td class="currency">{_fmt_currency(p['premium_volume'])}</td>
          <td class="pct">{_fmt_pct(p['loss_ratio'])}</td>
          <td class="pct">{_fmt_pct(p['expense_ratio'])}</td>
          <td class="pct">{_fmt_pct(p['commission_rate'])}</td>
          <td class="currency">{_fmt_currency(p['net_income'])}</td>
          <td class="currency">{_fmt_currency(p['cumulative_income'])}</td>
        </tr>
"""

    return f"""\
<table class="financial-table">
  <thead>
    <tr>
      <th>Year</th>
      <th>Premium Volume</th>
      <th>Loss Ratio</th>
      <th>Expense Ratio</th>
      <th>Commission Rate</th>
      <th>Net Income</th>
      <th>Cumulative Income</th>
    </tr>
  </thead>
  <tbody>
{rows}  </tbody>
</table>
"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def render_proposal_html(
    proposal_data: dict[str, Any],
    title: str,
    created_at: datetime | str,
    token_usage: dict[str, Any] | None = None,
    *,
    proposal_id: str = "",
    status: str = "draft",
) -> str:
    """Render a full proposal as a self-contained HTML document.

    Parameters
    ----------
    proposal_data:
        The raw JSONB ``proposal_data`` column value.
    title:
        Proposal title.
    created_at:
        Timestamp of generation.
    token_usage:
        Optional dict with ``input_tokens``, ``output_tokens``, etc.
    proposal_id:
        Used to build the PDF download link.
    status:
        Proposal status badge text.

    Returns
    -------
    str
        Complete HTML document (utf-8, self-contained).
    """
    sections: dict[str, Any] = proposal_data.get("sections", {})
    projections: list[dict] = proposal_data.get("financial_projections", [])

    # Format the date
    if isinstance(created_at, str):
        try:
            created_at = datetime.fromisoformat(created_at)
        except (ValueError, TypeError):
            pass

    if isinstance(created_at, datetime):
        date_str = created_at.strftime("%B %d, %Y at %I:%M %p")
    else:
        date_str = str(created_at)

    # Build section HTML
    sections_html = ""
    for key in SECTION_ORDER:
        sec = sections.get(key)
        if not sec:
            continue

        sec_title = sec.get("title", key.replace("_", " ").title())
        content = sec.get("content", "")

        # Convert markdown content to HTML
        content_html = _md.render(content)

        # If this is the financial_projections section, append the table
        extra = ""
        if key == "financial_projections" and projections:
            extra = _render_financial_table(projections)

        sections_html += f"""\
<div class="section" id="{key}">
  <h2>{sec_title}</h2>
  {content_html}
  {extra}
</div>
"""

    # Token usage footer
    token_html = ""
    if token_usage:
        items = []
        for k, v in token_usage.items():
            label = k.replace("_", " ").title()
            if isinstance(v, (int, float)):
                items.append(f"<span><strong>{label}:</strong> {v:,.0f}</span>")
            else:
                items.append(f"<span><strong>{label}:</strong> {v}</span>")
        token_html = f'<div class="token-usage">{"".join(items)}</div>'

    # PDF download button
    pdf_link = ""
    if proposal_id:
        pdf_link = (
            f'<a class="download-btn" '
            f'href="/v1/mga/proposals/{proposal_id}/pdf">'
            f"Download PDF</a>"
        )

    # Table of contents
    toc_items = ""
    for key in SECTION_ORDER:
        sec = sections.get(key)
        if sec:
            sec_title = sec.get("title", key.replace("_", " ").title())
            toc_items += f'    <li><a href="#{key}">{sec_title}</a></li>\n'

    toc_html = ""
    if toc_items:
        toc_html = f"""\
<nav class="section" style="margin-bottom:32px;">
  <h2>Table of Contents</h2>
  <ol style="line-height:2.2;">
{toc_items}  </ol>
</nav>
"""

    return f"""\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
{_CSS}
  </style>
</head>
<body>
  {pdf_link}
  <div class="container">
    <div class="cover">
      <h1>{title}</h1>
      <div class="meta">
        <span>Generated {date_str}</span>
      </div>
      <div class="status">{status}</div>
    </div>

    {toc_html}

    {sections_html}

    <div class="footer">
      <p>This proposal was generated by the Hermes MGA Proposal Agent.</p>
      {token_html}
    </div>
  </div>
</body>
</html>
"""


async def render_proposal_pdf(html: str) -> bytes:
    """Render HTML to PDF bytes via Playwright (Chromium).

    Strips the download button from the HTML before conversion.
    Uses the sync Playwright API in a thread pool to avoid event-loop
    conflicts with uvicorn.

    Parameters
    ----------
    html:
        The full HTML document from ``render_proposal_html``.

    Returns
    -------
    bytes
        Raw PDF file content.
    """
    import asyncio

    # Strip the download button — not useful in a PDF
    html = re.sub(
        r'<a class="download-btn"[^>]*>.*?</a>',
        "",
        html,
        flags=re.DOTALL,
    )

    def _generate_pdf(html_content: str) -> bytes:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.set_content(html_content, wait_until="networkidle")
            pdf_bytes = page.pdf(
                format="Letter",
                print_background=True,
                margin={"top": "0.5in", "bottom": "0.5in", "left": "0.5in", "right": "0.5in"},
            )
            browser.close()
        return pdf_bytes

    return await asyncio.to_thread(_generate_pdf, html)
