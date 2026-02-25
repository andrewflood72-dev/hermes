"""Probe the SERFF SFA filing detail page structure after row click."""
import asyncio
import json
import re
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup


async def probe():
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(
        headless=True,
        args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
    )
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 900},
        accept_downloads=True,
    )
    page = await context.new_page()

    # Navigate through to search results
    await page.goto("https://filingaccess.serff.com/sfa/home/CA", timeout=30000)
    await page.wait_for_load_state("networkidle", timeout=15000)
    await page.click("a[href='/sfa/userAgreement.xhtml']")
    await page.wait_for_load_state("networkidle", timeout=15000)
    accept = page.locator(".ui-button").first
    await accept.click()
    await page.wait_for_load_state("networkidle", timeout=20000)
    await asyncio.sleep(2)

    # Select P&C
    try:
        trigger = page.locator(".ui-selectonemenu-trigger").first
        await trigger.click(timeout=5000)
        await asyncio.sleep(1)
        panel = page.locator(".ui-selectonemenu-panel").first
        items = await panel.locator("li").all()
        for item in items:
            text = (await item.text_content()).strip()
            if "property" in text.lower():
                await item.click()
                break
        await page.wait_for_load_state("networkidle", timeout=15000)
        await asyncio.sleep(2)
    except Exception as e:
        print(f"P&C selection: {e}")

    await page.fill("input[id='simpleSearch:productName']", "Commercial Property", timeout=3000)
    await page.locator("button[id='simpleSearch:saveBtn']").click()
    await page.wait_for_load_state("networkidle", timeout=60000)
    await asyncio.sleep(3)
    print("Search results loaded")

    # Click first row to go to detail page
    first_row = page.locator("tbody.ui-datatable-data tr").first
    data_rk = await first_row.get_attribute("data-rk")
    print(f"Clicking row with data-rk={data_rk}")
    await first_row.click()
    await page.wait_for_load_state("networkidle", timeout=30000)
    await asyncio.sleep(3)

    print(f"\n=== DETAIL PAGE ===")
    print(f"URL: {page.url}")

    content = await page.content()
    soup = BeautifulSoup(content, "lxml")

    # Save full HTML
    with open("serff_detail_page.html", "w", encoding="utf-8") as f:
        f.write(content)
    print(f"HTML saved ({len(content)} bytes)")

    # Extract all text content section by section
    print("\n=== PAGE SECTIONS ===")

    # Look for panels
    for panel in soup.find_all(class_=re.compile(r"ui-panel")):
        title_el = panel.find(class_="ui-panel-title")
        title = title_el.get_text(strip=True) if title_el else "Untitled"
        content_el = panel.find(class_="ui-panel-content")
        if content_el:
            text = content_el.get_text(strip=True)[:500]
            print(f"\nPanel: {title}")
            print(f"  Content: {text[:300]}")

    # Look for fieldsets
    for fs in soup.find_all(class_=re.compile(r"ui-fieldset")):
        legend = fs.find(class_="ui-fieldset-legend")
        title = legend.get_text(strip=True) if legend else "Untitled"
        text = fs.get_text(strip=True)[:300]
        print(f"\nFieldset: {title}")
        print(f"  Content: {text}")

    # Extract all label/value pairs
    print("\n=== ALL KEY-VALUE PAIRS ===")
    kv = {}

    # Method 1: table rows with th/td
    for row in soup.find_all("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) == 2:
            key = cells[0].get_text(strip=True).rstrip(":")
            val = cells[1].get_text(strip=True)
            if key and val and len(key) < 80:
                kv[key] = val
                print(f"  {key}: {val[:200]}")
        elif len(cells) > 2:
            # Could be multi-column layout
            for i in range(0, len(cells) - 1, 2):
                key = cells[i].get_text(strip=True).rstrip(":")
                val = cells[i + 1].get_text(strip=True) if i + 1 < len(cells) else ""
                if key and val and len(key) < 80:
                    kv[key] = val
                    print(f"  {key}: {val[:200]}")

    # Method 2: dt/dd pairs
    for dt, dd in zip(soup.find_all("dt"), soup.find_all("dd")):
        key = dt.get_text(strip=True).rstrip(":")
        val = dd.get_text(strip=True)
        if key and val:
            kv[key] = val
            print(f"  [DL] {key}: {val[:200]}")

    # Method 3: label elements with associated values
    for label in soup.find_all("label"):
        key = label.get_text(strip=True).rstrip(":")
        for_id = label.get("for", "")
        if for_id:
            sibling = soup.find(id=for_id)
            if sibling:
                val = sibling.get_text(strip=True)
                if key and val:
                    kv[key] = val
                    print(f"  [Label] {key}: {val[:200]}")
        # Check next sibling
        ns = label.find_next_sibling()
        if ns and ns.name in ("span", "div", "input", "select"):
            val = ns.get_text(strip=True) or ns.get("value", "")
            if key and val and val not in kv.values():
                kv[key] = val
                print(f"  [LabelSib] {key}: {val[:200]}")

    # Look for document links
    print("\n=== DOCUMENT LINKS ===")
    doc_links = []
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        text = anchor.get_text(strip=True)
        if any(ext in href.lower() for ext in [".pdf", ".doc", ".xls", "/document/", "/download/"]):
            doc_links.append({"text": text, "href": href})
            print(f"  [{text[:60]}] -> {href[:120]}")

    # Look for buttons that might trigger downloads
    for btn in soup.find_all(["button", "input"], type=re.compile(r"submit|button")):
        val = btn.get("value", "") or btn.get_text(strip=True)
        onclick = btn.get("onclick", "")
        if any(kw in (val + onclick).lower() for kw in ["download", "document", "pdf", "view"]):
            print(f"  [Button] {val} onclick={onclick[:120]}")

    # Save extracted data
    with open("serff_detail_kv.json", "w") as f:
        json.dump(kv, f, indent=2)
    print(f"\nSaved {len(kv)} key-value pairs to serff_detail_kv.json")

    # Screenshot
    await page.screenshot(path="serff_detail.png", full_page=True)
    print("Screenshot saved to serff_detail.png")

    # Now go back and click a RATE filing (not just rule/form)
    print("\n\n=== TRYING A RATE FILING ===")
    await page.go_back()
    await asyncio.sleep(3)
    await page.wait_for_load_state("networkidle", timeout=15000)

    # Find a row with "Rate" in filing type
    rows = page.locator("tbody.ui-datatable-data tr")
    row_count = await rows.count()
    for i in range(row_count):
        row = rows.nth(i)
        text = await row.text_content()
        if "rate" in text.lower() and "rule/form" not in text.lower():
            rk = await row.get_attribute("data-rk")
            print(f"Found rate filing at row {i}: data-rk={rk}")
            print(f"  Row text: {text.strip()[:200]}")
            await row.click()
            await page.wait_for_load_state("networkidle", timeout=30000)
            await asyncio.sleep(3)

            rate_content = await page.content()
            rate_soup = BeautifulSoup(rate_content, "lxml")

            print(f"URL: {page.url}")

            # Extract KV pairs
            rate_kv = {}
            for tr in rate_soup.find_all("tr"):
                cells = tr.find_all(["th", "td"])
                if len(cells) == 2:
                    key = cells[0].get_text(strip=True).rstrip(":")
                    val = cells[1].get_text(strip=True)
                    if key and val and len(key) < 80:
                        rate_kv[key] = val

            print(f"\nRate filing KV pairs ({len(rate_kv)}):")
            for k, v in rate_kv.items():
                print(f"  {k}: {v[:200]}")

            # Doc links
            for anchor in rate_soup.find_all("a", href=True):
                href = anchor["href"]
                text = anchor.get_text(strip=True)
                if any(ext in href.lower() for ext in [".pdf", ".doc", ".xls", "/document/", "/download/"]):
                    print(f"  Doc: [{text[:60]}] -> {href[:120]}")

            with open("serff_rate_detail_kv.json", "w") as f:
                json.dump(rate_kv, f, indent=2)

            await page.screenshot(path="serff_rate_detail.png", full_page=True)
            break

    await browser.close()
    await pw.stop()


asyncio.run(probe())
