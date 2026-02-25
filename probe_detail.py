"""Probe a single SERFF SFA filing detail page to understand its structure."""
import asyncio
import json
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

    # Navigate to CA SFA
    print("Navigating to SERFF SFA CA...")
    await page.goto("https://filingaccess.serff.com/sfa/home/CA", timeout=30000)
    await page.wait_for_load_state("networkidle", timeout=15000)

    # Click "Begin Search"
    print("Clicking Begin Search...")
    await page.click("a[href='/sfa/userAgreement.xhtml']")
    await page.wait_for_load_state("networkidle", timeout=15000)

    # Accept agreement
    print("Accepting agreement...")
    accept = page.locator(".ui-button").first
    await accept.click()
    await page.wait_for_load_state("networkidle", timeout=20000)
    await asyncio.sleep(2)

    # Select Property & Casualty via PrimeFaces UI
    print("Selecting Business Type...")
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
        print(f"Business type selection: {e}")

    # Fill product name to limit results
    print("Filling product name: Commercial Property")
    await page.fill("input[id='simpleSearch:productName']", "Commercial Property", timeout=3000)

    # Submit search
    print("Submitting search...")
    await page.locator("button[id='simpleSearch:saveBtn']").click()
    await page.wait_for_load_state("networkidle", timeout=60000)
    await asyncio.sleep(3)

    # Find all clickable links in the results table
    print("\n=== RESULTS TABLE ===")
    rows = await page.locator("table.ui-datatable-data tr, tbody.ui-datatable-data tr").all()
    print(f"Result rows: {len(rows)}")

    # Look at first few rows for clickable links
    for i, row in enumerate(rows[:3]):
        cells = await row.locator("td").all()
        if not cells:
            continue
        cell_texts = []
        for c in cells:
            txt = (await c.text_content()).strip()
            cell_texts.append(txt[:60])
        print(f"  Row {i}: {cell_texts}")

        # Check for links in this row
        links = await row.locator("a").all()
        for link in links:
            href = await link.get_attribute("href") or ""
            onclick = await link.get_attribute("onclick") or ""
            text = (await link.text_content()).strip()
            print(f"    Link: [{text[:50]}] href={href[:80]} onclick={onclick[:100]}")

    # Click the first SERFF tracking number link
    print("\n=== CLICKING INTO FIRST FILING ===")
    first_link = page.locator("table.ui-datatable-data a, tbody.ui-datatable-data a").first
    if await first_link.count() > 0:
        link_text = (await first_link.text_content()).strip()
        print(f"Clicking: {link_text}")
        await first_link.click()
        await page.wait_for_load_state("networkidle", timeout=30000)
        await asyncio.sleep(3)

        print(f"URL after click: {page.url}")
        print(f"Title: {await page.title()}")

        # Parse the detail page
        content = await page.content()
        soup = BeautifulSoup(content, "lxml")

        # Extract all key-value pairs from tables
        print("\n=== DETAIL PAGE KEY-VALUE PAIRS ===")
        kv_pairs = {}
        for row in soup.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                if key and value and len(key) < 100:
                    kv_pairs[key] = value[:200]
                    print(f"  {key}: {value[:200]}")

        # Also check dt/dd pairs
        for dt, dd in zip(soup.find_all("dt"), soup.find_all("dd")):
            key = dt.get_text(strip=True)
            value = dd.get_text(strip=True)
            if key and value:
                kv_pairs[key] = value[:200]
                print(f"  [DL] {key}: {value[:200]}")

        # Look for document links
        print("\n=== DOCUMENT LINKS ===")
        import re
        doc_pattern = re.compile(r"(\.pdf|\.doc|\.xls|/document/|/download/)", re.I)
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            if doc_pattern.search(href):
                link_text = anchor.get_text(strip=True)
                print(f"  [{link_text[:60]}] -> {href[:120]}")

        # Look for PrimeFaces panels/fieldsets
        print("\n=== PANELS/SECTIONS ===")
        for panel in soup.find_all(class_=re.compile(r"ui-panel|ui-fieldset")):
            header = panel.find(class_=re.compile(r"ui-panel-title|ui-fieldset-legend"))
            if header:
                print(f"  Panel: {header.get_text(strip=True)}")

        # Look for any iframe or embedded viewer
        iframes = soup.find_all("iframe")
        print(f"\nIframes: {len(iframes)}")
        for iframe in iframes:
            print(f"  src={iframe.get('src', '')[:120]}")

        # Save full HTML for inspection
        with open("serff_detail_page.html", "w", encoding="utf-8") as f:
            f.write(content)
        print("\nFull HTML saved to serff_detail_page.html")

        # Screenshot
        await page.screenshot(path="serff_detail.png", full_page=True)
        print("Screenshot saved to serff_detail.png")

        # Dump raw metadata as JSON
        with open("serff_detail_kv.json", "w") as f:
            json.dump(kv_pairs, f, indent=2)
        print("KV pairs saved to serff_detail_kv.json")

    else:
        print("No clickable link found in results table!")

    await browser.close()
    await pw.stop()


asyncio.run(probe())
