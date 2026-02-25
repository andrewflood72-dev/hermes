"""Capture SERFF SFA search results page structure."""
import asyncio
from playwright.async_api import async_playwright


async def explore():
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
        )
    )
    page = await context.new_page()

    # Navigate: Home → Agreement → Search
    await page.goto("https://filingaccess.serff.com/sfa/home/CA", timeout=30000)
    await page.wait_for_load_state("networkidle", timeout=15000)

    await page.click("a[href='/sfa/userAgreement.xhtml']")
    await page.wait_for_load_state("networkidle", timeout=15000)

    # Accept agreement
    accept = page.locator(".ui-button").first
    await accept.click()
    await page.wait_for_load_state("networkidle", timeout=20000)
    await asyncio.sleep(2)

    # Select Property & Casualty
    await page.select_option(
        "select[id='simpleSearch:businessType_input']",
        label="Property & Casualty",
        timeout=5000,
    )

    # Fill product name
    await page.fill(
        "input[id='simpleSearch:productName']",
        "Commercial Property",
        timeout=3000,
    )

    # Fill date
    await page.fill(
        "input[id='simpleSearch:submissionStartDate_input']",
        "01/01/2025",
        timeout=3000,
    )

    # Submit
    await page.locator("button[id='simpleSearch:saveBtn']").click()
    await page.wait_for_load_state("networkidle", timeout=60000)
    await asyncio.sleep(3)

    print("After search URL:", page.url)
    print("Title:", await page.title())

    # Check for results
    content = await page.content()
    print(f"Page HTML length: {len(content)}")

    # Look for data tables
    tables = await page.locator("table").all()
    print(f"\nTables found: {len(tables)}")
    for i, t in enumerate(tables[:5]):
        rows = await t.locator("tr").all()
        print(f"  Table {i}: {len(rows)} rows")
        if rows:
            first_row = await rows[0].text_content()
            print(f"    First row: {first_row[:200]}")

    # Look for PrimeFaces data table
    pf_tables = await page.locator(".ui-datatable").all()
    print(f"\nPF DataTables: {len(pf_tables)}")
    for i, t in enumerate(pf_tables[:3]):
        rows = await t.locator("tr").all()
        print(f"  PF Table {i}: {len(rows)} rows")
        if len(rows) > 1:
            for j, r in enumerate(rows[:3]):
                text = (await r.text_content()).strip()[:200]
                print(f"    Row {j}: {text}")

    # Look for "no results" messages
    body_text = await page.locator("body").text_content()
    if "no" in body_text.lower() and "result" in body_text.lower():
        # Find the exact message
        for phrase in ["no results", "no records", "no filings", "not found", "0 results"]:
            if phrase in body_text.lower():
                idx = body_text.lower().index(phrase)
                print(f"\nFound: ...{body_text[max(0,idx-50):idx+100]}...")
                break

    # Check for any links that look like filing detail links
    links = await page.locator("a[href*='filing'], a[href*='detail'], a[href*='SERFF']").all()
    print(f"\nFiling-related links: {len(links)}")
    for link in links[:10]:
        href = await link.get_attribute("href")
        text = (await link.text_content()).strip()
        print(f"  [{text[:50]}] -> {href}")

    # Save page screenshot for visual inspection
    await page.screenshot(path="serff_results.png", full_page=True)
    print("\nScreenshot saved to serff_results.png")

    await browser.close()
    await pw.stop()


asyncio.run(explore())
