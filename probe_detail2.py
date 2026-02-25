"""Probe deeper — find how to access filing detail pages on SERFF SFA."""
import asyncio
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

    # Navigate to CA SFA
    await page.goto("https://filingaccess.serff.com/sfa/home/CA", timeout=30000)
    await page.wait_for_load_state("networkidle", timeout=15000)

    # Click "Begin Search"
    await page.click("a[href='/sfa/userAgreement.xhtml']")
    await page.wait_for_load_state("networkidle", timeout=15000)

    # Accept agreement
    accept = page.locator(".ui-button").first
    await accept.click()
    await page.wait_for_load_state("networkidle", timeout=20000)
    await asyncio.sleep(2)

    # Select Property & Casualty
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

    await page.fill("input[id='simpleSearch:productName']", "Commercial Property", timeout=3000)
    await page.locator("button[id='simpleSearch:saveBtn']").click()
    await page.wait_for_load_state("networkidle", timeout=60000)
    await asyncio.sleep(3)

    # Now examine the results table structure in detail
    content = await page.content()
    soup = BeautifulSoup(content, "lxml")

    # Find the datatable
    datatable = soup.find("tbody", class_="ui-datatable-data")
    if not datatable:
        datatable = soup.find("table", class_="ui-datatable-data")

    if datatable:
        first_row = datatable.find("tr")
        if first_row:
            print("=== FIRST ROW HTML ===")
            print(str(first_row)[:3000])
            print()

            # Check for all event attributes
            for attr in first_row.attrs:
                print(f"  Row attr: {attr}={first_row[attr]}")

            # Check all cells
            cells = first_row.find_all("td")
            for i, cell in enumerate(cells):
                print(f"\n  Cell {i}: text='{cell.get_text(strip=True)[:60]}'")
                for attr in cell.attrs:
                    print(f"    attr: {attr}={cell[attr]}")
                # Check children
                for child in cell.children:
                    if hasattr(child, 'name') and child.name:
                        print(f"    child: <{child.name}> attrs={dict(child.attrs)}")
                        if child.name == 'a':
                            print(f"      href={child.get('href', 'NONE')}")
                            print(f"      onclick={child.get('onclick', 'NONE')}")

    # Check ALL links on the page
    print("\n=== ALL LINKS ON PAGE ===")
    all_links = soup.find_all("a")
    for link in all_links:
        href = link.get("href", "")
        onclick = link.get("onclick", "")
        text = link.get_text(strip=True)[:50]
        if href or onclick:
            if "filing" in (href + onclick + text).lower() or "detail" in (href + onclick + text).lower() or "view" in (href + onclick + text).lower():
                print(f"  [{text}] href={href} onclick={onclick[:100]}")

    # Check for PrimeFaces row click events
    print("\n=== PRIMEFACES SCRIPTS ===")
    scripts = soup.find_all("script")
    for script in scripts:
        script_text = script.string or ""
        if "rowSelect" in script_text or "row" in script_text.lower() and "click" in script_text.lower():
            print(f"  Script: {script_text[:500]}")

    # Try clicking a row to see if it triggers navigation
    print("\n=== TRYING ROW CLICK ===")
    first_data_row = page.locator("tbody.ui-datatable-data tr").first
    if await first_data_row.count() > 0:
        # Get URL before click
        url_before = page.url
        print(f"URL before click: {url_before}")

        # Click the row
        await first_data_row.click()
        await asyncio.sleep(3)
        await page.wait_for_load_state("networkidle", timeout=15000)

        url_after = page.url
        print(f"URL after row click: {url_after}")

        if url_after != url_before:
            print("URL CHANGED — row click navigates to detail!")
        else:
            # Check if content changed (maybe AJAX loaded detail)
            new_content = await page.content()
            if len(new_content) != len(content):
                print(f"Content length changed: {len(content)} -> {len(new_content)}")

            # Check for any new panels or overlays
            new_soup = BeautifulSoup(new_content, "lxml")
            panels = new_soup.find_all(class_=re.compile(r"ui-dialog|ui-panel|detail"))
            print(f"Panels/dialogs after click: {len(panels)}")
            for p in panels:
                header = p.find(class_=re.compile(r"ui-dialog-title|ui-panel-title"))
                if header:
                    print(f"  Panel: {header.get_text(strip=True)}")

            # Save after-click HTML
            with open("serff_after_click.html", "w", encoding="utf-8") as f:
                f.write(new_content)
            print("After-click HTML saved to serff_after_click.html")

        await page.screenshot(path="serff_after_click.png", full_page=True)
        print("Screenshot saved to serff_after_click.png")

    # Also try double-clicking
    print("\n=== TRYING DOUBLE CLICK ===")
    # First navigate back to search if needed
    if page.url != url_before:
        await page.go_back()
        await asyncio.sleep(3)

    first_data_row = page.locator("tbody.ui-datatable-data tr").first
    if await first_data_row.count() > 0:
        await first_data_row.dblclick()
        await asyncio.sleep(3)
        await page.wait_for_load_state("networkidle", timeout=15000)
        print(f"URL after dblclick: {page.url}")

        new_content2 = await page.content()
        if len(new_content2) != len(content):
            print(f"Content length changed: {len(content)} -> {len(new_content2)}")
            with open("serff_after_dblclick.html", "w", encoding="utf-8") as f:
                f.write(new_content2)

        await page.screenshot(path="serff_after_dblclick.png", full_page=True)

    await browser.close()
    await pw.stop()


asyncio.run(probe())
