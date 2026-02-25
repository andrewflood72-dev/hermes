"""Deep probe of SERFF SFA rate filing detail page — find rate change data and PDF links."""
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

    # Navigate to search
    await page.goto("https://filingaccess.serff.com/sfa/home/CA", timeout=30000)
    await page.wait_for_load_state("networkidle", timeout=15000)
    await page.click("a[href='/sfa/userAgreement.xhtml']")
    await page.wait_for_load_state("networkidle", timeout=15000)
    await page.locator(".ui-button").first.click()
    await page.wait_for_load_state("networkidle", timeout=20000)
    await asyncio.sleep(2)

    # Select P&C
    trigger = page.locator(".ui-selectonemenu-trigger").first
    await trigger.click(timeout=5000)
    await asyncio.sleep(1)
    panel = page.locator(".ui-selectonemenu-panel").first
    for item in await panel.locator("li").all():
        if "property" in (await item.text_content()).strip().lower():
            await item.click()
            break
    await page.wait_for_load_state("networkidle", timeout=15000)
    await asyncio.sleep(2)

    await page.fill("input[id='simpleSearch:productName']", "Commercial Property", timeout=3000)
    await page.locator("button[id='simpleSearch:saveBtn']").click()
    await page.wait_for_load_state("networkidle", timeout=60000)
    await asyncio.sleep(3)

    # Find a Rate filing (pure rate, not rate/rule)
    rows = page.locator("tbody.ui-datatable-data tr")
    row_count = await rows.count()
    for i in range(row_count):
        row = rows.nth(i)
        text = await row.text_content()
        # Look for pure "Rate" filing type
        cells = await row.locator("td").all()
        if len(cells) >= 6:
            filing_type = (await cells[4].text_content()).strip()
            if filing_type == "Rate" or filing_type == "Rate/Rule":
                rk = await row.get_attribute("data-rk")
                serff = (await cells[6].text_content()).strip() if len(cells) > 6 else "?"
                carrier = (await cells[0].text_content()).strip()
                print(f"Row {i}: [{filing_type}] {carrier} — {serff} (rk={rk})")

                # Click it
                await row.click()
                await page.wait_for_load_state("networkidle", timeout=30000)
                await asyncio.sleep(3)

                content = await page.content()
                soup = BeautifulSoup(content, "lxml")

                print(f"\nURL: {page.url}")

                # Extract ALL label-sibling pairs
                print("\n=== LABEL-VALUE PAIRS ===")
                kv = {}
                for label in soup.find_all("label"):
                    key = label.get_text(strip=True).rstrip(":")
                    if not key:
                        continue
                    # Try next sibling
                    ns = label.find_next_sibling()
                    if ns:
                        val = ns.get_text(strip=True) or ns.get("value", "")
                        if val:
                            kv[key] = val
                            print(f"  {key}: {val[:200]}")

                # Also extract output/span elements with IDs
                print("\n=== OUTPUT ELEMENTS ===")
                for el in soup.find_all(["span", "div", "output"], id=True):
                    if el.get_text(strip=True):
                        print(f"  #{el['id']}: {el.get_text(strip=True)[:200]}")

                # Look for rate-specific fields
                print("\n=== RATE-SPECIFIC SEARCH ===")
                body_text = soup.get_text()
                for keyword in ["rate change", "overall", "impact", "policyholders",
                                "effective date", "rate increase", "rate decrease",
                                "percentage", "minimum", "maximum", "avg", "average",
                                "premium impact", "rate level"]:
                    idx = body_text.lower().find(keyword)
                    if idx >= 0:
                        context = body_text[max(0, idx-30):idx+100].strip()
                        print(f"  Found '{keyword}' at pos {idx}: ...{context}...")

                # Extract ALL panels with full content
                print("\n=== PANELS ===")
                for p in soup.find_all(class_=re.compile(r"ui-panel")):
                    title_el = p.find(class_="ui-panel-title")
                    title = title_el.get_text(strip=True) if title_el else "Untitled"
                    content_el = p.find(class_="ui-panel-content")
                    if content_el:
                        text = content_el.get_text(strip=True)[:500]
                        print(f"\n  [{title}]")
                        print(f"  {text[:400]}")

                # Find ALL anchor tags (even without obvious doc extensions)
                print("\n=== ALL ANCHORS ===")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    text = a.get_text(strip=True)
                    if text and href and href != "#" and not href.startswith("javascript:"):
                        print(f"  [{text[:60]}] -> {href[:150]}")

                # Find download buttons
                print("\n=== BUTTONS ===")
                for btn in soup.find_all(["button", "a"], class_=re.compile(r"ui-button|btn")):
                    text = btn.get_text(strip=True)
                    onclick = btn.get("onclick", "")[:150]
                    href = btn.get("href", "")
                    if text:
                        print(f"  [{text[:60]}] onclick={onclick} href={href}")

                # Look for tab panels that might have rate information
                print("\n=== TAB PANELS ===")
                for tab in soup.find_all(class_=re.compile(r"ui-tabs-panel|ui-tab")):
                    title_tab = tab.get("aria-label", "") or tab.get("id", "")
                    text = tab.get_text(strip=True)[:200]
                    print(f"  Tab '{title_tab}': {text}")

                # Save full detail page
                with open(f"serff_rate_detail_{rk}.html", "w", encoding="utf-8") as f:
                    f.write(content)
                with open(f"serff_rate_detail_{rk}.json", "w") as f:
                    json.dump(kv, f, indent=2)
                await page.screenshot(path=f"serff_rate_detail_{rk}.png", full_page=True)

                print(f"\nSaved HTML, JSON, and screenshot for filing {rk}")
                break

    await browser.close()
    await pw.stop()


asyncio.run(probe())
