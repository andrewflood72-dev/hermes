"""Debug why detail pages return empty metadata."""
import asyncio
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        ctx = await browser.new_context(user_agent=UA, viewport={"width": 1280, "height": 900})
        page = await ctx.new_page()

        # Session
        await page.goto("https://filingaccess.serff.com/sfa/home/NY", wait_until="load", timeout=30000)
        await asyncio.sleep(2)
        begin = page.locator("a:has-text('Begin')")
        await begin.wait_for(state="visible", timeout=10000)
        await begin.click()
        await asyncio.sleep(3)
        accept = page.locator("button:has-text('Accept')")
        await accept.wait_for(state="visible", timeout=10000)
        await accept.click()
        try:
            await page.wait_for_url("**/filingSearch**", timeout=15000)
        except Exception:
            await asyncio.sleep(3)
        print(f"Session: {page.url}\n")

        # Test a known "empty metadata" filing AND a known good one
        tests = [
            ("SMPJ-134638650", "134638650", "EMPTY"),
            ("LBRC-134395366", "134395366", "500"),
            ("CUNA-134641935", "134641935", "GOOD (previously worked)"),
        ]

        for name, fid, expected in tests:
            print(f"{'='*70}")
            print(f"Testing {name} (expected: {expected})")
            detail = await ctx.new_page()
            url = f"https://filingaccess.serff.com/sfa/search/filingSummary.xhtml?filingId={fid}"
            await detail.goto(url, timeout=20000)
            await asyncio.sleep(3)

            final_url = detail.url
            title = await detail.title()
            html = await detail.content()

            print(f"  URL: {final_url}")
            print(f"  Title: {title}")
            print(f"  HTML length: {len(html)}")

            if "filingSummary" in final_url:
                soup = BeautifulSoup(html, "html.parser")

                # Check labels
                labels = soup.find_all("label")
                print(f"  Labels found: {len(labels)}")
                for lbl in labels[:10]:
                    txt = lbl.get_text(strip=True)
                    for_id = lbl.get("for", "")
                    # Check for target and sibling
                    target = soup.find(id=for_id) if for_id else None
                    tval = target.get_text(strip=True)[:60] if target else ""
                    sib = lbl.find_next_sibling()
                    sval = ""
                    if sib:
                        sval = sib.get_text(strip=True)[:60]
                        stag = sib.name
                    else:
                        stag = "none"
                    print(f"    {txt!r}: for_target={tval!r}, sibling<{stag}>={sval!r}")

                # Check table rows
                rows = soup.find_all("tr")
                print(f"  Table rows: {len(rows)}")
                for row in rows[:5]:
                    cells = row.find_all(["th", "td"])
                    if len(cells) == 2:
                        print(f"    {cells[0].get_text(strip=True)}: {cells[1].get_text(strip=True)[:60]}")

                # Check panels
                panels = soup.find_all(class_="ui-panel")
                print(f"  Panels: {len(panels)}")

                # Show body text snippet
                body = soup.get_text()[:600].replace("\n", " ").strip()
                print(f"  Body text: {body[:300]}")

            await detail.close()
            await asyncio.sleep(1)

        await browser.close()


asyncio.run(main())
