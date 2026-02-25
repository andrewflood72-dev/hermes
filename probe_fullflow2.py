"""Test: click Accept then navigate to search page directly."""
import asyncio
from playwright.async_api import async_playwright

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(user_agent=UA)
        page = await ctx.new_page()

        # Home
        await page.goto("https://filingaccess.serff.com/sfa/home/CA", wait_until="networkidle", timeout=30000)
        print(f"Home: {page.url}")

        # Agreement
        await page.click("a:has-text('Begin')", timeout=5000)
        await page.wait_for_load_state("networkidle", timeout=15000)
        print(f"Agreement: {page.url}")

        # Accept
        await page.click("button:has-text('Accept')", timeout=5000)
        # Wait for potential navigation
        try:
            await page.wait_for_url("**/filingSearch**", timeout=10000)
            print(f"After accept (auto-nav): {page.url}")
        except:
            print(f"After accept (no nav): {page.url}")
            # Try navigating manually to search
            await page.goto("https://filingaccess.serff.com/sfa/search/filingSearch.xhtml",
                          wait_until="networkidle", timeout=15000)
            print(f"Manual nav to search: {page.url}  Title: {await page.title()}")

        # Check if we're on search form
        if "filingSearch" in page.url:
            print("\n*** On search form! ***")
            # Try to find the search elements
            has_search = await page.locator("input[id*='productName'], .ui-selectonemenu").count()
            print(f"  Search elements found: {has_search}")

            # Now test opening a detail page directly
            print("\nTesting direct detail URL...")
            detail_page = await ctx.new_page()
            await detail_page.goto(
                "https://filingaccess.serff.com/sfa/search/filingSummary.xhtml?filingId=132395430",
                wait_until="networkidle", timeout=15000
            )
            title = await detail_page.title()
            print(f"  Detail URL: {detail_page.url}")
            print(f"  Detail Title: {title}")
            html = await detail_page.content()
            # Check for real content vs error
            if "Filing Summary" in html or "SERFF Tracking" in html:
                print("  *** Detail page loaded successfully! ***")
                # Extract a few fields
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(html, "html.parser")
                for label in soup.find_all("label"):
                    txt = label.get_text(strip=True)
                    if any(k in txt.lower() for k in ["tracking", "company", "filed date", "rate"]):
                        sibling = label.find_next_sibling()
                        val = sibling.get_text(strip=True) if sibling else "?"
                        print(f"  {txt}: {val}")
            elif "403" in title or "Forbidden" in html:
                print("  *** 403 on detail page ***")
            elif "agreement" in detail_page.url.lower():
                print("  *** Redirected to agreement ***")
            else:
                print(f"  Page content (first 500): {html[:500]}")
            await detail_page.close()
        else:
            html = await page.content()
            print(f"  Not on search. HTML: {html[:500]}")

        await browser.close()

asyncio.run(main())
