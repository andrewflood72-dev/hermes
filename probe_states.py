"""Test detail page access per state with correct filing IDs."""
import asyncio
from playwright.async_api import async_playwright

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

# One filing ID per state (numeric part after the dash)
TEST_IDS = {
    "NY": "133483022",  # AAAN-133483022
    "CA": "134171153",  # ACCD-134171153
    "TX": "134135730",  # AACI-134135730
    "OH": "134719356",  # ACCD-134719356
}

async def test_state(state: str, filing_id: str):
    print(f"\n{'='*50}")
    print(f"{state}: filingId={filing_id}")
    print(f"{'='*50}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        ctx = await browser.new_context(user_agent=UA, viewport={"width": 1280, "height": 900})
        page = await ctx.new_page()

        # Home → Begin → Accept
        await page.goto(f"https://filingaccess.serff.com/sfa/home/{state}", wait_until="load", timeout=20000)
        try:
            begin = page.locator("a:has-text('Begin')")
            if await begin.is_visible(timeout=5000):
                await begin.click()
                await page.wait_for_load_state("load", timeout=10000)
        except:
            await page.goto("https://filingaccess.serff.com/sfa/userAgreement.xhtml", wait_until="load", timeout=15000)

        if "filingSearch" not in page.url:
            btn = page.locator("button:has-text('Accept')").first
            if await btn.is_visible(timeout=3000):
                await btn.click()
                try:
                    await page.wait_for_url("**/filingSearch**", timeout=15000)
                except:
                    await asyncio.sleep(2)

        print(f"  Session: {page.url}")
        if "filingSearch" not in page.url:
            print("  FAILED to reach search form")
            await browser.close()
            return

        # Open detail in new tab (shares session cookies)
        detail = await ctx.new_page()
        detail_url = f"https://filingaccess.serff.com/sfa/search/filingSummary.xhtml?filingId={filing_id}"
        await detail.goto(detail_url, wait_until="load", timeout=15000)
        print(f"  Detail: {detail.url}")

        html = await detail.content()
        if "Filing Summary" in html or "SERFF Tracking" in html:
            print("  DETAIL PAGE LOADED!")
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            for label in soup.find_all("label"):
                txt = label.get_text(strip=True)
                if any(k in txt.lower() for k in ["tracking", "company", "filed", "effective", "disposition", "description"]):
                    sibling = label.find_next_sibling()
                    val = sibling.get_text(strip=True)[:80] if sibling else "?"
                    print(f"    {txt}: {val}")
        elif "unauthorized" in detail.url:
            print("  UNAUTHORIZED!")
        elif "agreement" in detail.url.lower():
            print("  REDIRECTED TO AGREEMENT!")
        else:
            print(f"  Unknown: {html[:300]}")

        await browser.close()

async def main():
    for state, fid in TEST_IDS.items():
        await test_state(state, fid)

asyncio.run(main())
