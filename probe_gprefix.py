"""Test if G-prefix filing IDs work on SERFF."""
import asyncio
from playwright.async_api import async_playwright

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        ctx = await browser.new_context(user_agent=UA, viewport={"width": 1280, "height": 900})
        page = await ctx.new_page()

        # Home first (sets state cookie)
        await page.goto("https://filingaccess.serff.com/sfa/home/CA", wait_until="load", timeout=20000)
        await asyncio.sleep(1)

        # Click Begin
        try:
            link = page.locator("a:has-text('Begin')")
            await link.click(timeout=5000)
            await page.wait_for_load_state("load", timeout=10000)
        except Exception as e:
            print(f"Begin failed: {e}")

        print(f"After begin: {page.url}")

        # Click Accept if needed
        if "agreement" in page.url.lower():
            try:
                btn = page.locator("button:has-text('Accept')").first
                await btn.click(timeout=5000)
                try:
                    await page.wait_for_url("**/filingSearch**", timeout=15000)
                except:
                    await asyncio.sleep(3)
            except Exception as e:
                print(f"Accept failed: {e}")

        print(f"Session: {page.url}")
        if "filingSearch" not in page.url:
            print("FAILED!")
            await browser.close()
            return

        # Test various filing IDs
        test_ids = [
            ("G133973962", "G-prefix CA filing"),
            ("133973962", "stripped G-prefix"),
            ("134171153", "known good CA numeric"),
        ]
        for fid, desc in test_ids:
            detail = await ctx.new_page()
            url = f"https://filingaccess.serff.com/sfa/search/filingSummary.xhtml?filingId={fid}"
            await detail.goto(url, wait_until="load", timeout=15000)
            html = await detail.content()
            if "Filing Summary" in html or "SERFF Tracking" in html:
                print(f"  {fid:>12s} ({desc}): OK")
            elif "unauthorized" in detail.url:
                print(f"  {fid:>12s} ({desc}): UNAUTHORIZED")
            elif "sessionExpired" in detail.url:
                print(f"  {fid:>12s} ({desc}): SESSION EXPIRED")
            else:
                snip = html[html.find("<body"):html.find("<body")+200] if "<body" in html else html[:200]
                print(f"  {fid:>12s} ({desc}): {detail.url.split('/')[-1][:50]}")
            await detail.close()

        await browser.close()

asyncio.run(main())
