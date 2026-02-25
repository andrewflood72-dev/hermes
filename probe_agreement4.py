"""Check if SERFF is blocking us entirely or just CA."""
import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        for state in ["NY", "TX", "OH", "CA"]:
            page = await browser.new_page()
            url = f"https://filingaccess.serff.com/sfa/home/{state}"
            try:
                resp = await page.goto(url, wait_until="load", timeout=15000)
                status = resp.status if resp else "no response"
                title = await page.title()
                html_len = len(await page.content())
                print(f"{state}: status={status} title={title!r} html_len={html_len}")
            except Exception as e:
                print(f"{state}: ERROR {e}")
            await page.close()

        # Try NY with full flow
        print("\n--- NY full flow test ---")
        page = await browser.new_page()
        await page.goto("https://filingaccess.serff.com/sfa/home/NY", wait_until="load", timeout=15000)
        await asyncio.sleep(2)
        title = await page.title()
        print(f"Home: title={title!r} url={page.url}")

        # Dump home page content
        html = await page.content()
        print(f"HTML length: {len(html)}")
        if len(html) > 200:
            print(html[:3000])

        await browser.close()

asyncio.run(main())
