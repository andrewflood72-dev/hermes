"""Probe the SERFF SFA agreement page - dump full HTML."""
import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Go directly to agreement page
        print("Navigating to agreement page...")
        await page.goto("https://filingaccess.serff.com/sfa/userAgreement.xhtml",
                       wait_until="load", timeout=30000)
        # Wait extra for JS
        await asyncio.sleep(3)

        print(f"URL: {page.url}")
        print(f"Title: {await page.title()}")

        html = await page.content()
        print(f"\nHTML length: {len(html)}")
        print("\n=== FULL HTML (first 5000 chars) ===")
        print(html[:5000])

        if len(html) > 5000:
            print("\n=== LAST 2000 chars ===")
            print(html[-2000:])

        await browser.close()

asyncio.run(main())
