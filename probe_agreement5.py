"""Test SERFF access with anti-detection measures."""
import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:

        # Test 1: With user-agent override
        print("=== Test 1: Custom user-agent ===")
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        )
        page = await ctx.new_page()
        resp = await page.goto("https://filingaccess.serff.com/sfa/home/NY", wait_until="load", timeout=15000)
        print(f"  Status: {resp.status if resp else 'none'}, Title: {await page.title()}")
        await browser.close()

        # Test 2: With extra headers
        print("\n=== Test 2: Extra headers ===")
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
            },
        )
        page = await ctx.new_page()
        resp = await page.goto("https://filingaccess.serff.com/sfa/home/NY", wait_until="load", timeout=15000)
        print(f"  Status: {resp.status if resp else 'none'}, Title: {await page.title()}")
        html = await page.content()
        if resp and resp.status == 200:
            print(f"  HTML length: {len(html)}")
            print(html[:1000])
        await browser.close()

        # Test 3: Headed mode (non-headless)
        print("\n=== Test 3: Headed mode (non-headless) ===")
        try:
            browser = await p.chromium.launch(headless=False)
            page = await browser.new_page()
            resp = await page.goto("https://filingaccess.serff.com/sfa/home/NY", wait_until="load", timeout=15000)
            print(f"  Status: {resp.status if resp else 'none'}, Title: {await page.title()}")
            html = await page.content()
            if resp and resp.status == 200:
                print(f"  HTML length: {len(html)}")
                print(html[:1000])
            await browser.close()
        except Exception as e:
            print(f"  Headed mode error: {e}")

        # Test 4: Chromium args to avoid detection
        print("\n=== Test 4: Stealth-like args ===")
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        )
        # Remove webdriver property
        await ctx.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = { runtime: {} };
        """)
        page = await ctx.new_page()
        resp = await page.goto("https://filingaccess.serff.com/sfa/home/NY", wait_until="load", timeout=15000)
        print(f"  Status: {resp.status if resp else 'none'}, Title: {await page.title()}")
        html = await page.content()
        if resp and resp.status == 200:
            print(f"  HTML length: {len(html)}")
            print(html[:1000])
        await browser.close()

asyncio.run(main())
