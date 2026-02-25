"""Test full SERFF flow with user-agent fix: home → agreement → search → detail."""
import asyncio
from playwright.async_api import async_playwright

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(user_agent=UA)
        page = await ctx.new_page()

        # Step 1: Home
        print("Step 1: Home page...")
        await page.goto("https://filingaccess.serff.com/sfa/home/CA", wait_until="networkidle", timeout=30000)
        print(f"  URL: {page.url}  Title: {await page.title()}")

        # Find Begin Search link
        links = await page.locator("a").all()
        for a in links:
            txt = (await a.text_content()).strip()
            href = await a.get_attribute("href") or ""
            if txt:
                print(f"  Link: {txt!r} -> {href}")

        # Step 2: Click Begin Search
        print("\nStep 2: Begin Search...")
        try:
            await page.click("a:has-text('Begin')", timeout=5000)
            await page.wait_for_load_state("networkidle", timeout=15000)
        except:
            # Try direct link
            for a in await page.locator("a").all():
                href = await a.get_attribute("href") or ""
                if "agreement" in href.lower() or "search" in href.lower():
                    await a.click()
                    await page.wait_for_load_state("networkidle", timeout=15000)
                    break
        print(f"  URL: {page.url}  Title: {await page.title()}")

        # Step 3: Agreement page — find and click accept
        print("\nStep 3: Agreement page...")
        # Find all buttons
        for btn in await page.locator("button").all():
            txt = await btn.text_content()
            cls = await btn.get_attribute("class") or ""
            visible = await btn.is_visible()
            print(f"  Button: text={txt!r} class={cls!r} visible={visible}")

        for inp in await page.locator("input[type='submit'], input[type='button']").all():
            val = await inp.get_attribute("value") or ""
            visible = await inp.is_visible()
            print(f"  Input: value={val!r} visible={visible}")

        # Click accept
        accepted = False
        for selector in [
            "button:has-text('Accept')",
            "button:has-text('Agree')",
            ".ui-button",
            "input[value*='Accept']",
            "input[value*='Agree']",
            "button.ui-button",
        ]:
            try:
                el = page.locator(selector).first
                if await el.is_visible(timeout=2000):
                    txt = await el.text_content() if await el.evaluate("e => e.tagName") != "INPUT" else await el.get_attribute("value")
                    print(f"  Clicking: {selector} ({txt!r})")
                    await el.click()
                    await page.wait_for_load_state("networkidle", timeout=20000)
                    accepted = True
                    break
            except:
                continue

        print(f"  Accepted: {accepted}")
        print(f"  URL: {page.url}  Title: {await page.title()}")

        # Step 4: Are we on search form?
        if "filingSearch" in page.url:
            print("\n*** SUCCESS: Reached search form! ***")
        else:
            print(f"\n  Not on search form, dumping page...")
            html = await page.content()
            print(html[:3000])

        await browser.close()

asyncio.run(main())
