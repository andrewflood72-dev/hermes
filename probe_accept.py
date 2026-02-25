"""Quick test: can we click Accept on CA and NY agreement pages?"""
import asyncio
from playwright.async_api import async_playwright

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

async def test_state(state: str):
    print(f"\n{'='*50}")
    print(f"Testing {state}")
    print(f"{'='*50}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        ctx = await browser.new_context(user_agent=UA, viewport={"width": 1280, "height": 900})
        page = await ctx.new_page()

        # Home
        await page.goto(f"https://filingaccess.serff.com/sfa/home/{state}", wait_until="load", timeout=20000)
        print(f"  Home: {page.url}")

        # Click Begin
        try:
            begin = page.locator("a:has-text('Begin')")
            if await begin.is_visible(timeout=5000):
                await begin.click()
                await page.wait_for_load_state("load", timeout=10000)
        except Exception as e:
            print(f"  Begin click failed: {e}")
            await page.goto("https://filingaccess.serff.com/sfa/userAgreement.xhtml",
                          wait_until="load", timeout=15000)

        print(f"  Agreement: {page.url}  Title: {await page.title()}")

        if "filingSearch" in page.url:
            print("  Already on search — session still valid!")
            await browser.close()
            return True

        # Check what's visible
        for selector in [
            "button:has-text('Accept')",
            "button.ui-button-text-only:has-text('Accept')",
            ".ui-button:first-of-type",
            "input[value*='Accept']",
        ]:
            try:
                el = page.locator(selector).first
                vis = await el.is_visible(timeout=2000)
                txt = await el.text_content() if vis else "N/A"
                print(f"  Selector '{selector}': visible={vis} text={txt!r}")
            except Exception as e:
                print(f"  Selector '{selector}': error={e}")

        # Try clicking Accept
        try:
            btn = page.locator("button:has-text('Accept')").first
            if await btn.is_visible(timeout=3000):
                print("  Clicking Accept...")
                await btn.click()
                try:
                    await page.wait_for_url("**/filingSearch**", timeout=15000)
                    print(f"  SUCCESS: {page.url}")
                except:
                    await asyncio.sleep(2)
                    print(f"  After click: {page.url}")
            else:
                print("  Accept button NOT visible!")
        except Exception as e:
            print(f"  Click failed: {e}")

        # Now test opening a detail page in same context
        if "filingSearch" in page.url:
            print("\n  Testing detail page in same session...")
            detail = await ctx.new_page()
            await detail.goto(
                "https://filingaccess.serff.com/sfa/search/filingSummary.xhtml?filingId=132395430",
                wait_until="load", timeout=15000
            )
            print(f"  Detail URL: {detail.url}")
            print(f"  Detail title: {await detail.title()}")
            html = await detail.content()
            if "Filing Summary" in html or "SERFF Tracking" in html:
                print("  Detail page loaded!")
            elif "agreement" in detail.url.lower():
                print("  Redirected to agreement!")
            else:
                print(f"  Unknown page, HTML[:200]: {html[:200]}")
            await detail.close()

        await browser.close()

async def main():
    for state in ["NY", "CA", "TX", "OH"]:
        await test_state(state)

asyncio.run(main())
