"""Probe the SERFF SFA agreement page - proper flow through home page."""
import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Step 1: Home page
        print("Step 1: Home page...")
        await page.goto("https://filingaccess.serff.com/sfa/home/CA",
                       wait_until="load", timeout=30000)
        await asyncio.sleep(2)
        print(f"  URL: {page.url}")

        # Dump home page links
        print("\n  Home page links:")
        for a in await page.locator("a").all():
            txt = (await a.text_content()).strip()
            href = await a.get_attribute("href") or ""
            if txt:
                print(f"    {txt!r} -> {href!r}")

        # Step 2: Click any link that looks like "begin" or "search" or "filing"
        print("\nStep 2: Looking for 'Begin Search' or similar...")
        clicked = False
        for selector in [
            "a:has-text('Begin')",
            "a:has-text('Search')",
            "a:has-text('Filing')",
            "a:has-text('Access')",
            ".ui-button",
            "button",
        ]:
            try:
                el = page.locator(selector).first
                if await el.is_visible(timeout=2000):
                    txt = await el.text_content()
                    print(f"  Found: {selector} -> {txt!r}")
                    await el.click()
                    await page.wait_for_load_state("load", timeout=15000)
                    await asyncio.sleep(2)
                    clicked = True
                    break
            except Exception:
                continue

        if not clicked:
            print("  No clickable element found, trying all links...")
            for a in await page.locator("a").all():
                href = await a.get_attribute("href") or ""
                if "agreement" in href.lower() or "search" in href.lower():
                    print(f"  Clicking: {href}")
                    await a.click()
                    await page.wait_for_load_state("load", timeout=15000)
                    await asyncio.sleep(2)
                    break

        print(f"\n  Now at: {page.url}")
        print(f"  Title: {await page.title()}")

        # Step 3: Dump current page
        html = await page.content()
        print(f"\n  HTML length: {len(html)}")

        # If this is the agreement page, dump it
        if "agreement" in page.url.lower() or "Agreement" in html:
            print("\n=== AGREEMENT PAGE HTML (first 8000 chars) ===")
            print(html[:8000])
        else:
            print("\n=== CURRENT PAGE HTML (first 5000 chars) ===")
            print(html[:5000])

        # Look for buttons/forms on current page
        print("\n=== BUTTONS ===")
        for btn in await page.locator("button").all():
            txt = await btn.text_content()
            cls = await btn.get_attribute("class") or ""
            visible = await btn.is_visible()
            print(f"  button: text={txt!r} class={cls!r} visible={visible}")

        print("\n=== INPUTS ===")
        for inp in await page.locator("input").all():
            typ = await inp.get_attribute("type") or ""
            val = await inp.get_attribute("value") or ""
            name = await inp.get_attribute("name") or ""
            visible = await inp.is_visible()
            if typ in ("submit", "button", "checkbox"):
                print(f"  input: type={typ!r} value={val!r} name={name!r} visible={visible}")

        print("\n=== LINKS ===")
        for a in await page.locator("a").all():
            txt = (await a.text_content()).strip()
            href = await a.get_attribute("href") or ""
            cls = await a.get_attribute("class") or ""
            if txt:
                print(f"  a: text={txt!r} href={href!r} class={cls!r}")

        await browser.close()

asyncio.run(main())
