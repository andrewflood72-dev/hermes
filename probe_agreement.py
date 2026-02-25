"""Probe the SERFF SFA agreement page for CA to see what buttons exist."""
import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Go to CA home page
        await page.goto("https://filingaccess.serff.com/sfa/home/CA", wait_until="networkidle", timeout=30000)
        print(f"Home URL: {page.url}")

        # Click Begin Search
        try:
            await page.click("a[href='/sfa/userAgreement.xhtml']", timeout=10000)
            await page.wait_for_load_state("networkidle", timeout=15000)
        except Exception as e:
            print(f"Begin search click failed: {e}")
            await page.goto("https://filingaccess.serff.com/sfa/userAgreement.xhtml", wait_until="networkidle", timeout=15000)

        print(f"Agreement URL: {page.url}")

        # Dump the page HTML (just the body, trimmed)
        html = await page.content()

        # Find all buttons, inputs, and links
        print("\n=== ALL BUTTONS ===")
        for btn in await page.locator("button").all():
            txt = await btn.text_content()
            cls = await btn.get_attribute("class") or ""
            typ = await btn.get_attribute("type") or ""
            print(f"  button: text={txt!r} class={cls!r} type={typ!r}")

        print("\n=== ALL INPUT[type=submit] and INPUT[type=button] ===")
        for inp in await page.locator("input[type='submit'], input[type='button']").all():
            val = await inp.get_attribute("value") or ""
            cls = await inp.get_attribute("class") or ""
            name = await inp.get_attribute("name") or ""
            print(f"  input: value={val!r} class={cls!r} name={name!r}")

        print("\n=== ALL LINKS ===")
        for a in await page.locator("a").all():
            txt = (await a.text_content()).strip()
            href = await a.get_attribute("href") or ""
            cls = await a.get_attribute("class") or ""
            if txt or href:
                print(f"  a: text={txt!r} href={href!r} class={cls!r}")

        print("\n=== ALL .ui-button elements ===")
        for el in await page.locator(".ui-button").all():
            tag = await el.evaluate("e => e.tagName")
            txt = await el.text_content()
            cls = await el.get_attribute("class") or ""
            print(f"  {tag}: text={txt!r} class={cls!r}")

        print("\n=== FORM elements ===")
        for form in await page.locator("form").all():
            fid = await form.get_attribute("id") or ""
            action = await form.get_attribute("action") or ""
            print(f"  form: id={fid!r} action={action!r}")

        # Also try to find anything with "agree" or "accept" text (case insensitive)
        print("\n=== Elements with agree/accept text ===")
        body_text = await page.locator("body").inner_text()
        for line in body_text.split("\n"):
            line = line.strip()
            if line and ("agree" in line.lower() or "accept" in line.lower()):
                print(f"  {line!r}")

        await browser.close()

asyncio.run(main())
