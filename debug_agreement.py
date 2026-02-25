"""Debug the SERFF SFA agreement page to find the accept button."""
import asyncio
from playwright.async_api import async_playwright


async def debug():
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless=True)
    ctx = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        viewport={"width": 1280, "height": 900},
    )
    page = await ctx.new_page()

    await page.goto(
        "https://filingaccess.serff.com/sfa/home/NY",
        wait_until="networkidle", timeout=30000,
    )
    print(f"Home URL: {page.url}")

    try:
        await page.click("a[href='/sfa/userAgreement.xhtml']", timeout=10000)
        await page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        await page.goto(
            "https://filingaccess.serff.com/sfa/userAgreement.xhtml",
            wait_until="networkidle", timeout=30000,
        )

    print(f"Agreement URL: {page.url}")

    # Dump all clickable elements
    js = """() => {
        var result = [];
        var els = document.querySelectorAll('button, input, a, span[role=button], [class*=button]');
        for (var i = 0; i < els.length; i++) {
            var el = els[i];
            result.push({
                tag: el.tagName,
                type: el.type || '',
                text: (el.textContent || '').trim().substring(0, 60),
                value: el.value || '',
                cls: (el.className || '').substring(0, 80),
                id: el.id || '',
                vis: el.offsetParent !== null
            });
        }
        return result;
    }"""

    elements = await page.evaluate(js)
    print(f"\nFound {len(elements)} elements:")
    for el in elements:
        vis = "VISIBLE" if el["vis"] else "hidden"
        print(
            f"  [{vis}] <{el['tag']}> id='{el['id']}' "
            f"class='{el['cls']}' text='{el['text']}' value='{el['value']}'"
        )

    # Also dump the page title and main content area
    title = await page.title()
    print(f"\nPage title: {title}")

    # Get the main content text (first 500 chars)
    body_text = await page.evaluate("() => document.body.innerText.substring(0, 500)")
    print(f"\nBody text:\n{body_text}")

    await browser.close()
    await pw.stop()


asyncio.run(debug())
