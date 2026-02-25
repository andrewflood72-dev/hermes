"""Quick script to explore the SERFF SFA portal structure."""
import asyncio
from playwright.async_api import async_playwright


async def explore():
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(
        headless=True,
        args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
    )
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    )
    page = await context.new_page()

    # Step 1: Go to home page first (establishes session)
    print("Step 1: Loading CA home page...")
    await page.goto(
        "https://filingaccess.serff.com/sfa/home/CA",
        timeout=30000,
    )
    await page.wait_for_load_state("networkidle", timeout=15000)
    print(f"  URL: {page.url}")

    # Step 2: Click "Begin Search" link
    print("\nStep 2: Clicking 'Begin Search'...")
    await page.click("a[href='/sfa/userAgreement.xhtml']")
    await page.wait_for_load_state("networkidle", timeout=15000)
    print(f"  URL: {page.url}")

    # Find PrimeFaces buttons
    pf_buttons = await page.locator(".ui-button").all()
    print(f"  PrimeFaces buttons: {len(pf_buttons)}")
    for i, btn in enumerate(pf_buttons):
        text = (await btn.text_content()).strip()
        print(f"    Button {i}: [{text}]")

    # Step 3: Click agree button (first PF button)
    if pf_buttons:
        text = (await pf_buttons[0].text_content()).strip()
        print(f"\nStep 3: Clicking [{text}]...")
        await pf_buttons[0].click()
        await page.wait_for_load_state("networkidle", timeout=20000)
        await asyncio.sleep(2)

        print(f"  URL: {page.url}")
        title = await page.title()
        print(f"  Title: {title}")

        # Find search form elements
        selects = await page.locator("select").all()
        inputs = await page.locator("input[type=text]").all()
        print(f"\n  Select fields: {len(selects)}")
        for s in selects[:15]:
            name = await s.get_attribute("name") or "?"
            sid = await s.get_attribute("id") or "?"
            # Get options
            opts = await s.locator("option").all()
            opt_texts = []
            for o in opts[:5]:
                opt_texts.append((await o.text_content()).strip())
            print(f"    select name={name} id={sid} options={opt_texts}")

        print(f"\n  Text inputs: {len(inputs)}")
        for inp in inputs[:15]:
            name = await inp.get_attribute("name") or "?"
            iid = await inp.get_attribute("id") or "?"
            placeholder = await inp.get_attribute("placeholder") or ""
            print(f"    input name={name} id={iid} placeholder={placeholder}")

        # Find buttons on search page
        all_buttons = await page.locator("button").all()
        print(f"\n  Buttons on search page: {len(all_buttons)}")
        for b in all_buttons[:15]:
            text = (await b.text_content()).strip()
            bid = await b.get_attribute("id") or "?"
            btype = await b.get_attribute("type") or "?"
            bclass = await b.get_attribute("class") or ""
            print(f"    id={bid} type={btype} [{text[:50]}] class={bclass[:80]}")

    await browser.close()
    await pw.stop()


asyncio.run(explore())
