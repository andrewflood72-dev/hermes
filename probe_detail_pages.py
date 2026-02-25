"""Investigate why detail pages return empty metadata."""
import asyncio
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        ctx = await browser.new_context(user_agent=UA, viewport={"width": 1280, "height": 900})
        page = await ctx.new_page()

        # Establish session: home → begin → accept
        print("Establishing session...")
        await page.goto("https://filingaccess.serff.com/sfa/home/NY", wait_until="networkidle", timeout=60000)
        begin = page.locator("a:has-text('Begin')")
        await begin.wait_for(state="visible", timeout=15000)
        await begin.click()
        await asyncio.sleep(5)

        accept = page.locator("button:has-text('Accept')")
        await accept.wait_for(state="visible", timeout=15000)
        await accept.click()
        try:
            await page.wait_for_url("**/filingSearch**", timeout=15000)
        except Exception:
            await asyncio.sleep(3)
        print(f"Session: {page.url}")

        # Test filings that previously failed
        test_ids = [
            ("PERR-134599633", "134599633"),   # empty metadata
            ("DNBU-134360206", "134360206"),    # empty metadata
            ("MRKB-134558020", "134558020"),    # empty metadata
            ("BNIC-134711440", "134711440"),    # unauthorized
            ("AICO-133964667", "133964667"),    # 500
        ]

        for name, fid in test_ids:
            detail = await ctx.new_page()
            url = f"https://filingaccess.serff.com/sfa/search/filingSummary.xhtml?filingId={fid}"
            await detail.goto(url, timeout=15000)
            await asyncio.sleep(2)

            final_url = detail.url
            title = await detail.title()
            html = await detail.content()

            soup = BeautifulSoup(html, "html.parser")
            labels = soup.find_all("label")

            print(f"\n{'='*60}")
            print(f"{name} (id={fid}):")
            print(f"  URL: {final_url}")
            print(f"  Title: {title}")
            print(f"  HTML len: {len(html)}, labels: {len(labels)}")

            if "filingSummary" in final_url:
                if len(labels) > 0:
                    for lbl in labels[:8]:
                        txt = lbl.get_text(strip=True)
                        for_id = lbl.get("for", "")
                        sib = lbl.find_next_sibling()
                        val = sib.get_text(strip=True)[:80] if sib else "?"
                        target = soup.find(id=for_id) if for_id else None
                        tval = target.get_text(strip=True)[:80] if target else ""
                        print(f"  Label: {txt!r} -> for_target={tval!r} sibling={val!r}")
                else:
                    body = soup.get_text()[:800]
                    print(f"  Body text:\n{body}")
            else:
                body = soup.get_text()[:300]
                print(f"  Redirected. Body: {body}")

            await detail.close()
            await asyncio.sleep(1)

        await browser.close()


asyncio.run(main())
