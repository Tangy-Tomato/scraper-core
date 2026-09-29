import time
from pathlib import Path

from playwright.sync_api import sync_playwright

from shared.exceptions import CaptchaException, ScrapeException
from shared.contracts import ScrapeTask


def scrape(task: ScrapeTask, relay_dir: Path) -> str:
    relay_dir.mkdir(parents=True, exist_ok=True)
    import WorkerNodeActual as legacy_scraper

    legacy_scraper.HTML_DROPOFF = relay_dir
    output_path = relay_dir / f"{task.task_id}.html"
    legacy_task = {
        "hash_id": task.task_id,
        "type": task.task_type,
        "target_url": task.target_url,
        "Search_Keyword": task.search_keyword,
        "Search_Location": task.search_location,
        "order_id": task.order_id,
        "query_type": task.query_type,
    }
    config = legacy_scraper.get_browser_config()
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--mute-audio",
                ],
            )
            try:
                context = browser.new_context(
                    viewport=config["viewport"],
                    user_agent=config["user_agent"],
                    locale=config["locale"],
                    timezone_id=config["timezone"],
                )
                try:
                    page = context.new_page()
                    page.route("**/*", legacy_scraper.intercept_heavy_resources)
                    page.add_init_script(
                        "Object.defineProperty(navigator, 'webdriver', "
                        "{get: () => undefined})"
                    )
                    for attempt in range(legacy_scraper.MAX_RETRIES_PER_QUERY):
                        result = legacy_scraper.process_query(page, legacy_task, 1)
                        if result == "SUCCESS" or result == "SKIP":
                            break
                        if result == "CAPTCHA":
                            raise CaptchaException("Google Maps returned a CAPTCHA")
                        if result == "OFFLINE":
                            raise ScrapeException("scraper network is unavailable")
                        if attempt + 1 < legacy_scraper.MAX_RETRIES_PER_QUERY:
                            time.sleep(min(2 ** (attempt + 1), 8))
                    else:
                        raise ScrapeException("legacy scraper exhausted its retries")
                finally:
                    context.close()
            finally:
                browser.close()
        with output_path.open("r", encoding="utf-8") as source:
            content = source.read()
        if len(content) < 1000:
            raise ScrapeException("legacy scraper produced an invalid HTML payload")
        return content
    except (CaptchaException, ScrapeException):
        raise
    except Exception as exc:
        raise ScrapeException(f"scraper failed: {exc}") from exc
    finally:
        output_path.unlink(missing_ok=True)
