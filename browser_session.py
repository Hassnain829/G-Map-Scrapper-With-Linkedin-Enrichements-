from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from playwright.sync_api import (
    BrowserContext,
    Page,
    Playwright,
    sync_playwright,
)


class SharedChromiumSession:
    """
    One persistent Chromium context for the complete pipeline.

    Maps and enrichment use separate tabs inside the same context, so
    cookies, consent state, and CAPTCHA/manual-login state are preserved.
    The profile is also reused across future program runs.
    """

    def __init__(
        self,
        *,
        headless: bool = False,
        profile_dir: Optional[str] = None,
    ) -> None:
        self.headless = headless
        self.profile_dir = Path(
            profile_dir
            or os.getenv(
                "BROWSER_PROFILE_DIR",
                ".chromium_profile",
            )
        ).resolve()

        self.playwright: Optional[Playwright] = None
        self.context: Optional[BrowserContext] = None
        self.primary_page: Optional[Page] = None

    def __enter__(self) -> "SharedChromiumSession":
        self.profile_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.playwright = sync_playwright().start()

        launch_kwargs = {
            "user_data_dir": str(self.profile_dir),
            "headless": self.headless,
            "locale": "en-US",
            "viewport": {
                "width": 1440,
                "height": 1000,
            },
        }

        # Installed Chrome generally behaves closer to the browser the user
        # already uses. Fall back to bundled Chromium if Chrome is unavailable.
        channel = os.getenv(
            "BROWSER_CHANNEL",
            "chrome",
        ).strip()

        try:
            if channel:
                self.context = (
                    self.playwright.chromium
                    .launch_persistent_context(
                        channel=channel,
                        **launch_kwargs,
                    )
                )
            else:
                raise RuntimeError(
                    "No browser channel configured"
                )
        except Exception as channel_error:
            print(
                "Could not launch the installed Chrome channel; "
                "falling back to Playwright Chromium. "
                f"Reason: {channel_error}"
            )
            self.context = (
                self.playwright.chromium
                .launch_persistent_context(
                    **launch_kwargs,
                )
            )

        pages = self.context.pages
        self.primary_page = (
            pages[0]
            if pages
            else self.context.new_page()
        )

        return self

    def new_page(self) -> Page:
        if self.context is None:
            raise RuntimeError(
                "The browser session has not started."
            )

        return self.context.new_page()

    def __exit__(
        self,
        exc_type,
        exc,
        traceback,
    ) -> None:
        try:
            if self.context is not None:
                self.context.close()
        finally:
            if self.playwright is not None:
                self.playwright.stop()
