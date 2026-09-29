class ApiClientError(Exception):
    """Raised when the master rejects a worker API request."""


class CaptchaException(Exception):
    """Raised when Google Maps requires CAPTCHA verification."""


class ScrapeException(Exception):
    """Raised when a scrape cannot be completed and should be retried."""
