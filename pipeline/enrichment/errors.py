from __future__ import annotations


class ProviderError(RuntimeError):
    def __init__(self, message: str, *, provider: str = "", retryable: bool = False) -> None:
        super().__init__(message)
        self.provider = provider
        self.retryable = retryable


class RetryableProviderError(ProviderError):
    def __init__(self, message: str, *, provider: str = "") -> None:
        super().__init__(message, provider=provider, retryable=True)


class NonRetryableProviderError(ProviderError):
    def __init__(self, message: str, *, provider: str = "") -> None:
        super().__init__(message, provider=provider, retryable=False)


class QuotaExceededError(RetryableProviderError):
    pass


class RateLimitError(RetryableProviderError):
    pass


class NoEmailFoundError(NonRetryableProviderError):
    pass


class MissingProfileDataError(NonRetryableProviderError):
    pass
