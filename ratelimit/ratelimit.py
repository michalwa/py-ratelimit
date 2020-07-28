from typing import Callable, Optional, Dict, List, Tuple, Any
from datetime import datetime
import inspect


SECOND = 1
MINUTE = 60 * SECOND
HOUR = 60 * MINUTE
DAY = 24 * HOUR
WEEK = 7 * DAY
MONTH = 30 * DAY
YEAR = 365 * DAY


class RateLimitException(BaseException):
    """
    Raised when a rate-limited callable is called more frequently than its maximum
    allowed rate.
    """


class RateLimitedCallable:
    """
    Restricts calls to a callable to a specific number of times per a specific
    amount of time.
    """

    def __init__(self, f: Callable, max_calls: int, per_seconds: float, session_arg: Optional[str] = None):
        """Constructs a call rate limiter that will disallow calls more frequent
        than the specified rate.

        Parameters
        ----------
          * f: `Callable` - The callable to wrap.
          * max_calls: `int` - The number of calls to allow per `per_seconds` seconds.
          * per_seconds: `float` - The amount of time in seconds during which
            `max_calls` calls should be allowed.
          * session: `str` (optional) - Name of a parameter in the callable's signature
            that will be intercepted and used to identify separate rate limiting sessions.
            This functionality is disabled if `session` is `None`.
        """

        self.f = f
        self.max_calls = max_calls
        self.per_seconds = per_seconds

        self.session_arg = None  # type: Optional[Tuple[int, str]]

        if session_arg is not None:
            signature = inspect.signature(self.f)

            if session_arg not in signature.parameters:
                raise ValueError(
                    f"Parameter '{session_arg}' must be present in "
                    "the signature of the wrapped callable.")

            self.session_arg = list(signature.parameters.keys()).index(session_arg), session_arg

        self._calls = {}  # type: Dict[Any, List[datetime]]

    def __call__(self, *args, **kwargs):

        session = None

        if self.session_arg is not None:
            index, name = self.session_arg
            if name in kwargs:
                session = kwargs[name]
            elif len(args) > index:
                session = args[index]

        self._update_calls(session)

        if len(self._calls[session]) == self.max_calls:
            raise RateLimitException(
                f"Calls to {self.f.__name__} issued too frequently! "
                f"Maximum allowed rate is {self.max_calls}/{self.per_seconds}s.")

        else:
            self._calls[session].append(datetime.now())
            return self.f(*args, **kwargs)

    def _update_calls(self, session):
        """Removes stored call times that are older than `self.per_seconds`"""

        now = datetime.now()

        if session not in self._calls:
            self._calls[session] = []

        self._calls[session] = [t for t in self._calls[session] if (now - t).total_seconds() < self.per_seconds]


def ratelimit(n: int, *, per: float, session: Optional[str] = None):
    """(decorator)
    Limits the number of calls to the decorated callable to the specified number
    per the specified time.

    When the callable is called sooner than allowed by the specified rate,
    `RateLimitException` is raised.

    Parameters
    ----------
      * n: `int` - The number of times the callable will be allowed to be called
        per the specified unit of time.
      * per: `float` - The time during which `n` calls to the callable will be allowed.
      * session: `str` (optional) - Name of a parameter in the callable's signature
        that will be intercepted and used to identify separate rate limiting sessions.
        This functionality is disabled if `session` is `None`.
    """

    def decorator(f):
        return RateLimitedCallable(f, n, per, session)

    return decorator
