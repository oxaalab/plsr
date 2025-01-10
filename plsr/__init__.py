# plsr package initialization

# Re-export plsr_log for back-compat with modules that import it from package root.
from .console import plsr_log, console  # noqa: F401
