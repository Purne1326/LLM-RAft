import re
from typing import Tuple

class CascadeOutputValidator:
    """
    Validates output from the fast local model to decide whether to accept it
    or escalate to the cloud provider.
    """
    REFUSAL_PATTERNS = [
        re.compile(r"as an ai,? i (?:cannot|can't|am unable to)", re.IGNORECASE),
        re.compile(r"i don't have (?:the capability|access|information)", re.IGNORECASE),
        re.compile(r"i am sorry,? but i cannot", re.IGNORECASE)
    ]

    @classmethod
    def is_acceptable(cls, prompt: str, output: str) -> Tuple[bool, str]:
        stripped = output.strip()
        if not stripped:
            return False, "empty output"

        if len(stripped) < 10 and len(prompt) > 20:
            return False, "suspiciously short answer"

        # Check for refusal
        for pat in cls.REFUSAL_PATTERNS:
            if pat.search(stripped):
                return False, "model refused or expressed inability"

        # Check for repetitive loops (common failure mode of small quantised models)
        words = stripped.split()
        if len(words) > 20:
            unique_ratio = len(set(words)) / len(words)
            if unique_ratio < 0.25:
                return False, f"degenerative repetition detected (unique ratio {unique_ratio:.2f})"

        return True, "passed quality checks"

