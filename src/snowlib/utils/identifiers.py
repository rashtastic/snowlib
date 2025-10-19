"""Utilities for validating Snowflake identifiers"""

import re


def is_valid_identifier(name: str) -> bool:
    """Check if a string is a valid Snowflake unquoted identifier"""
    if not name:
        return False
    
    # Pattern: starts with letter or underscore, followed by letters, digits, or underscores
    pattern = r'^[A-Za-z_][A-Za-z0-9_]*$'
    return bool(re.match(pattern, name))
