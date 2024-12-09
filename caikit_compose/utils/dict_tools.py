"""
Tools for working with dicts
"""


def deep_merge(base: dict, overrides: dict) -> dict:
    """Helper to perform a deep merge of the overrides into the base. The merge
    is done in place, but the resulting dict is also returned for convenience.

    The merge logic is quite simple: If both the base and overrides have a key
    and the type of the key for both is a dict, recursively merge, otherwise
    set the base value to the override value.

    Args:
        base:  dict
            The base config that will be updated with the overrides
        overrides:  dict
            The override config

    Returns:
        merged:  dict
            The merged results of overrides merged onto base
    """
    for key, value in overrides.items():
        if (
            key not in base
            or not isinstance(base[key], dict)
            or not isinstance(value, dict)
        ):
            base[key] = value
        else:
            base[key] = deep_merge(base[key], value)
    return base
