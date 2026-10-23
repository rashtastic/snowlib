# ❄️ SnowLib

[![PyPI](https://img.shields.io/pypi/v/snowlib.svg)](https://pypi.org/project/snowlib/)
[![Python Versions](https://img.shields.io/pypi/pyversions/snowlib.svg)](https://pypi.org/project/snowlib/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![DOI (latest)](https://zenodo.org/badge/DOI/10.5281/zenodo.17354788.svg)](https://doi.org/10.5281/zenodo.17354788)

Python-Snowflake utilities

```bash
pip install snowlib
pip install snowlib[snowpark]
pip install snowlib[sqlalchemy]
pip install snowlib[excel]
```

Usage: [developer guide](https://github.com/rashtastic/snowlib/blob/main/docs/developer-guide.md) · Changes: [changelog](https://github.com/rashtastic/snowlib/blob/main/CHANGELOG.md)

## Compatibility

| Package | 3.14 | 3.13-3.10 | 3.9* |
|---------|:----:|:---------:|:----:|
| **snowflake-connector-python** | ≥3.17.0, <5.0.0 | ≥3.17.0, <5.0.0 | ≥3.17.0, <5.0.0 |
| **snowflake-snowpark-python** | ≥1.9.0 | ≥1.9.0 | ≥1.9.0 |
| **sqlalchemy** | ≥2.0, <2.1 | ≥2.0, <2.1 | ≥2.0, <2.1 |
| **snowflake-sqlalchemy** | ≥1.6.0 | ≥1.6.0 | ≥1.6.0 |
| **pandas** | 2.3.3 | 2.3.1 | 2.3.1 |
| **pyarrow** | ≥22.0.0 | ✓ | <22.0.0 |

**Notes:**
- Version ranges shown when specified in `pyproject.toml`
- ✓ = Supported
- \- = Not available (no compatible wheel or version constraint)
- The connector cap applies with the `snowpark` extra, as snowpark itself caps it; a plain install takes the newest connector
- Python 3.9: PyArrow capped at <22.0.0 (22.0.0+ requires ≥3.10)
- SQLAlchemy support: `pip install snowlib[sqlalchemy]`
- \* Python 3.9 reached [end-of-life in October 2025](https://peps.python.org/pep-0596/#lifespan) - please consider upgrading