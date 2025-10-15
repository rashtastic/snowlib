# ❄️ SnowLib

[![PyPI](https://img.shields.io/pypi/v/snowlib.svg)](https://pypi.org/project/snowlib/)
[![Python Versions](https://img.shields.io/pypi/pyversions/snowlib.svg)](https://pypi.org/project/snowlib/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![DOI (latest)](https://zenodo.org/badge/DOI/10.5281/zenodo.17354788.svg)](https://doi.org/10.5281/zenodo.17354788)
[![DOI (v0.1.1)](https://zenodo.org/badge/DOI/10.5281/zenodo.17354789.svg)](https://doi.org/10.5281/zenodo.17354789)


Python-Snowflake utilities

```bash
pip install snowlib
pip install snowlib[snowpark]
pip install snowlib[excel]
pip install snowlib[snowpark,excel]
```

## Compatibility

| Python | snowflake-connector-python | snowflake-snowpark-python | pandas | pyarrow |
|--------|----------------------------|---------------------------|--------|---------|
| 3.14   | >=3.17.0                   | -                         | ✓      | -       |
| 3.13   | >=3.17.0, <4.0.0           | ✓                         | ✓      | ✓       |
| 3.12   | >=3.17.0, <4.0.0           | ✓                         | ✓      | ✓       |
| 3.11   | >=3.17.0, <4.0.0           | ✓                         | ✓      | ✓       |
| 3.10   | >=3.17.0, <4.0.0           | ✓                         | ✓      | ✓       |
| 3.9    | >=3.17.0, <4.0.0           | ✓                         | ✓      | ✓       |

**Notes:**
- ✓ = Supported with `[snowpark]` extra
- \- = Not supported (no wheel available or incompatible)
- Python 3.14 uses base connector only (Snowpark pending compatibility)
