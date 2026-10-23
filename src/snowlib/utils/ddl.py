"""GET_DDL for one object or many in a single query."""

from typing import NamedTuple, Optional

# GET_DDL's own object type names, which differ from SHOW and DESCRIBE:
# DYNAMIC_TABLE has an underscore, materialized views are VIEW, and there is
# no STAGE.
GET_DDL_TYPES = frozenset(
    {
        "ALERT",
        "DATABASE",
        "DYNAMIC_TABLE",
        "EVENT_TABLE",
        "FILE_FORMAT",
        "FUNCTION",
        "ICEBERG_TABLE",
        "PIPE",
        "POLICY",
        "PROCEDURE",
        "SCHEMA",
        "SEMANTIC VIEW",
        "SEQUENCE",
        "STREAM",
        "TABLE",
        "TAG",
        "TASK",
        "VIEW",
    }
)

# Types whose name in GET_DDL carries argument types: 'NAME(NUMBER, VARCHAR)'
SIGNATURE_TYPES = frozenset({"FUNCTION", "PROCEDURE"})


class DdlTarget(NamedTuple):
    """A schema-level object to fetch DDL for, named exactly as SHOW returns it"""

    object_type: str
    database: str
    schema: str
    name: str
    arg_types: Optional[list[str]] = None


def quote_ident(part: str) -> str:
    """Quote one name part in its exact case: my_task -> "my_task" """
    return '"' + part.replace('"', '""') + '"'


def _split_top_level(text: str) -> list[str]:
    """Split on commas outside parentheses: 'NUMBER(38,0), VARCHAR' -> 2 parts"""
    parts, depth, current = [], 0, ""
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(current)
            current = ""
        else:
            current += ch
    parts.append(current)
    return [p.strip() for p in parts if p.strip()]


def arg_types_from_show(arguments: str) -> list[str]:
    """Argument types from SHOW FUNCTIONS: 'F(ARRAY, [BOOLEAN]) RETURN OBJECT' -> ['ARRAY', 'BOOLEAN']"""
    signature = arguments.split(" RETURN ")[0]
    inner = signature[signature.index("(") + 1 : signature.rindex(")")]
    return [a.strip("[]").strip() for a in _split_top_level(inner)]


def _check_type(object_type: str) -> str:
    """Upper-cased GET_DDL type, or ValueError if GET_DDL does not take it"""
    normalized = object_type.upper()
    if normalized not in GET_DDL_TYPES:
        raise ValueError(
            f"GET_DDL does not support object type {object_type!r}. Supported: "
            + ", ".join(sorted(GET_DDL_TYPES))
        )
    return normalized


def object_label(target: DdlTarget) -> str:
    """Unqualified name, with argument types for functions and procedures"""
    object_type = _check_type(target.object_type)
    if object_type in SIGNATURE_TYPES:
        return f"{target.name}({', '.join(target.arg_types or [])})"
    if target.arg_types:
        raise ValueError(f"{object_type} {target.name} does not take argument types")
    return target.name


def ddl_name(target: DdlTarget) -> str:
    """Name as GET_DDL expects it: quoted parts, plus argument types where needed"""
    object_type = _check_type(target.object_type)
    fqn = ".".join(
        quote_ident(p) for p in (target.database, target.schema, target.name)
    )
    if object_type in SIGNATURE_TYPES:
        return f"{fqn}({', '.join(target.arg_types or [])})"
    return fqn


def ddl_query(targets: list[DdlTarget]) -> tuple[str, list[str]]:
    """One UNION ALL query returning object_type, object_name and ddl per target"""
    if not targets:
        raise ValueError("ddl_query needs at least one target")
    selects, bindings = [], []
    for target in targets:
        selects.append(
            "SELECT %s AS object_type, %s AS object_name, GET_DDL(%s, %s, TRUE) AS ddl"
        )
        object_type = _check_type(target.object_type)
        bindings += [object_type, object_label(target), object_type, ddl_name(target)]
    return "\nUNION ALL\n".join(selects), bindings
