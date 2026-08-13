from __future__ import annotations

from datetime import date, datetime

# NULL is written as an empty field. A quoted empty string would be read as 0
# for numeric columns under the reader's default null representation.
NULL_TOKEN = ""


def format_decimal(value: float | None) -> str:
    if value is None:
        return NULL_TOKEN
    # no trailing zeros, matching how the source export renders decimals
    text = f"{value:.2f}"
    if text[-2:] == "00":
        text = text[:-3]
        return "0" if text == "-0" else text
    if text[-1] == "0":
        return text[:-1]
    return text


def format_integer(value: float | int | None) -> str:
    if value is None:
        return NULL_TOKEN
    return str(int(round(float(value))))


def format_bool(value: bool | int | None) -> str:
    if value is None:
        return NULL_TOKEN
    return "1" if value else "0"


def format_string(value: str | None) -> str:
    if value is None or value == "":
        return NULL_TOKEN
    return value


def format_array(items: list[str] | None) -> str:
    if items is None or len(items) == 0:
        return NULL_TOKEN
    return "#~#".join(items)


def format_date(value: date | None) -> str:
    if value is None:
        return NULL_TOKEN
    return value.isoformat()


def format_datetime_ms(value: datetime | None) -> str:
    if value is None:
        return NULL_TOKEN
    base = value.replace(microsecond=0).isoformat(sep=" ")
    return f"{base}.000"
