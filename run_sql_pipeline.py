#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_sql_pipeline.py — последовательный запуск *.sql из каталога sql_pipeline.

Что делает скрипт
-----------------
1. Берёт файлы *.sql из каталога (по умолчанию — каталог, где лежит этот скрипт)
   в естественном порядке: 1.sql, 2.sql, ..., 10.sql (а не 1, 10, 2).
2. Подключается к Greenplum/PostgreSQL по параметрам GP_* из .env.secret.
3. Подставляет значения параметров вида :name из param.txt (или из --param).
4. Выполняет операторы каждого файла по порядку.
5. После каждого файла дописывает в resume_exec.txt строку статистики:
   номер/имя файла, количество обработанных строк, время, наличие ошибок.
6. При первой ошибке останавливает весь процесс, пишет ошибку в лог и
   возвращает ненулевой код.
7. После успешного выполнения всех файлов (или при аварийной остановке)
   дописывает итоговую строку с суммарным временем работы.

Формат .env.secret
------------------
Поддерживаются оба варианта:

    GP_HOST=gp.example.local
    GP_PORT=5432
    GP_DB=dwh
    GP_USER=user
    GP_PASSWORD=secret

или секция:

    [GP_HOST]
    HOST=gp.example.local
    PORT=5432
    DB=dwh
    USER=user
    PASSWORD=secret

Пароль никогда не пишется в лог и не выводится на экран.

Дополнительно можно задать GP_GSSENCMODE (по умолчанию disable — иначе
некоторые сборки Greenplum отвечают ошибкой GSSAPI negotiation) и GP_SSLMODE.

Формат param.txt
----------------
    :mm_id = 1
    :prd = '2025-08-01'

Значение без кавычек, похожее на число, подставляется как число; SQL-выражения
(например, to_date(...), date '2025-09-01', '2025-09-01'::date) — как есть;
остальные значения — как строковый литерал (одиночные кавычки экранируются).
Значение в одиночных кавычках подставляется как есть.

Параметры в SQL
---------------
Поддерживаются два вида параметров:

1. :name — подставляется как SQL-литерал:
       :mm_id       -> 1
       :prd           -> to_date('20251101', 'YYYYMMDD')

2. ~p_name~ (и ~name~) — «сырая» подстановка без кавычек и без преобразования
   в число. Нужна для имён таблиц/партиций, форматов дат и списков:
       schm.tab_name_p~p_mm_id~
           -> schm.tab_name_p1
       to_date('~p_strt_dt~', '~p_dt_fmt~')
           -> to_date('20260101', 'YYYYMMDD')
       in (~p_sd_lt~)
           -> in (9, 8, 2)

   Значение для ~p_mm_id~ берётся из параметра :mm_id (префикс p_
   отбрасывается). Ведущие нули сохраняются: :mm_id = 000001 даст 000001.
   Подстановка ~...~ выполняется и внутри строковых литералов, но не в
   комментариях.

Примеры запуска
---------------
    python3 run_sql_pipeline.py
    python3 run_sql_pipeline.py --dry-run
    python3 run_sql_pipeline.py --param prd='2025-08-01'
    python3 run_sql_pipeline.py --check-connection
    python3 run_sql_pipeline.py --truncate-log -v

Коды возврата
-------------
    0 — все файлы выполнены успешно;
    1 — ошибка подключения/выполнения SQL (процесс остановлен);
    2 — ошибка конфигурации (нет файлов, не найден .env.secret, param.txt,
        не заданы обязательные параметры и т.п.).
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

try:
    import psycopg2
except ImportError:  # pragma: no cover - защита от запуска без драйвера
    sys.stderr.write(
        "Ошибка: не установлен psycopg2.\n"
        "Установите: python3 -m pip install psycopg2-binary\n"
    )
    sys.exit(2)


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_ENV_FILE = PROJECT_ROOT / ".env.secret"
DEFAULT_PARAMS_FILE = SCRIPT_DIR / "param.txt"
DEFAULT_LOG_FILE = SCRIPT_DIR / "resume_exec.txt"
ENV_SECTION = "GP_HOST"

CONNECTION_KEYS = ("GP_HOST", "GP_PORT", "GP_DB", "GP_USER", "GP_PASSWORD")

_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_TILDE_PARAM_RE = re.compile(r"~([A-Za-z_][A-Za-z0-9_]*)~")
_DOLLAR_TAG_RE = re.compile(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$")
_INT_RE = re.compile(r"^[+-]?\d+$")
_FLOAT_RE = re.compile(
    r"^[+-]?(?:\d+\.\d*|\.\d+|\d+[eE][+-]?\d+|\d+\.\d*[eE][+-]?\d+)$"
)
_PARAM_LINE_RE = re.compile(
    r"^\s*:?([A-Za-z_][A-Za-z0-9_]*)\s*(?:=|:)\s*(.*?)\s*$"
)
_PARAM_LINE_SPACE_RE = re.compile(
    r"^\s*:?([A-Za-z_][A-Za-z0-9_]*)\s+(.+?)\s*$"
)
_SQL_FUNCTION_RE = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*\s*\(.*\)$", re.DOTALL
)
_SQL_TYPED_LITERAL_RE = re.compile(
    r"^(?:date|timestamp|time|interval)\s+'.*'$", re.IGNORECASE | re.DOTALL
)
_SQL_SPECIAL_RE = re.compile(
    r"^(?:current_date|current_time|current_timestamp|localtime|"
    r"localtimestamp|now\s*\(\s*\))$",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Разбор .env.secret и param.txt
# ---------------------------------------------------------------------------
def parse_env_file(path: Path) -> Dict[str, str]:
    """Читает KEY=VALUE-файл, поддерживая секции [SECTION]."""
    if not path.is_file():
        raise FileNotFoundError(f"Файл секретов не найден: {path}")

    values: Dict[str, str] = {}
    section: Optional[str] = None

    try:
        text = path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError:
        text = path.read_text(encoding="cp1251")

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1].strip()
            continue
        if line.lower().startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        if section:
            values[f"{section}.{key}"] = value
        # Плоские ключи тоже сохраняем: так работает текущий .env.secret.
        values[key] = value

    return values


def env_get(env: Dict[str, str], key: str, section: str = ENV_SECTION) -> Optional[str]:
    """Ищет ключ сначала в секции, затем как плоский ключ."""
    candidates: List[str] = []
    if section:
        candidates.append(f"{section}.{key}")
        if key.startswith("GP_"):
            # [GP_HOST] HOST=... / [GP_HOST] PORT=...
            candidates.append(f"{section}.{key[3:]}")
    candidates.append(key)

    for candidate in candidates:
        value = env.get(candidate)
        if value is not None and value != "":
            return value
    return None


def parse_params_file(path: Path) -> Dict[str, str]:
    """Читает param.txt: строки вида ':name = value' или ':name value'."""
    if not path.is_file():
        raise FileNotFoundError(f"Файл параметров не найден: {path}")

    params: Dict[str, str] = {}
    for lineno, raw_line in enumerate(
        path.read_text(encoding="utf-8-sig").splitlines(), 1
    ):
        line = raw_line.strip()
        if not line or line.startswith(("#", "--")):
            continue

        match = _PARAM_LINE_RE.match(line) or _PARAM_LINE_SPACE_RE.match(line)
        if not match:
            raise ValueError(
                f"{path}:{lineno}: не удалось разобрать строку параметра: {line!r}"
            )

        name = match.group(1)
        value = match.group(2).strip()
        if not value:
            raise ValueError(f"{path}:{lineno}: пустое значение параметра :{name}")
        params[name] = value

    return params


def parse_cli_param(text: str) -> Tuple[str, str]:
    """Разбирает --param name=value (допускается ведущее двоеточие)."""
    if "=" not in text:
        raise argparse.ArgumentTypeError(
            f"ожидается NAME=VALUE, получено: {text!r}"
        )
    name, value = text.split("=", 1)
    name = name.strip().lstrip(":")
    value = value.strip()
    if not _NAME_RE.fullmatch(name):
        raise argparse.ArgumentTypeError(f"некорректное имя параметра: {name!r}")
    if not value:
        raise argparse.ArgumentTypeError(f"пустое значение параметра :{name}")
    return name, value


def quote_sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def to_sql_literal(value: str) -> str:
    """Преобразует значение параметра в безопасный SQL-литерал."""
    value = value.strip()
    if not value:
        raise ValueError("пустое значение параметра")

    if len(value) >= 2 and value[0] == value[-1] == "'":
        # Пользователь уже задал строковый литерал.
        return value
    if len(value) >= 2 and value[0] == value[-1] == '"':
        return quote_sql_string(value[1:-1])

    if _INT_RE.match(value) or _FLOAT_RE.match(value):
        return value
    if value.lower() in ("true", "false", "null"):
        return value.lower()

    # SQL-выражения и явные приведения типов оставляем как есть:
    #   to_date('20250901', 'YYYYMMDD')
    #   date '2025-09-01'
    #   '2025-09-01'::date
    if (
        _SQL_FUNCTION_RE.match(value)
        or _SQL_TYPED_LITERAL_RE.match(value)
        or _SQL_SPECIAL_RE.match(value)
        or "::" in value
    ):
        return value

    return quote_sql_string(value)


# ---------------------------------------------------------------------------
# Лексический разбор SQL: параметры, комментарии, кавычки, разделение на операторы
# ---------------------------------------------------------------------------
def _skip_single_quote(sql: str, start: int) -> int:
    """Возвращает индекс сразу после закрывающей кавычки."""
    i = start + 1
    n = len(sql)
    is_escape_string = (
        start > 0
        and sql[start - 1] in ("e", "E")
        and (start < 2 or not (sql[start - 2].isalnum() or sql[start - 2] == "_"))
    )
    while i < n:
        ch = sql[i]
        if ch == "\\" and is_escape_string:
            i += 2
            continue
        if ch == "'":
            if i + 1 < n and sql[i + 1] == "'":
                i += 2
                continue
            return i + 1
        i += 1
    return n


def _skip_double_quote(sql: str, start: int) -> int:
    i = start + 1
    n = len(sql)
    while i < n:
        if sql[i] == '"':
            if i + 1 < n and sql[i + 1] == '"':
                i += 2
                continue
            return i + 1
        i += 1
    return n


def _skip_block_comment(sql: str, start: int) -> int:
    """Пропускает /* ... */ с поддержкой вложенности (как в PostgreSQL)."""
    i = start + 2
    depth = 1
    n = len(sql)
    while i < n and depth > 0:
        if sql.startswith("/*", i):
            depth += 1
            i += 2
        elif sql.startswith("*/", i):
            depth -= 1
            i += 2
        else:
            i += 1
    return i


def raw_param_value(value: str) -> str:
    """
    Значение для подстановки ~p_name~: без внешних кавычек, как есть.

    В отличие от :name, здесь сохраняются ведущие нули и не добавляются
    SQL-кавычки: именно это нужно для имён таблиц/партиций.
    """
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        quote = value[0]
        inner = value[1:-1]
        # '000009' -> 000009, но '9','8' оставляем как есть.
        if quote not in inner.replace(quote + quote, ""):
            return inner.replace(quote + quote, quote)
    return value


def resolve_tilde_param(token: str, params: Dict[str, str]) -> Optional[str]:
    """
    ~p_mm_id~ -> параметр :mm_id; ~mm_id~ -> :mm_id.

    Если параметра mm_id нет, но есть p_mm_id, используется он.
    """
    if token.startswith("p_"):
        name = token[2:]
        if name in params:
            return name
    if token in params:
        return token
    return None


def _missing_tilde_label(token: str) -> str:
    if token.startswith("p_"):
        return f"~{token}~ -> :{token[2:]}"
    return f"~{token}~"


def substitute_tildes(
    sql: str, params: Optional[Dict[str, str]]
) -> Tuple[str, Set[str], Set[str]]:
    """
    Подставляет ~p_name~ / ~name~ сырыми значениями из params.

    Подстановка выполняется в обычном SQL и внутри строковых литералов
    (например, to_date('~p_st_dtb~', '~p_dt_frm~')), но не в
    комментариях. Значения не заключаются в кавычки и не преобразуются
    в числа, поэтому ведущие нули сохраняются.
    """
    out: List[str] = []
    used: Set[str] = set()
    missing: Set[str] = set()

    def replace_match(match: "re.Match[str]") -> str:
        token = match.group(1)
        used.add(f"~{token}~")
        if params is None:
            return match.group(0)

        resolved = resolve_tilde_param(token, params)
        if resolved is not None:
            return raw_param_value(params[resolved])

        missing.add(_missing_tilde_label(token))
        return match.group(0)

    i = 0
    n = len(sql)
    quote: Optional[str] = None
    escape_string = False

    while i < n:
        ch = sql[i]

        if quote is None:
            if ch == "-" and i + 1 < n and sql[i + 1] == "-":
                end = sql.find("\n", i)
                if end < 0:
                    end = n
                out.append(sql[i:end])
                i = end
                continue
            if ch == "/" and i + 1 < n and sql[i + 1] == "*":
                end = _skip_block_comment(sql, i)
                out.append(sql[i:end])
                i = end
                continue
            if ch == "$":
                dollar = _DOLLAR_TAG_RE.match(sql, i)
                if dollar:
                    tag = dollar.group(0)
                    end = sql.find(tag, dollar.end())
                    end = n if end < 0 else end + len(tag)
                    out.append(sql[i:end])
                    i = end
                    continue
            if ch == "'":
                quote = "'"
                escape_string = (
                    i > 0
                    and sql[i - 1] in ("e", "E")
                    and (
                        i < 2
                        or not (sql[i - 2].isalnum() or sql[i - 2] == "_")
                    )
                )
                out.append(ch)
                i += 1
                continue
            if ch == '"':
                quote = '"'
                out.append(ch)
                i += 1
                continue
        else:
            if ch == "\\" and quote == "'" and escape_string:
                out.append(sql[i:i + 2])
                i += 2
                continue
            if ch == quote:
                if i + 1 < n and sql[i + 1] == quote:
                    out.append(sql[i:i + 2])
                    i += 2
                    continue
                out.append(ch)
                i += 1
                quote = None
                escape_string = False
                continue

        if ch == "~":
            match = _TILDE_PARAM_RE.match(sql, i)
            if match:
                out.append(replace_match(match))
                i = match.end()
                continue

        out.append(ch)
        i += 1

    return "".join(out), used, missing


def _strip_leading_comments(sql: str) -> str:
    i = 0
    n = len(sql)
    while i < n:
        if sql[i].isspace():
            i += 1
        elif sql.startswith("--", i):
            newline = sql.find("\n", i)
            i = n if newline < 0 else newline + 1
        elif sql.startswith("/*", i):
            i = _skip_block_comment(sql, i)
        else:
            break
    return sql[i:]


def strip_sql_comments(sql: str) -> str:
    """Удаляет комментарии вне строковых литералов (для проверки пустоты)."""
    out: List[str] = []
    i = 0
    n = len(sql)
    while i < n:
        if sql.startswith("--", i):
            newline = sql.find("\n", i)
            i = n if newline < 0 else newline + 1
        elif sql.startswith("/*", i):
            i = _skip_block_comment(sql, i)
        elif sql[i] == "'":
            end = _skip_single_quote(sql, i)
            out.append(sql[i:end])
            i = end
        elif sql[i] == '"':
            end = _skip_double_quote(sql, i)
            out.append(sql[i:end])
            i = end
        else:
            out.append(sql[i])
            i += 1
    return "".join(out)


def split_and_substitute(
    sql: str, params: Optional[Dict[str, str]] = None
) -> Tuple[List[str], Set[str], Set[str]]:
    """
    Разбирает SQL на операторы и подставляет :name и ~p_name~.

    Возвращает (операторы, использованные параметры, отсутствующие параметры).
    Параметры в результате имеют вид ':name' или '~p_name~'.
    Комментарии и '::type' не затрагиваются; semicolon внутри
    кавычек/комментариев не разделяет операторы.
    """
    sql, tilde_used, tilde_missing = substitute_tildes(sql, params)
    statements: List[str] = []
    buffer: List[str] = []
    used: Set[str] = set(tilde_used)
    missing: Set[str] = set(tilde_missing)

    def flush() -> None:
        statement = "".join(buffer).strip()
        buffer.clear()
        if statement and strip_sql_comments(statement).strip():
            statements.append(statement)

    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]

        if ch == "'":
            end = _skip_single_quote(sql, i)
            buffer.append(sql[i:end])
            i = end
        elif ch == '"':
            end = _skip_double_quote(sql, i)
            buffer.append(sql[i:end])
            i = end
        elif ch == "-" and i + 1 < n and sql[i + 1] == "-":
            end = sql.find("\n", i)
            if end < 0:
                end = n
            buffer.append(sql[i:end])
            i = end
        elif ch == "/" and i + 1 < n and sql[i + 1] == "*":
            end = _skip_block_comment(sql, i)
            buffer.append(sql[i:end])
            i = end
        elif ch == "$":
            match = _DOLLAR_TAG_RE.match(sql, i)
            if match:
                tag = match.group(0)
                end = sql.find(tag, match.end())
                end = n if end < 0 else end + len(tag)
                buffer.append(sql[i:end])
                i = end
            else:
                buffer.append(ch)
                i += 1
        elif ch == ":":
            # '::type' — приведение типа, а не параметр.
            if i > 0 and sql[i - 1] == ":":
                buffer.append(ch)
                i += 1
                continue

            match = _NAME_RE.match(sql, i + 1)
            if not match:
                buffer.append(ch)
                i += 1
                continue

            name = match.group(0)
            used.add(":" + name)
            if params is None:
                buffer.append(sql[i:match.end()])
            elif name in params:
                buffer.append(to_sql_literal(params[name]))
            else:
                missing.add(":" + name)
                buffer.append(sql[i:match.end()])
            i = match.end()
        elif ch == ";":
            flush()
            i += 1
        else:
            buffer.append(ch)
            i += 1

    flush()
    return statements, used, missing


def leading_keyword(statement: str) -> str:
    body = _strip_leading_comments(statement)
    match = re.match(r"[A-Za-z]+", body)
    return match.group(0).lower() if match else ""


def is_commit(statement: str) -> bool:
    return leading_keyword(statement) in ("commit", "end")


def is_rollback(statement: str) -> bool:
    return leading_keyword(statement) in ("rollback", "abort")


# ---------------------------------------------------------------------------
# Логирование и форматирование
# ---------------------------------------------------------------------------
def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def format_duration(seconds: float) -> str:
    seconds = max(0.0, seconds)
    if seconds < 60:
        return f"{seconds:.3f}s"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {sec:06.3f}s"
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes):02d}m {sec:06.3f}s"


def display_param_value(value: str) -> str:
    """Значение параметра в одну строку для лога."""
    return value.replace("\r", " ").replace("\n", " ")


class RunLog:
    """Пишет строки одновременно в консоль и в resume_exec.txt."""

    def __init__(self, path: Path, truncate: bool = False) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = path.open(
            "w" if truncate else "a", encoding="utf-8", newline="\n"
        )

    def line(self, text: str = "") -> None:
        decorated = f"[{now_text()}] {text}" if text else ""
        print(decorated, flush=True)
        self._fh.write(decorated + "\n")
        self._fh.flush()

    def raw(self, text: str = "") -> None:
        """Строка без временной метки (разделители, заголовки)."""
        print(text, flush=True)
        self._fh.write(text + "\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


class ConfigError(RuntimeError):
    """Ошибка конфигурации: нет файлов, ключей, параметров."""


class ScriptExecutionError(RuntimeError):
    def __init__(
        self,
        statement_no: int,
        statement_total: int,
        rows: int,
        original: BaseException,
    ):
        super().__init__(str(original))
        self.statement_no = statement_no
        self.statement_total = statement_total
        self.rows = rows
        self.original = original


def redact(text: str, secret: Optional[str]) -> str:
    text = " ".join(str(text).split())
    if secret:
        text = text.replace(secret, "***")
    return text


def format_db_error(exc: BaseException, password: Optional[str]) -> str:
    parts: List[str] = []
    message = str(exc).strip()

    if isinstance(exc, psycopg2.Error):
        diag = getattr(exc, "diag", None)
        if diag is not None:
            if diag.sqlstate:
                parts.append(f"sqlstate={diag.sqlstate}")
            if diag.message_primary:
                message = diag.message_primary
            if diag.message_detail:
                message += f" | detail: {diag.message_detail}"
            if diag.message_hint:
                message += f" | hint: {diag.message_hint}"

    if parts:
        return redact(f"{' '.join(parts)}: {message}", password)
    return redact(message, password)


# ---------------------------------------------------------------------------
# Порядок файлов и подготовка
# ---------------------------------------------------------------------------
def natural_sql_key(path: Path) -> Tuple[int, int, str]:
    prefix = re.match(r"^(\d+)", path.name)
    if prefix:
        return 0, int(prefix.group(1)), path.name.lower()
    return 1, 0, path.name.lower()


@dataclass
class ScriptPlan:
    index: int
    total: int
    path: Path
    statements: List[str]
    used_params: Set[str]
    missing_params: Set[str]


def build_plans(
    files: Sequence[Path], params: Dict[str, str]
) -> List[ScriptPlan]:
    plans: List[ScriptPlan] = []
    total = len(files)

    for index, path in enumerate(files, 1):
        try:
            sql_text = path.read_text(encoding="utf-8-sig")
        except UnicodeDecodeError:
            sql_text = path.read_text(encoding="cp1251")

        statements, used, missing = split_and_substitute(sql_text, params=params)
        plans.append(
            ScriptPlan(
                index=index,
                total=total,
                path=path,
                statements=statements,
                used_params=used,
                missing_params=missing,
            )
        )

    return plans


def print_dry_run(
    sql_dir: Path,
    files: Sequence[Path],
    plans: Sequence[ScriptPlan],
    params: Dict[str, str],
) -> int:
    params_text = ", ".join(
        f":{name}={display_param_value(value)}"
        for name, value in sorted(params.items())
    ) or "(нет)"
    print(f"SQL dir:  {sql_dir}")
    print(f"Files:    {len(files)}")
    print(f"Params:   {params_text}")
    print("")

    has_missing = False
    for plan in plans:
        status = "OK"
        if plan.missing_params:
            status = "MISSING: " + ", ".join(sorted(plan.missing_params))
            has_missing = True
        used = ", ".join(sorted(plan.used_params)) or "-"
        print(
            f"  {plan.index:>2}/{plan.total}  {plan.path.name:<20} "
            f"statements={len(plan.statements):<4} params={used:<45} {status}"
        )

    if has_missing:
        print("")
        print("Не все параметры заданы. Добавьте их в param.txt или передайте --param.")
        return 2

    print("")
    print("Dry run: SQL не выполнялся, подключение к БД не создавалось.")
    return 0


# ---------------------------------------------------------------------------
# Подключение к БД
# ---------------------------------------------------------------------------
def load_connection_settings(
    env_path: Path, connect_timeout: int
) -> Tuple[Dict[str, object], Optional[str]]:
    env = parse_env_file(env_path)

    missing = [
        key for key in CONNECTION_KEYS if key != "GP_PORT" and not env_get(env, key)
    ]
    if missing:
        raise ConfigError(
            "в .env.secret не заданы обязательные ключи: " + ", ".join(missing)
        )

    host = env_get(env, "GP_HOST")
    port_text = env_get(env, "GP_PORT") or "5432"
    dbname = env_get(env, "GP_DB")
    user = env_get(env, "GP_USER")
    password = env_get(env, "GP_PASSWORD")

    try:
        port = int(port_text)
    except ValueError as exc:
        raise ConfigError(f"некорректный GP_PORT: {port_text!r}") from exc

    settings: Dict[str, object] = {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user,
        "password": password,
        "connect_timeout": connect_timeout,
        "application_name": "run_sql_pipeline",
        # На этом Greenplum стандартное согласование GSSAPI ломает подключение
        # ("received invalid response to GSSAPI negotiation"), поэтому по
        # умолчанию отключаем его. При необходимости задайте GP_GSSENCMODE.
        "gssencmode": env_get(env, "GP_GSSENCMODE") or "disable",
    }
    sslmode = env_get(env, "GP_SSLMODE")
    if sslmode:
        settings["sslmode"] = sslmode
    return settings, password


def connect_db(settings: Dict[str, object]):
    conn = psycopg2.connect(**settings)
    conn.autocommit = False
    try:
        conn.set_client_encoding("UTF8")
    except psycopg2.Error:
        # Не критично: сервер может использовать другую кодировку по умолчанию.
        conn.rollback()
    return conn


def describe_target(settings: Dict[str, object]) -> str:
    return (
        f"host={settings.get('host')} port={settings.get('port')} "
        f"db={settings.get('dbname')} user={settings.get('user')}"
    )


def check_connection(env_path: Path, connect_timeout: int) -> int:
    password: Optional[str] = None
    try:
        settings, password = load_connection_settings(env_path, connect_timeout)
        print(f"Подключение: {describe_target(settings)}")
        conn = connect_db(settings)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "select version(), current_database(), current_user, "
                    "current_setting('server_version_num')"
                )
                row = cur.fetchone()
            conn.rollback()
        finally:
            conn.close()
        print("Подключение успешно.")
        if row:
            print(f"version: {row[0]}")
            print(f"database: {row[1]} | user: {row[2]} | server_version_num: {row[3]}")
        return 0
    except Exception as exc:  # noqa: BLE001 - CLI должен показать любую ошибку
        print(f"Ошибка подключения: {format_db_error(exc, password)}")
        return 1


# ---------------------------------------------------------------------------
# Выполнение
# ---------------------------------------------------------------------------
def execute_script(conn, plan: ScriptPlan, verbose: bool = False) -> int:
    """Выполняет один SQL-файл. Возвращает суммарный rowcount."""
    rows = 0
    total_statements = len(plan.statements)

    with conn.cursor() as cur:
        for statement_no, statement in enumerate(plan.statements, 1):
            try:
                if is_commit(statement):
                    conn.commit()
                    if verbose:
                        print(
                            f"    [{statement_no}/{total_statements}] COMMIT",
                            flush=True,
                        )
                    continue
                if is_rollback(statement):
                    conn.rollback()
                    if verbose:
                        print(
                            f"    [{statement_no}/{total_statements}] ROLLBACK",
                            flush=True,
                        )
                    continue

                cur.execute(statement)
                rowcount = cur.rowcount
                if rowcount is not None and rowcount > 0:
                    rows += rowcount
                if verbose:
                    shown = rowcount if rowcount is not None and rowcount >= 0 else 0
                    print(
                        f"    [{statement_no}/{total_statements}] "
                        f"rows={shown}",
                        flush=True,
                    )
            except Exception as exc:
                # Откатываем только текущую незакоммиченную часть.
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise ScriptExecutionError(
                    statement_no, total_statements, rows, exc
                ) from exc

    conn.commit()
    return rows


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Последовательно выполняет *.sql из sql_pipeline, подставляет "
            "параметры :name из param.txt и пишет статистику в resume_exec.txt."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Примеры:\n"
            "  python3 run_sql_pipeline.py\n"
            "  python3 run_sql_pipeline.py --dry-run\n"
            "  python3 run_sql_pipeline.py --param prd='2025-08-01'\n"
            "  python3 run_sql_pipeline.py --check-connection\n"
            "  python3 run_sql_pipeline.py --truncate-log -v\n"
        ),
    )
    parser.add_argument(
        "--sql-dir",
        type=Path,
        default=SCRIPT_DIR,
        help=f"каталог с *.sql (по умолчанию: {SCRIPT_DIR})",
    )
    parser.add_argument(
        "--pattern",
        default="*.sql",
        help="маска файлов (по умолчанию: *.sql)",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_FILE,
        help=f"файл с GP_* (по умолчанию: {DEFAULT_ENV_FILE})",
    )
    parser.add_argument(
        "--params-file",
        type=Path,
        default=None,
        help="файл параметров (по умолчанию: <sql-dir>/param.txt)",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="файл статистики (по умолчанию: <sql-dir>/resume_exec.txt)",
    )
    parser.add_argument(
        "--param",
        action="append",
        default=[],
        type=parse_cli_param,
        metavar="NAME=VALUE",
        help="параметр :name (можно повторять; перекрывает param.txt)",
    )
    parser.add_argument(
        "--connect-timeout",
        type=int,
        default=15,
        metavar="SEC",
        help="таймаут подключения в секундах (по умолчанию: 15)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="только показать план и проверить параметры, без подключения к БД",
    )
    parser.add_argument(
        "--check-connection",
        action="store_true",
        help="проверить подключение к БД и выйти",
    )
    parser.add_argument(
        "--truncate-log",
        action="store_true",
        help="очистить resume_exec.txt перед запуском (по умолчанию дописываем)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="печатать каждый выполненный оператор",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    sql_dir = args.sql_dir.expanduser().resolve()
    env_path = args.env_file.expanduser().resolve()
    params_path = (
        args.params_file.expanduser().resolve()
        if args.params_file
        else sql_dir / "param.txt"
    )
    log_path = (
        args.log_file.expanduser().resolve()
        if args.log_file
        else sql_dir / "resume_exec.txt"
    )

    if args.check_connection:
        return check_connection(env_path, args.connect_timeout)

    if not sql_dir.is_dir():
        print(f"Ошибка: каталог не найден: {sql_dir}", file=sys.stderr)
        return 2

    files = sorted(sql_dir.glob(args.pattern), key=natural_sql_key)
    if not files:
        print(
            f"Ошибка: в {sql_dir} не найдено файлов по маске {args.pattern!r}",
            file=sys.stderr,
        )
        return 2

    if args.dry_run:
        try:
            params = parse_params_file(params_path)
            params.update(dict(args.param))
        except Exception as exc:
            print(f"Ошибка параметров: {exc}", file=sys.stderr)
            return 2

        plans = build_plans(files, params)
        return print_dry_run(sql_dir, files, plans, params)

    log = RunLog(log_path, truncate=args.truncate_log)
    started = time.perf_counter()
    total_rows = 0
    completed = 0
    status = "ABORTED"
    exit_code = 0
    conn = None
    password: Optional[str] = None

    try:
        log.raw("=" * 88)
        log.line(f"RUN START | files={len(files)} | dir={sql_dir}")

        # 1. Параметры и предварительная проверка всех файлов.
        try:
            params = parse_params_file(params_path)
            params.update(dict(args.param))
        except Exception as exc:  # noqa: BLE001
            raise ConfigError(f"ошибка чтения параметров: {exc}") from exc

        plans = build_plans(files, params)
        missing_lines = [
            f"{plan.path.name}: "
            + ", ".join(sorted(plan.missing_params))
            for plan in plans
            if plan.missing_params
        ]
        if missing_lines:
            raise ConfigError(
                "не заданы параметры SQL:\n    "
                + "\n    ".join(missing_lines)
                + f"\n  Добавьте их в {params_path} или передайте --param NAME=VALUE."
            )

        log.line(f"PARAMS | file={params_path}")
        for name in sorted(params):
            log.line(f"  :{name} = {display_param_value(params[name])}")

        # 2. Подключение.
        settings, password = load_connection_settings(
            env_path, args.connect_timeout
        )
        log.line(f"TARGET | {describe_target(settings)}")
        log.raw("-" * 88)

        try:
            conn = connect_db(settings)
        except Exception as exc:  # noqa: BLE001
            raise ConfigError(
                f"не удалось подключиться к БД: {format_db_error(exc, password)}"
            ) from exc

        # 3. Последовательное выполнение файлов.
        for plan in plans:
            log.line(
                f"[{plan.index}/{plan.total}] {plan.path.name} | RUNNING | "
                f"statements={len(plan.statements)}"
            )
            script_started = time.perf_counter()
            try:
                rows = execute_script(conn, plan, verbose=args.verbose)
            except ScriptExecutionError as exc:
                elapsed = time.perf_counter() - script_started
                error_text = format_db_error(exc.original, password)
                log.line(
                    f"[{plan.index}/{plan.total}] {plan.path.name} | ERROR | "
                    f"rows={exc.rows} | total_rows={total_rows} | "
                    f"time={format_duration(elapsed)} | errors=yes"
                )
                log.line(
                    f"  ERROR: statement {exc.statement_no}/{exc.statement_total}: "
                    f"{error_text}"
                )
                status = "ABORTED"
                exit_code = 1
                break

            elapsed = time.perf_counter() - script_started
            total_rows += rows
            completed += 1
            log.line(
                f"[{plan.index}/{plan.total}] {plan.path.name} | OK | "
                f"rows={rows} | total_rows={total_rows} | "
                f"time={format_duration(elapsed)} | errors=no"
            )
        else:
            # for-else: цикл дошёл до конца без break, значит все файлы успешны.
            status = "SUCCESS"

    except ConfigError as exc:
        log.line(f"ERROR | {exc}")
        exit_code = 2
    except KeyboardInterrupt:
        log.line("ERROR | выполнение прервано пользователем (Ctrl+C)")
        exit_code = 130
    except BaseException as exc:
        log.line(f"ERROR | непредвиденная ошибка: {format_db_error(exc, password)}")
        exit_code = 1
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

        log.raw("-" * 88)
        log.line(
            f"TOTAL | status={status} | scripts={completed}/{len(files)} | "
            f"rows={total_rows} | "
            f"time={format_duration(time.perf_counter() - started)} | "
            f"errors={'no' if status == 'SUCCESS' else 'yes'}"
        )
        log.close()

    return exit_code


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
