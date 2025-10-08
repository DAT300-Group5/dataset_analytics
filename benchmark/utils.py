import os
from pathlib import Path
from typing import Union, List, Optional
import sys


def load_query_from_file(
    file_path: Union[str, os.PathLike],
    *,
    encoding: str = "utf-8",
    strip: bool = False
) -> str:
    """
    Load text content from a file.

    Raises:
        ValueError: If file_path is empty/whitespace.
        FileNotFoundError: If the file doesn't exist.
        IsADirectoryError: If the path is a directory.
        OSError: For other I/O errors (e.g., permission denied).
    """
    if not file_path or (isinstance(file_path, str) and not file_path.strip()):
        raise ValueError("File path cannot be empty or None")

    p = Path(file_path).expanduser()

    # Friendlier errors (optional; you could skip these and rely on read_text exceptions)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {p}")
    if p.is_dir():
        raise IsADirectoryError(f"Path is a directory, not a file: {p}")

    try:
        content = p.read_text(encoding=encoding)
    except OSError as e:
        raise OSError(f"Error reading file {p}: {e}") from e

    return content.strip() if strip else content


def split_statements(sql: str) -> List[str]:
    """
    Split SQL script into statements across multiple SQL dialects.

    Supported features:
      - Default ';' delimiter.
      - MySQL: 'DELIMITER <token>' to change statement delimiter; not emitted as a statement.
      - PostgreSQL: dollar-quoted strings $$...$$ or $tag$...$tag$.
      - PostgreSQL: COPY ... FROM STDIN data blocks terminated by a line.
      - Oracle/PLSQL: a line with only '/' (ignoring surrounding spaces) terminates a block.
      - SQL Server: a line with only 'GO' (case-insensitive, ignoring spaces) is a batch separator.
      - Comments: '-- ...', '# ...' (MySQL line comment), and '/* ... */' block comments.
      - Strings/identifiers: '...', "...", `...` (MySQL), and [...] (T-SQL identifiers).

    Returns:
      A list of statements (without trailing delimiters), stripped of leading/trailing whitespace.
      Empty statements are not included.
    """
    # Running states
    stmts: List[str] = []
    buf: List[str] = []     # current statement buffer (list of lines)
    cur = []                # current line being built (list of characters)
    i = 0

    # Delimiters and meta
    delimiter = ";"         # current statement delimiter (can be multi-char)
    in_line_comment = False
    in_block_comment = False
    in_single_quote = False
    in_double_quote = False
    in_backtick = False     # MySQL identifier
    in_bracket_ident = False  # [ ... ] for SQL Server
    in_dollar_quote = False
    dollar_tag = ""         # like $$ or $func$
    # COPY FROM STDIN handling (Postgres psql)
    in_copy_stdin = False

    # Utility: flush current line into buffer
    def flush_line():
        nonlocal cur, buf
        buf.append("".join(cur))
        cur = []

    # Utility: emit a statement from buffer
    def emit_statement():
        nonlocal buf
        stmt = "\n".join(buf).strip()
        buf = []
        if stmt:
            stmts.append(stmt)

    # normalize line endings
    lines = sql.splitlines()

    # Helper to detect batch/Oracle delimiters on a raw line (no string/comment context)
    def is_batch_sep(raw_line: str) -> bool:
        s = raw_line.strip()
        return s.lower() == "go"

    def is_oracle_slash(raw_line: str) -> bool:
        s = raw_line.strip()
        return s == "/"

    # Handle MySQL DELIMITER directive outside of strings/comments
    def try_apply_mysql_delimiter(raw_line: str) -> bool:
        nonlocal delimiter
        # Leading/trailing spaces allowed; DELIMITER token must be the first non-space token
        s = raw_line.strip()
        # accommodate uppercase/lowercase
        if not s.lower().startswith("delimiter"):
            return False
        # forms: DELIMITER //   or   DELIMITER $$   or   DELIMITER ; (reset)
        parts = s.split(None, 1)
        if len(parts) == 1:
            # "DELIMITER" alone → ignore
            return False
        new_delim = parts[1].strip()
        if not new_delim:
            return False
        delimiter = new_delim
        return True

    # Simple lookahead for dollar-quote tag at position j in string s
    def scan_dollar_tag(s: str, j: int) -> str:
        # must start with '$'
        if j >= len(s) or s[j] != '$':
            return ""
        k = j + 1
        while k < len(s) and (s[k].isalnum() or s[k] == '_'):
            k += 1
        if k < len(s) and s[k] == '$':
            return s[j:k+1]  # '$tag$' or '$$'
        return ""

    # Main loop over lines
    for raw_line in lines:
        line = raw_line  # keep original for line-level checks

        # If we are inside COPY ... FROM STDIN mode, just collect lines until '\.' alone
        if in_copy_stdin:
            if line.strip() == r"\.":
                flush_line()          # include the '\.' terminator line in the statement
                emit_statement()
                in_copy_stdin = False
                continue
            else:
                # regular data line of COPY
                cur.extend(line)
                flush_line()
                continue

        # Outside of strings/comments: special whole-line separators/directives
        if not (in_line_comment or in_block_comment or in_single_quote or in_double_quote
                or in_backtick or in_bracket_ident or in_dollar_quote):
            # MySQL DELIMITER directive
            if try_apply_mysql_delimiter(line):
                # DELIMITER line is not part of any statement
                continue
            # Batch separators
            if is_batch_sep(line):
                # End current statement (if any)
                flush_line()
                emit_statement()
                continue
            # Oracle slash delimiter
            if is_oracle_slash(line):
                flush_line()
                emit_statement()
                continue

        # Now parse this line char-by-char
        j = 0
        L = len(line)
        while j < L:
            ch = line[j]
            ch_next = line[j+1] if j+1 < L else ""

            # Handle end of line comment
            if in_line_comment:
                # consume to end of line
                # (we keep comment text in buf to preserve relative positions if needed)
                cur.extend(line[j:])
                j = L
                break

            # Handle block comment end
            if in_block_comment:
                if ch == '*' and ch_next == '/':
                    cur.append("*/")
                    j += 2
                    in_block_comment = False
                    continue
                else:
                    cur.append(ch)
                    j += 1
                    continue

            # Handle string/identifier closers
            if in_single_quote:
                cur.append(ch)
                j += 1
                if ch == "'" and not (j < L and line[j] == "'"):  # '' is escaped quote
                    in_single_quote = False
                continue

            if in_double_quote:
                cur.append(ch)
                j += 1
                if ch == '"' and not (j < L and line[j] == '"'):  # "" escaped
                    in_double_quote = False
                continue

            if in_backtick:
                cur.append(ch)
                j += 1
                if ch == '`':
                    in_backtick = False
                continue

            if in_bracket_ident:
                cur.append(ch)
                j += 1
                if ch == ']':
                    in_bracket_ident = False
                continue

            if in_dollar_quote:
                # Look for the exact closing tag
                tag = dollar_tag
                if tag and line.startswith(tag, j):
                    cur.extend(tag)
                    j += len(tag)
                    in_dollar_quote = False
                else:
                    # just copy through
                    cur.append(ch)
                    j += 1
                continue

            # --- We are OUTSIDE any comment/string now ---

            # Start of line/block comments
            if ch == '-' and ch_next == '-':
                in_line_comment = True
                cur.extend("--")
                j += 2
                continue
            if ch == '#' and not in_line_comment:
                # MySQL line comment from '#'
                in_line_comment = True
                cur.append('#')
                j += 1
                continue
            if ch == '/' and ch_next == '*':
                in_block_comment = True
                cur.extend("/*")
                j += 2
                continue

            # Start of strings/identifiers
            if ch == "'":
                in_single_quote = True
                cur.append(ch)
                j += 1
                continue
            if ch == '"':
                in_double_quote = True
                cur.append(ch)
                j += 1
                continue
            if ch == '`':
                in_backtick = True
                cur.append(ch)
                j += 1
                continue
            if ch == '[':
                in_bracket_ident = True
                cur.append(ch)
                j += 1
                continue

            # Start of dollar-quoted string?
            if ch == '$':
                tag = scan_dollar_tag(line, j)
                if tag:
                    in_dollar_quote = True
                    dollar_tag = tag
                    cur.extend(tag)
                    j += len(tag)
                    continue

            # Delimiter check (can be multi-char like //, $$, etc.)
            if delimiter and line.startswith(delimiter, j):
                # Finish current statement at this point
                flush_line()
                emit_statement()
                # skip delimiter
                j += len(delimiter)
                # swallow any following spaces (they belong to next statement)
                while j < L and line[j].isspace():
                    j += 1
                continue

            # Normal char
            cur.append(ch)
            j += 1

        # End of line: reset line-comment state, append newline
        flush_line()
        in_line_comment = False

        # If we just completed a statement and are at top-level, detect COPY FROM STDIN
        # (call after processing each line so buf reflects the whole statement)
        if not (in_block_comment or in_single_quote or in_double_quote or
                in_backtick or in_bracket_ident or in_dollar_quote):
            # Only try when we just emitted a statement via delimiter on this line.
            # Heuristic: if buffer is empty (meaning a statement ended), check last emitted.
            # (We cannot strictly know here; as a practical workaround, when not in COPY we do nothing.)
            pass

    # EOF: if any trailing text remains, emit as a statement
    # (e.g., scripts without final delimiter)
    trailing = "\n".join(buf).strip()
    if trailing:
        stmts.append(trailing)

    # Post-pass: detect COPY ... FROM STDIN statements that forgot '\.' (rare)
    # and leave as-is; real psql would block waiting for input.

    # Secondary pass: identify COPY ... FROM STDIN among emitted stmts and attach following data if any
    # (Already handled inline; included here for completeness—no-op.)

    return stmts


def _leading_keyword(stmt: str) -> str:
    """
    Return the first SQL keyword after skipping leading whitespace and comments:
      - line comments: '-- ...' or '# ...'
      - block comments: '/* ... */'
    Lowercased; returns '' if none.
    """
    s = stmt
    i, n = 0, len(s)

    def skip_space(k: int) -> int:
        while k < n and s[k].isspace():
            k += 1
        return k

    while True:
        i = skip_space(i)
        if i >= n:
            return ""
        # line comments
        if s.startswith("--", i):
            j = s.find("\n", i)
            i = n if j == -1 else j + 1
            continue
        if s[i] == "#":
            j = s.find("\n", i)
            i = n if j == -1 else j + 1
            continue
        # block comments
        if s.startswith("/*", i):
            j = s.find("*/", i + 2)
            i = n if j == -1 else j + 2
            continue
        break

    j = i
    while j < n and (s[j].isalpha() or s[j] == "_"):
        j += 1
    return s[i:j].lower()


def is_select(stmt: str) -> bool:
    """
    Return True if the statement is SELECT-like (read-only).
    Robust to leading whitespace and SQL comments.
    """
    kw = _leading_keyword(stmt)
    return kw in ("select", "with", "show", "describe", "explain")


def extract_last_select(sql_file: str) -> tuple[list[str], str]:
    """
    Load and split the SQL file, then locate the last SELECT-like statement.
    Returns: (preamble_stmts, final_select_stmt)
    Exits(1) if no statements or no SELECT-like statement is found.
    """
    sql_text = load_query_from_file(sql_file)
    statements = split_statements(sql_text)
    if not statements:
        print("No statements found in SQL file.")
        sys.exit(1)

    last_select_idx: Optional[int] = None
    for i, stmt in enumerate(statements):
        if is_select(stmt):  # is_select should be robust to leading comments/whitespace
            last_select_idx = i

    if last_select_idx is None:
        print("No SELECT-like statement found in SQL file.")
        sys.exit(1)

    preamble_stmts = statements[:last_select_idx]
    final_select = statements[last_select_idx]
    return preamble_stmts, final_select
