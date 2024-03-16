from collections import namedtuple

Token = namedtuple("Token", "type,text")
_NOTHING = object()


class ParseError(Exception):
    pass


class _peekable:
    def __init__(self, it):
        self._it = it
        self._peeked = _NOTHING

    def __next__(self):
        if self._peeked is not _NOTHING:
            val = self._peeked
            self._peeked = _NOTHING
            return val
        return next(self._it)

    def peek(self):
        if self._peeked is _NOTHING:
            try:
                self._peeked = next(self._it)
            except StopIteration:
                self._peeked = None
        return self._peeked


def scan(text):
    yield from _scan(_peekable(iter(text)))


_one_char_tokens = {
    ",": "COMMA",
    "(": "LPAREN",
    ")": "RPAREN",
    ";": "SEMICOLON",
    "*": "STAR",
    "=": "EQUAL",
}

_keywords = {
    "SELECT".casefold(): "SELECT",
    "FROM".casefold(): "FROM",
    "WHERE".casefold(): "WHERE",
    "CREATE".casefold(): "CREATE",
    "TABLE".casefold(): "TABLE",
    "INDEX".casefold(): "INDEX",
    "ON".casefold(): "ON",
}


def _scan(it):
    while True:
        c = next(it, None)
        if c is None:
            break

        if c.isspace():
            continue
        elif c in _one_char_tokens:
            yield Token(_one_char_tokens[c], c)
        elif c.isalpha():
            name = c
            while it.peek() is not None and (it.peek().isalnum() or it.peek() == "_"):
                name += next(it)
            if name.casefold() in _keywords:
                yield Token(_keywords[name.casefold()], name)
            else:
                yield Token("NAME", name)
        elif c in ("'", '"'):
            terminator = c
            str_content = ""
            while True:
                c = next(it, None)
                if c is None:
                    raise ParseError("Unterminated string literal")
                if c == terminator:
                    if it.peek() == terminator:
                        str_content += terminator
                    else:
                        break
                else:
                    str_content += c
            yield Token("STRING" if c == "'" else "NAME", str_content)
        else:
            raise ParseError(f"Unexpected token {c!r}")


def _expect(it, ty):
    try:
        tok = next(it)
    except StopIteration:
        raise ParseError(f"Expected {ty}, got end of input")
    if tok.type != ty:
        raise ParseError(f"Expected {ty}, got {tok.type}")
    return tok


def parse(text):
    yield from _parse(_peekable(scan(text)))


SelectStmt = namedtuple("SelectStmt", "selects,from_table,where")
CreateTableStmt = namedtuple("CreateTableStmt", "name,columns")
CreateIndexStmt = namedtuple("CreateIndexStmt", "name,table_name,columns")
CreateTableField = namedtuple("CreateTableField", "name,type")
FunctionExpr = namedtuple("FunctionExpr", "name,args")
NameExpr = namedtuple("NameExpr", "name")
StarExpr = namedtuple("StarExpr", "")
BinaryExpr = namedtuple("BinaryExpr", "op,lhs,rhs")
StringExpr = namedtuple("StringExpr", "text")


def _parse(it):
    if it.peek() and it.peek().type == "SELECT":
        yield _parse_select_stmt(it)
    elif it.peek() and it.peek().type == "CREATE":
        yield _parse_create(it)
    else:
        raise ParseError(f"Unexpected token {it.peek()!r}")

    if it.peek() is not None:
        raise ParseError(f"Trailing characters after query: {it.peek()!r}")


def _parse_select_stmt(it):
    _expect(it, "SELECT")

    selects = []
    first = True
    while it.peek() and it.peek().type != "FROM":
        if first:
            first = False
        else:
            _expect(it, "COMMA")
        selects.append(_parse_selection(it))

    _expect(it, "FROM")

    from_table = _expect(it, "NAME")

    tok = next(it, None)
    where = None
    if tok and tok.type == "WHERE":
        # FIXME: proper expression parsing
        lhs = next(it, None)
        op = next(it, None)
        rhs = next(it, None)
        if (
            lhs is None
            or op is None
            or rhs is None
            or lhs.type != "NAME"
            or op.type != "EQUAL"
            or rhs.type != "STRING"
        ):
            raise ParseError("Unsupported WHERE clause")
        where = BinaryExpr(op.type, NameExpr(lhs.text), StringExpr(rhs.text))
    elif tok and tok.type != "SEMICOLON":
        raise ParseError(f"Expected end of input or semicolon, got {tok.text!r}")

    return SelectStmt(selects, from_table.text, where)


def _parse_selection(it):
    name = next(it, None)
    if not name or name.type not in ("NAME", "STAR"):
        raise ParseError(f"Expected name or '*', got {name!r}")

    if name.type == "STAR":
        return StarExpr()

    if not it.peek() or it.peek().type != "LPAREN":
        return NameExpr(name.text)

    args = []
    first = True
    next(it)
    while it.peek() and it.peek().type != "RPAREN":
        if first:
            first = False
        else:
            _expect(it, "COMMA")
        args.append(_parse_selection(it))

    _expect(it, "RPAREN")

    return FunctionExpr(name.text.upper(), args)


def _parse_create(it):
    _expect(it, "CREATE")

    next_ = it.peek()
    if not next_:
        raise ParseError("Unexpected end of input in create statement")

    if next_.type == "TABLE":
        return _parse_create_table(it)
    elif next_.type == "INDEX":
        return _parse_create_index(it)
    else:
        raise ParseError(f"Unexpected {next_!r} in create statement")


def _parse_create_table(it):
    _expect(it, "TABLE")

    table_name = _expect(it, "NAME").text

    _expect(it, "LPAREN")
    columns = []
    first = True
    while it.peek() and it.peek().type != "RPAREN":
        if first:
            first = False
        else:
            _expect(it, "COMMA")
        col_name = _expect(it, "NAME").text
        type_parts = []
        while it.peek() and it.peek().type not in ("COMMA", "RPAREN"):
            type_parts.append(_expect(it, "NAME").text)
        col_type = " ".join(type_parts)
        columns.append(CreateTableField(col_name, col_type))

    _expect(it, "RPAREN")

    tok = next(it, None)
    if tok and tok.type != "SEMICOLON":
        raise ParseError(f"Expected end of input or semicolon, got {tok.text!r}")

    return CreateTableStmt(table_name, tuple(columns))


def _parse_create_index(it):
    _expect(it, "INDEX")

    index_name = _expect(it, "NAME").text

    _expect(it, "ON")

    table_name = _expect(it, "NAME").text

    _expect(it, "LPAREN")
    columns = []
    first = True
    while it.peek() and it.peek().type != "RPAREN":
        if first:
            first = False
        else:
            _expect(it, "COMMA")
        columns.append(_expect(it, "NAME"))

    _expect(it, "RPAREN")

    tok = next(it, None)
    if tok and tok.type != "SEMICOLON":
        raise ParseError(f"Expected end of input or semicolon, got {tok.text!r}")

    return CreateIndexStmt(index_name, table_name, tuple(columns))
