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
}

_keywords = {
    "SELECT".casefold(): "SELECT",
    "FROM".casefold(): "FROM",
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
            while it.peek() is not None and it.peek().isalnum():
                name += next(it)
            if name.casefold() in _keywords:
                yield Token(_keywords[name.casefold()], name)
            else:
                yield Token("NAME", name)
        else:
            raise ParseError(f"Unexpected token {c!r}")


def parse(text):
    yield from _parse(_peekable(scan(text)))


SelectStmt = namedtuple("SelectStmt", "selects,from_table")
FunctionExpr = namedtuple("FunctionExpr", "name,args")
NameExpr = namedtuple("NameExpr", "name")
StarExpr = namedtuple("StarExpr", "")


def _parse(it):
    if it.peek() and it.peek().type == "SELECT":
        yield _parse_select_stmt(it)
    else:
        raise ParseError(f"Unexpected token {it.peek()!r}")

    if it.peek() is not None:
        raise ParseError(f"Trailing characters after query: {it.peek()!r}")


def _parse_select_stmt(it):
    next(it)

    selects = []
    first = True
    while it.peek() and it.peek().type != "FROM":
        if first:
            first = False
        else:
            comma = next(it, None)
            if not comma or comma.type != "COMMA":
                raise ParseError(f"Expected comma, got {comma!r}")
        selects.append(_parse_selection(it))

    if not it.peek() or it.peek().type != "FROM":
        raise ParseError(f"Expected 'FROM', got {it.peek()!r}")

    next(it)

    try:
        from_table = next(it)
    except StopIteration:
        raise ParseError("Unexpected end of input, expected name")

    if from_table.type != "NAME":
        raise ParseError(f"Expected name, got {from_table.text!r}")

    semicolon = next(it, None)
    if semicolon is not None and semicolon.type != "SEMICOLON":
        raise ParseError(f"Expected end of input or semicolon, got {semicolon!r}")

    return SelectStmt(selects, from_table.text)


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
            comma = next(it, None)
            if comma is None or comma.type != "COMMA":
                raise ParseError(f"Expected comma, got {comma!r}")
        args.append(_parse_selection(it))

    rparen = next(it, None)
    if rparen is None or rparen.type != "RPAREN":
        raise ParseError(f"Expected rparen, got {rparen!r}")

    return FunctionExpr(name.text.upper(), args)
