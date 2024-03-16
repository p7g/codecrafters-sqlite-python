import struct
import sys
from collections import namedtuple

import app.parser as parser


def main():
    database_file_path = sys.argv[1]
    command = sys.argv[2]

    with open(database_file_path, "rb") as database_file:
        database_file.seek(16)  # Skip the first 16 bytes of the header
        page_size = int.from_bytes(database_file.read(2), byteorder="big")

        database_file.seek(20)
        page_reserved_space_size = int.from_bytes(
            database_file.read(1), byteorder="big"
        )

        database_file.seek(56)
        text_encoding = ["utf-8", "utf-16-le", "utf-16-be"][
            int.from_bytes(database_file.read(4), byteorder="big") - 1
        ]

        db_config = DBConfig(
            page_size=page_size,
            text_encoding=text_encoding,
            page_reserved=page_reserved_space_size,
        )

        if command == ".dbinfo":
            print(f"database page size: {page_size}")

            database_file.seek(0)
            page = database_file.read(page_size)
            btree_header = parse_btree_header(page, is_first_page=True)[0]
            print(f"number of tables: {btree_header.cell_count}")

        elif command == ".tables":
            print(
                " ".join(
                    row.tbl_name
                    for row in select_all_from_sqlite_schema(database_file, db_config)
                    if row.type == "table" and not row.tbl_name.startswith("sqlite_")
                )
            )
        else:
            stmt = next(parser.parse(command))

            if not isinstance(stmt, parser.SelectStmt):
                print("Only know select", file=sys.stderr)
                return 1

            table_name = stmt.from_table
            if table_name.casefold() in (
                "sqlite_schema".casefold(),
                "sqlite_master".casefold(),
                "sqlite_temp_schema".casefold(),
                "sqlite_temp_master".casefold(),
            ):
                table_schema = SqliteSchema(
                    "table",
                    "sqlite_schema",
                    "sqlite_schema",
                    1,
                    "CREATE TABLE sqlite_schema (\n"
                    "  type text,\n"
                    "  name text,\n"
                    "  tbl_name text,\n"
                    "  rootpage integer,\n"
                    "  sql text\n"
                    ");",
                )
                indexes = []
            else:
                # FIXME: be more efficient
                table_schema = None
                indexes = []
                for sqlite_schema in select_all_from_sqlite_schema(
                    database_file, db_config
                ):
                    if sqlite_schema.tbl_name.casefold() != table_name.casefold():
                        continue
                    elif sqlite_schema.type == "table":
                        table_schema = sqlite_schema
                    elif sqlite_schema.type == "index":
                        indexes.append(sqlite_schema)

                if table_schema is None:
                    print(f"Unknown table '{table_name}'", file=sys.stderr)
                    return 1

            create_table_ast = next(parser.parse(table_schema.sql))
            assert isinstance(create_table_ast, parser.CreateTableStmt)

            column_order = {
                name.casefold(): i
                for i, (name, _type) in enumerate(create_table_ast.columns)
            }
            column_order["rowid".casefold()] = ROWID_COL_IDX

            primary_key_column_idx = next(
                (
                    column_index
                    for column_index, column in enumerate(create_table_ast.columns)
                    if column.type.casefold().startswith(
                        "integer primary key".casefold()
                    )
                ),
                None,
            )

            table_info = TableInfo(
                rootpage=table_schema.rootpage, int_pk_column=primary_key_column_idx
            )

            is_count_star = (
                len(stmt.selects) == 1
                and isinstance(stmt.selects[0], parser.FunctionExpr)
                and stmt.selects[0].name == "COUNT"
                and len(stmt.selects[0].args) == 1
                and isinstance(stmt.selects[0].args[0], parser.StarExpr)
            )

            try:
                if len(stmt.selects) == 1 and isinstance(
                    stmt.selects[0], parser.StarExpr
                ):
                    selected_columns = list(range(len(column_order)))
                elif is_count_star:
                    selected_columns = []
                elif not all(
                    isinstance(select, parser.NameExpr) for select in stmt.selects
                ):
                    print("Only simple queries are supported", file=sys.stderr)
                    return 1
                else:
                    selected_columns = [
                        column_order[name_expr.name.casefold()]
                        for name_expr in stmt.selects
                    ]
            except KeyError as e:
                print(f"Unknown column {e}", file=sys.stderr)
                return 1

            where = None
            if stmt.where:
                # TODO: find ideal index. For each alternation of the WHERE
                # clause, find an index that can be used (if any). From there
                # use some heuristics to decide which index would be most
                # effective and use that one.
                filter_column_name = stmt.where.lhs.name

                for index_schema in indexes:
                    create_index = next(parser.parse(index_schema.sql))
                    assert isinstance(
                        create_index, parser.CreateIndexStmt
                    ), create_index
                    if (
                        create_index.columns[0].text.casefold()
                        == filter_column_name.casefold()
                    ):
                        break
                else:
                    index_schema = None

                index_rootpage = index_schema.rootpage if index_schema else None

                where = Where(
                    BinOp(
                        "=",
                        column_order[filter_column_name.casefold()],
                        stmt.where.rhs.text,
                    ),
                    index_rootpage,
                )

            rows = read_table(
                database_file,
                db_config,
                table_info,
                selected_columns,
                where,
            )

            if is_count_star:
                i = -1
                for i, _ in enumerate(rows):
                    pass
                print(i + 1)
            else:
                for column_values in rows:
                    print("|".join(str(val) for val in column_values))

    return 0


BTreeHeader = namedtuple(
    "BTreeHeader",
    [
        "type",
        "first_freeblock",
        "cell_count",
        "cell_content_start",
        "fragmented_free_bytes",
        "rightmost_pointer",
    ],
)


BTREE_PAGE_INTERIOR_INDEX = 0x02
BTREE_PAGE_INTERIOR_TABLE = 0x05
BTREE_PAGE_LEAF_INDEX = 0x0A
BTREE_PAGE_LEAF_TABLE = 0x0D


def parse_btree_header(page, is_first_page=False):
    offset = 100 if is_first_page else 0
    type_, first_freeblock, cell_count, cell_content_start, fragmented_free_bytes = (
        struct.unpack_from(">BHHHB", page, offset)
    )
    if type_ in (BTREE_PAGE_INTERIOR_INDEX, BTREE_PAGE_INTERIOR_TABLE):
        (rightmost_pointer,) = struct.unpack_from(">I", page, offset + 8)
        bytes_read = 12
    else:
        rightmost_pointer = 0
        bytes_read = 8
    return BTreeHeader(
        type_,
        first_freeblock,
        cell_count,
        cell_content_start or 65536,
        fragmented_free_bytes,
        rightmost_pointer,
    ), bytes_read


def parse_varint(buf, offset=0):
    n = 0
    for i in range(offset, offset + 9):
        byte = buf[i]
        n <<= 7
        n |= byte & 0x7F
        if byte & 0x80 == 0:
            break
    else:
        i = -1
    return n, i + 1 - offset


def size_for_type(serial_type):
    if serial_type < 5:
        return serial_type
    elif serial_type == 5:
        return 6
    elif 6 <= serial_type <= 7:
        return 8
    elif 8 <= serial_type <= 9:
        return 0
    elif serial_type >= 12 and serial_type % 2 == 0:
        return (serial_type - 12) // 2
    elif serial_type >= 13 and serial_type % 2 == 1:
        return (serial_type - 13) // 2
    else:
        raise NotImplementedError(serial_type)


def parse_record(db_config, table_info, page, rowid, offset, selection, where):
    initial_offset = offset
    header_size, bytes_read = parse_varint(page, offset)
    header_end = offset + header_size
    offset += bytes_read
    column_types = []
    total_size = header_size
    while offset != header_end:
        column_serial_type, bytes_read = parse_varint(page, offset)
        column_size = size_for_type(column_serial_type)
        column_types.append((column_serial_type, column_size))
        offset += bytes_read
        total_size += column_size

    column_selection = {column_id: order for order, column_id in enumerate(selection)}

    column_values: list = [None] * len(column_selection)
    for column_id, (column_serial_type, size) in enumerate(column_types):
        if column_id not in column_selection and (
            not where or column_id != where.condition.lhs
        ):
            offset += size
            continue

        if column_serial_type == 0:
            if column_id == table_info.int_pk_column:
                value = rowid
            else:
                value = None
        elif 1 <= column_serial_type <= 6:
            number_byte_size = (
                column_serial_type
                if column_serial_type < 5
                else 6
                if column_serial_type == 5
                else 8
            )
            value = int.from_bytes(
                page[offset : offset + number_byte_size], byteorder="big", signed=True
            )
        elif column_serial_type == 7:
            value = struct.unpack_from(">d", page, offset)
        elif column_serial_type in (8, 9):
            value = int(column_serial_type == 9)
        elif column_serial_type >= 12 and column_serial_type % 2 == 0:
            value_len = (column_serial_type - 12) // 2
            value = page[offset : offset + value_len]
        elif column_serial_type >= 13 and column_serial_type % 2 == 1:
            value_len = (column_serial_type - 13) // 2
            blob_value = page[offset : offset + value_len]
            try:
                value = blob_value.decode(db_config.text_encoding)
            except UnicodeDecodeError:
                # FIXME: why does this happen?
                value = blob_value
        else:
            raise NotImplementedError(column_serial_type)

        offset += size

        if where and column_id == where.condition.lhs and value != where.condition.rhs:
            return None, total_size

        if column_id in column_selection:
            column_values[column_selection[column_id]] = value

    return column_values, offset - initial_offset


DBConfig = namedtuple("DBConfig", "page_size,text_encoding,page_reserved")
TableInfo = namedtuple("TableInfo", "rootpage,int_pk_column")
BinOp = namedtuple("BinOp", "op,lhs,rhs")
Where = namedtuple("Where", "condition,index_rootpage")
ROWID_COL_IDX = -1


def get_page(file, db_config, id_):
    file.seek((id_ - 1) * db_config.page_size)
    return file.read(db_config.page_size)


def read_table(file, db_config, table_info, selection, where):
    if where and where.index_rootpage:
        matching_ids = list(
            _read_index(file, db_config, table_info, where.index_rootpage, where)
        )
        page = get_page(file, db_config, table_info.rootpage)
        yield from _read_table_by_id(
            file,
            db_config,
            table_info,
            page,
            selection,
            (min(matching_ids), max(matching_ids)),
            set(matching_ids),
        )
    else:
        page = get_page(file, db_config, table_info.rootpage)
        yield from _read_table(file, db_config, table_info, page, selection, where)


def _read_table(file, db_config, table_info, page, selection, where):
    btree_header, bytes_read = parse_btree_header(
        page, is_first_page=table_info.rootpage == 1
    )

    btree_offset = bytes_read
    if table_info.rootpage == 1:
        btree_offset += 100

    for i in range(btree_header.cell_count):
        (cell_content_offset,) = struct.unpack_from(">H", page, btree_offset + 2 * i)

        if btree_header.type == BTREE_PAGE_INTERIOR_TABLE:
            (left_ptr,) = struct.unpack_from(">I", page, cell_content_offset)
            left_page = get_page(file, db_config, left_ptr)
            yield from _read_table(
                file, db_config, table_info, left_page, selection, where
            )
        else:
            assert btree_header.type == BTREE_PAGE_LEAF_TABLE
            payload_size, bytes_read = parse_varint(page, cell_content_offset)
            cell_content_offset += bytes_read

            rowid, bytes_read = parse_varint(page, cell_content_offset)
            cell_content_offset += bytes_read

            if (
                where
                and where.condition.op == "="
                and where.condition.lhs == ROWID_COL_IDX
                and rowid != where.condition.rhs
            ):
                column_values = None
            else:
                column_values, bytes_read = parse_record(
                    db_config,
                    table_info,
                    page,
                    rowid,
                    cell_content_offset,
                    selection,
                    where,
                )
                assert bytes_read == payload_size, (bytes_read, payload_size)

            # filtered out
            if column_values is None:
                continue

            yield column_values

    if btree_header.type == BTREE_PAGE_INTERIOR_TABLE:
        rightmost_page = get_page(file, db_config, btree_header.rightmost_pointer)
        yield from _read_table(
            file, db_config, table_info, rightmost_page, selection, where
        )


def _read_index(file, db_config, table_info, page_id, where):
    page = get_page(file, db_config, page_id)
    btree_header, cell_array_offset = parse_btree_header(page)
    is_leaf = btree_header.type == BTREE_PAGE_LEAF_INDEX
    assert is_leaf or btree_header.type == BTREE_PAGE_INTERIOR_INDEX

    def _read_key(cell_content_offset):
        if not is_leaf:
            (left_pointer,) = struct.unpack_from(">I", page, cell_content_offset)
            cell_content_offset += 4
        else:
            left_pointer = None

        payload_size, bytes_read = parse_varint(page, cell_content_offset)
        cell_content_offset += bytes_read
        index_data, bytes_read = parse_record(
            db_config, table_info, page, None, cell_content_offset, [0, 1], None
        )
        assert bytes_read == payload_size
        assert index_data is not None
        return index_data, left_pointer

    L = 0
    R = btree_header.cell_count
    while L < R:
        i = (L + R) // 2

        (cell_content_offset,) = struct.unpack_from(
            ">H", page, cell_array_offset + 2 * i
        )
        index_data, left_pointer = _read_key(cell_content_offset)

        if index_data[0] < where.condition.rhs:
            L = i + 1
        else:
            R = i

    for i in range(L, btree_header.cell_count):
        (cell_content_offset,) = struct.unpack_from(
            ">H", page, cell_array_offset + 2 * i
        )
        index_data, left_pointer = _read_key(cell_content_offset)

        if not is_leaf:
            assert left_pointer is not None
            yield from _read_index(file, db_config, table_info, left_pointer, where)

        if index_data[0] == where.condition.rhs:
            yield index_data[1]

        if index_data[0] > where.condition.rhs:
            break
    else:
        if not is_leaf:
            yield from _read_index(
                file, db_config, table_info, btree_header.rightmost_pointer, where
            )


def _read_table_by_id(file, db_config, table_info, page, selection, id_range, ids):
    btree_header, bytes_read = parse_btree_header(
        page, is_first_page=table_info.rootpage == 1
    )
    is_leaf = btree_header.type == BTREE_PAGE_LEAF_TABLE
    assert is_leaf or btree_header.type == BTREE_PAGE_INTERIOR_TABLE

    btree_offset = bytes_read
    if table_info.rootpage == 1:
        btree_offset += 100

    L = 0
    R = btree_header.cell_count
    while L < R:
        i = (L + R) // 2

        (cell_content_offset,) = struct.unpack_from(">H", page, btree_offset + 2 * i)

        if is_leaf:
            _payload_size, bytes_read = parse_varint(page, cell_content_offset)
            cell_content_offset += bytes_read
            rowid, bytes_read = parse_varint(page, cell_content_offset)
            cell_content_offset += bytes_read
        else:
            rowid, bytes_read = parse_varint(page, cell_content_offset + 4)

        if rowid < id_range[0]:
            L = i + 1
        else:
            R = i

    for i in range(L, btree_header.cell_count):
        (cell_content_offset,) = struct.unpack_from(">H", page, btree_offset + 2 * i)

        if not is_leaf:
            (left_ptr,) = struct.unpack_from(">I", page, cell_content_offset)
            left_page = get_page(file, db_config, left_ptr)
            yield from _read_table_by_id(
                file, db_config, table_info, left_page, selection, id_range, ids
            )
        else:
            payload_size, bytes_read = parse_varint(page, cell_content_offset)
            cell_content_offset += bytes_read

            rowid, bytes_read = parse_varint(page, cell_content_offset)
            cell_content_offset += bytes_read

            if rowid not in ids:
                column_values = None
            else:
                column_values, bytes_read = parse_record(
                    db_config,
                    table_info,
                    page,
                    rowid,
                    cell_content_offset,
                    selection,
                    None,
                )
                assert bytes_read == payload_size, (bytes_read, payload_size)

            # filtered out
            if column_values is not None:
                yield column_values

            if rowid >= id_range[1]:
                break
    else:
        if not is_leaf:
            rightmost_page = get_page(file, db_config, btree_header.rightmost_pointer)
            yield from _read_table_by_id(
                file, db_config, table_info, rightmost_page, selection, id_range, ids
            )


SqliteSchema = namedtuple(
    "SqliteSchema", ["type", "name", "tbl_name", "rootpage", "sql"]
)


def select_all_from_sqlite_schema(file, db_config):
    for column_values in read_table(
        file, db_config, TableInfo(1, None), list(range(5)), None
    ):
        yield SqliteSchema(*column_values)


if __name__ == "__main__":
    sys.exit(main())
