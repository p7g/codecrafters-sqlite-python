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

        database_file.seek(56)
        text_encoding = ["utf-8", "utf-16-le", "utf-16-be"][
            int.from_bytes(database_file.read(4), byteorder="big") - 1
        ]

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
                    for row in select_all_from_sqlite_schema(
                        database_file, page_size, text_encoding
                    )
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
                table_info = SqliteSchema(
                    0,
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
            else:
                # FIXME: be more efficient
                try:
                    table_info = next(
                        table_info
                        for table_info in select_all_from_sqlite_schema(
                            database_file, page_size, text_encoding
                        )
                        if table_info.type == "table"
                        and table_info.tbl_name.casefold() == table_name.casefold()
                    )
                except StopIteration:
                    print(f"Unknown table '{table_name}'", file=sys.stderr)
                    return 1

            page = get_page(database_file, table_info.rootpage, page_size)
            btree_header, bytes_read = parse_btree_header(
                page, table_info.rootpage == 1
            )

            if (
                len(stmt.selects) == 1
                and isinstance(stmt.selects[0], parser.FunctionExpr)
                and stmt.selects[0].name == "COUNT"
                and len(stmt.selects[0].args) == 1
                and isinstance(stmt.selects[0].args[0], parser.StarExpr)
            ):
                print(btree_header.cell_count)
            else:
                columns = [
                    tuple(column_spec.strip().split(None, 1))
                    for column_spec in (
                        table_info.sql.split("(", 1)[1].rsplit(")", 1)[0].split(",")
                    )
                ]
                column_order = {
                    name.casefold(): i for i, (name, _type) in enumerate(columns)
                }

                try:
                    if len(stmt.selects) == 1 and isinstance(
                        stmt.selects[0], parser.StarExpr
                    ):
                        selected_columns = list(range(len(column_order)))
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

                    primary_key_selected_column_idx = next(
                        (
                            selection_index
                            for selection_index, column_index in enumerate(
                                selected_columns
                            )
                            if columns[column_index][1].casefold().split()[:3]
                            == [
                                "integer".casefold(),
                                "primary".casefold(),
                                "key".casefold(),
                            ]
                        ),
                        None,
                    )
                except KeyError as e:
                    print(f"Unknown column {e}", file=sys.stderr)
                    return 1

                for rowid, column_values in read_table(
                    database_file,
                    table_info.rootpage,
                    page_size,
                    text_encoding,
                    selected_columns,
                ):
                    if primary_key_selected_column_idx is not None:
                        column_values[primary_key_selected_column_idx] = rowid
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


def parse_record(buf, offset, text_encoding, columns):
    initial_offset = offset
    header_size, bytes_read = parse_varint(buf, offset)
    header_end = offset + header_size
    offset += bytes_read
    column_types = []
    while offset != header_end:
        column_serial_type, bytes_read = parse_varint(buf, offset)
        column_types.append((column_serial_type, size_for_type(column_serial_type)))
        offset += bytes_read

    column_selection = {column_id: order for order, column_id in enumerate(columns)}

    column_values: list = [None] * len(column_selection)
    for i, (column_serial_type, size) in enumerate(column_types):
        if i not in column_selection:
            offset += size
            continue

        if column_serial_type == 0:
            continue  # None is already stored in column_values
        elif 1 <= column_serial_type <= 6:
            number_byte_size = (
                column_serial_type
                if column_serial_type < 5
                else 6
                if column_serial_type == 5
                else 8
            )
            value = int.from_bytes(
                buf[offset : offset + number_byte_size], byteorder="big", signed=True
            )
        elif column_serial_type == 7:
            value = struct.unpack_from(">d", buf, offset)
        elif column_serial_type in (8, 9):
            value = int(column_serial_type == 9)
        elif column_serial_type >= 12 and column_serial_type % 2 == 0:
            value_len = (column_serial_type - 12) // 2
            value = buf[offset : offset + value_len]
        elif column_serial_type >= 13 and column_serial_type % 2 == 1:
            value_len = (column_serial_type - 13) // 2
            blob_value = buf[offset : offset + value_len]
            try:
                value = blob_value.decode(text_encoding)
            except UnicodeDecodeError:
                # FIXME: why does this happen?
                value = blob_value
        else:
            raise NotImplementedError(column_serial_type)

        column_values[column_selection[i]] = value
        offset += size

    return column_values, offset - initial_offset


def get_page(file, id_, page_size):
    file.seek((id_ - 1) * page_size)
    return file.read(page_size)


def read_table(file, rootpage, page_size, text_encoding, columns):
    page = get_page(file, rootpage, page_size)
    btree_header, bytes_read = parse_btree_header(page, is_first_page=rootpage == 1)

    btree_offset = bytes_read
    if rootpage == 1:
        btree_offset += 100

    for _i in range(btree_header.cell_count):
        (cell_content_offset,) = struct.unpack_from(">H", page, btree_offset)
        btree_offset += 2

        _payload_size, bytes_read = parse_varint(page, cell_content_offset)
        cell_content_offset += bytes_read

        rowid, bytes_read = parse_varint(page, cell_content_offset)
        cell_content_offset += bytes_read

        column_values, bytes_read = parse_record(
            page, cell_content_offset, text_encoding, columns
        )

        # ???
        # assert bytes_read == payload_size, (bytes_read, payload_size)

        yield (rowid, column_values)


SqliteSchema = namedtuple(
    "SqliteSchema", ["rowid", "type", "name", "tbl_name", "rootpage", "sql"]
)


def select_all_from_sqlite_schema(file, page_size, text_encoding):
    for rowid, column_values in read_table(
        file, 1, page_size, text_encoding, list(range(5))
    ):
        yield SqliteSchema(rowid, *column_values)


if __name__ == "__main__":
    sys.exit(main())
