import struct
import sys
from collections import namedtuple


def main():
    database_file_path = sys.argv[1]
    command = sys.argv[2]

    if command == ".dbinfo":
        with open(database_file_path, "rb") as database_file:
            database_file.seek(16)  # Skip the first 16 bytes of the header
            page_size = int.from_bytes(database_file.read(2), byteorder="big")

            print(f"database page size: {page_size}")

            database_file.seek(0)
            page = database_file.read(page_size)
            btree_header = parse_btree_header(page, is_first_page=True)[0]
            print(f"number of tables: {btree_header.cell_count}")

    elif command == ".tables":
        with open(database_file_path, "rb") as database_file:
            database_file.seek(16)  # Skip the first 16 bytes of the header
            page_size = int.from_bytes(database_file.read(2), byteorder="big")

            database_file.seek(56)
            text_encoding = ["utf-8", "utf-16-le", "utf-16-be"][int.from_bytes(database_file.read(4), byteorder="big") - 1]

            database_file.seek(0)
            page = database_file.read(page_size)
            btree_header, bytes_read = parse_btree_header(page, is_first_page=True)

            btree_offset = 100 + bytes_read
            cells = []
            for i in range(btree_header.cell_count):
                cell_content_offset, = struct.unpack_from(">H", page, btree_offset)
                btree_offset += 2

                _payload_size, bytes_read = parse_varint(page, cell_content_offset)
                cell_content_offset += bytes_read

                rowid, bytes_read = parse_varint(page, cell_content_offset)
                cell_content_offset += bytes_read

                column_values, bytes_read = parse_record(page, cell_content_offset, text_encoding)

                # ???
                # assert bytes_read == payload_size, (bytes_read, payload_size)

                column_values.insert(0, rowid)
                cells.append(column_values)

            print(" ".join(cell[2] for cell in cells))
    else:
        print(f"Invalid command: {command}")


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
        if byte & 0x80 == 0:
            n <<= 8
            n |= byte
            break
        else:
            n <<= 7
            n |= byte & 0x7F
    else:
        i = -1
    return n, i + 1 - offset


def parse_record(buf, offset, text_encoding):
    initial_offset = offset
    header_size, bytes_read = parse_varint(buf, offset)
    header_end = offset + header_size
    offset += bytes_read
    column_types = []
    while offset != header_end:
        column_serial_type, bytes_read = parse_varint(buf, offset)
        column_types.append(column_serial_type)
        offset += bytes_read

    column_values = []
    for column_serial_type in column_types:
        if column_serial_type == 0:
            column_values.append(None)
        elif 1 <= column_serial_type <= 6:
            number_byte_size = (
                column_serial_type
                if column_serial_type < 5
                else 6
                if column_serial_type == 5
                else 8
            )
            value = int.from_bytes(buf[offset:offset + number_byte_size], byteorder="big", signed=True)
            column_values.append(value)
            offset += number_byte_size
        elif column_serial_type == 7:
            value = struct.unpack_from(">d", buf, offset)
            column_values.append(value)
            offset += 8
        elif column_serial_type in (8, 9):
            column_values.append(int(column_serial_type == 9))
        elif column_serial_type >= 12 and column_serial_type % 2 == 0:
            value_len = (column_serial_type - 12) // 2
            column_values.append(buf[offset:offset + value_len])
            offset += value_len
        elif column_serial_type >= 13 and column_serial_type % 2 == 1:
            value_len = (column_serial_type - 13) // 2
            column_values.append(buf[offset:offset + value_len].decode(text_encoding))
            offset += value_len
        else:
            raise NotImplementedError(column_serial_type)

    return column_values, offset - initial_offset


if __name__ == "__main__":
    main()
