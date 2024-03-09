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
            btree_header = parse_btree_header(page, is_first_page=True)
            print(f"number of tables: {btree_header.cell_count}")
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
    else:
        rightmost_pointer = 0
    return BTreeHeader(
        type_,
        first_freeblock,
        cell_count,
        cell_content_start or 65536,
        fragmented_free_bytes,
        rightmost_pointer,
    )


if __name__ == "__main__":
    main()
