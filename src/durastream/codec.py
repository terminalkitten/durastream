import struct
import zlib

HEADER = struct.Struct(">II")  # len, crc
HEADER_SIZE = HEADER.size  # 8
TOKEN_WIDTH = 20  # zero-padded offset token width


def pack_frame(payload: bytes) -> bytes:
    return HEADER.pack(len(payload), zlib.crc32(payload)) + payload


def iter_frames(data: bytes):
    """
    Yield payload, next_pos per intact frame
    stop at the first torn/corrupt one.
    """
    pos = 0
    n = len(data)
    while pos + HEADER_SIZE <= n:
        length, crc = HEADER.unpack_from(data, pos)
        end = pos + HEADER_SIZE + length
        if end > n:
            break  # torn tail: header promises bytes we don't have
        payload = data[pos + HEADER_SIZE : end]
        if zlib.crc32(payload) != crc:
            break  # corruption: reject this record and everything after
        yield payload, end
        pos = end


def to_token(offset: int) -> str:
    """offset, 20-char zero-padded token."""
    return str(offset).zfill(TOKEN_WIDTH)


def from_token(token: str, next_offset: int) -> int:
    """offset token, handles '-1' (start) and 'now' (tail)."""
    token = token.strip()
    if token == "-1":
        return 0
    if token == "now":
        return next_offset
    return int(token)
