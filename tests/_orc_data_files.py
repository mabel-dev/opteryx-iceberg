"""ORC data files for Iceberg tests (opteryx-core docs/ORC_READER_DESIGN.md §9, D2).

pyarrow.orc writes the data - an independent ORC writer - but cannot set ORC type
attributes, and Iceberg identifies a column by the `iceberg.id` attribute. So the helper
rewrites the footer of an UNCOMPRESSED pyarrow file, appending the attribute to each
top-level column's type, and patches the postscript's footer length. Nothing else in the
file moves: stripes, row counts and streams are untouched.
"""

import io

import pyarrow as pa
import pyarrow.orc as po


def _varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _walk(buf):
    """[(field, wire, value)] for one protobuf message; value is an int (varint) or bytes."""
    out, i = [], 0
    while i < len(buf):
        key, shift = 0, 0
        while True:
            c = buf[i]
            i += 1
            key |= (c & 0x7F) << shift
            shift += 7
            if not c & 0x80:
                break
        field, wire = key >> 3, key & 7
        if wire == 0:
            v, shift = 0, 0
            while True:
                c = buf[i]
                i += 1
                v |= (c & 0x7F) << shift
                shift += 7
                if not c & 0x80:
                    break
            out.append((field, wire, v))
        elif wire == 2:
            n, shift = 0, 0
            while True:
                c = buf[i]
                i += 1
                n |= (c & 0x7F) << shift
                shift += 7
                if not c & 0x80:
                    break
            out.append((field, wire, bytes(buf[i : i + n])))
            i += n
        else:
            raise ValueError(f"unexpected wire type {wire}")
    return out


def _emit(fields):
    out = bytearray()
    for field, wire, value in fields:
        out += _varint((field << 3) | wire)
        if wire == 0:
            out += _varint(value)
        else:
            out += _varint(len(value)) + value
    return bytes(out)


def _attribute(key, value):
    pair = _emit([(1, 2, key.encode()), (2, 2, value.encode())])
    return (7, 2, pair)


def with_iceberg_ids(data, ids, attributes=None):
    """`data` (an uncompressed ORC file) with `iceberg.id` set on each top-level column
    named in `ids` ({column name: field id}). A column not named gets no attribute.
    `attributes` ({column name: {key: value}}) adds further type attributes, e.g.
    `iceberg.binary-type` / `iceberg.length` / `iceberg.long-type`."""
    ps_len = data[-1]
    ps = _walk(data[-1 - ps_len : -1])
    footer_len = next(v for f, _, v in ps if f == 1)
    if next(v for f, _, v in ps if f == 2) != 0:
        raise ValueError("with_iceberg_ids rewrites an uncompressed file's footer")
    body_end = len(data) - 1 - ps_len - footer_len
    footer = _walk(data[body_end : body_end + footer_len])
    types = [f for f in footer if f[0] == 4]
    root = _walk(types[0][2])
    names = [v.decode() for f, _, v in root if f == 3]
    column_types = types[1:]
    rewritten = []
    for name, (_, wire, raw) in zip(names, column_types):
        fields = _walk(raw)
        if name in ids:
            fields.append(_attribute("iceberg.id", str(ids[name])))
        for key, value in (attributes or {}).get(name, {}).items():
            fields.append(_attribute(key, value))
        rewritten.append((4, 2, _emit(fields)))
    new_footer = [types[0]] + rewritten
    out_footer = []
    seen_types = False
    for f in footer:
        if f[0] == 4:
            if not seen_types:
                out_footer.extend(new_footer)
                seen_types = True
            continue
        out_footer.append(f)
    footer_bytes = _emit(out_footer)
    new_ps = _emit([(1, 0, len(footer_bytes)) if f == 1 else (f, w, v) for f, w, v in ps])
    return data[:body_end] + footer_bytes + new_ps + bytes([len(new_ps)])


def _subtypes(fields):
    """The child type ids of one ORC type (packed or repeated varints)."""
    out = []
    for f, wire, v in fields:
        if f != 2:
            continue
        if wire == 0:
            out.append(v)
        else:
            i = 0
            while i < len(v):
                n, shift = 0, 0
                while True:
                    c = v[i]
                    i += 1
                    n |= (c & 0x7F) << shift
                    shift += 7
                    if not c & 0x80:
                        break
                out.append(n)
    return out


def with_nested_iceberg_ids(data, ids):
    """`data` (an uncompressed ORC file) with `iceberg.id` set on every type whose PATH is in `ids`
    ({path: field id}) - nested types included, as Iceberg's ORC writer sets them. Paths: a top-level
    column is its name, a struct member `s.a`, a list's element `l[]`, a map's `m.key` / `m.value`."""
    ps_len = data[-1]
    ps = _walk(data[-1 - ps_len : -1])
    footer_len = next(v for f, _, v in ps if f == 1)
    if next(v for f, _, v in ps if f == 2) != 0:
        raise ValueError("with_nested_iceberg_ids rewrites an uncompressed file's footer")
    body_end = len(data) - 1 - ps_len - footer_len
    footer = _walk(data[body_end : body_end + footer_len])
    types = [f for f in footer if f[0] == 4]
    parsed = [_walk(raw) for _, _, raw in types]
    paths = {}

    def visit(type_id, path):
        paths[type_id] = path
        fields = parsed[type_id]
        kind = next(v for f, _, v in fields if f == 1)
        names = [v.decode() for f, _, v in fields if f == 3]
        for i, child in enumerate(_subtypes(fields)):
            if kind == 12:      # STRUCT
                visit(child, f"{path}.{names[i]}" if path else names[i])
            elif kind == 10:    # LIST
                visit(child, f"{path}[]")
            elif kind == 11:    # MAP
                visit(child, f"{path}.{'key' if i == 0 else 'value'}")

    visit(0, "")
    rewritten = []
    for type_id, fields in enumerate(parsed):
        fields = list(fields)
        path = paths.get(type_id)
        if path in ids:
            fields.append(_attribute("iceberg.id", str(ids[path])))
        rewritten.append((4, 2, _emit(fields)))
    out_footer, placed = [], False
    for f in footer:
        if f[0] == 4:
            if not placed:
                out_footer.extend(rewritten)
                placed = True
            continue
        out_footer.append(f)
    footer_bytes = _emit(out_footer)
    new_ps = _emit([(1, 0, len(footer_bytes)) if f == 1 else (f, w, v) for f, w, v in ps])
    return data[:body_end] + footer_bytes + new_ps + bytes([len(new_ps)])


def orc_bytes(table, ids=None, attributes=None):
    """An uncompressed pyarrow ORC file for `table`, with `iceberg.id` on the columns in
    `ids` ({name: id}) and any further type `attributes` ({name: {key: value}})."""
    buf = io.BytesIO()
    po.write_table(table, buf, compression="uncompressed")
    data = buf.getvalue()
    return with_iceberg_ids(data, ids or {}, attributes) if (ids or attributes) else data
