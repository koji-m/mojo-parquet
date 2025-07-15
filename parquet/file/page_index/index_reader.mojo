import parquet.gen.parquet.ttypes as tt
from thrift.protocol.compact import TCompactProtocol
from thrift.transport import TMemoryBuffer
from parquet.file.page_index.index import Index, TypedIndex, NativeBool, NativeInt32, NativeInt64, NativeByteArray
from parquet.types import PhysicalType
from parquet.utils import Range

fn decode_column_index(bytes: List[UInt8], column_type: PhysicalType) raises -> Index:
    var transport = TMemoryBuffer(bytes, 0)
    var protocol = TCompactProtocol(
        transport,
        Optional[Bool](None),
        0,
        List[Int16](),
        0,
        List[Int16](),
        Optional[Int16](None),
    )
    var tcolumn_index = tt.ColumnIndex.read(protocol)
    if column_type == PhysicalType.BOOLEAN:
        return Index(
            TypedIndex[NativeBool](tcolumn_index),
        )
    elif column_type == PhysicalType.INT32:
        return Index(
            TypedIndex[NativeInt32](tcolumn_index),
        )
    elif column_type == PhysicalType.INT64:
        return Index(
            TypedIndex[NativeInt64](tcolumn_index),
        )
    elif column_type == PhysicalType.BYTE_ARRAY:
        return Index(
            TypedIndex[NativeByteArray](tcolumn_index),
        )
    else:
        raise Error("Unsupported column type for index decoding: ", column_type.value)

fn accumulate_range(l: Optional[Range], r: Optional[Range]) raises -> Optional[Range]:
    if not l:
        return r
    if not r:
        return l
    return Optional(Range(
        start=min(l.value().start, r.value().start),
        end=max(l.value().end, r.value().end),
    ))