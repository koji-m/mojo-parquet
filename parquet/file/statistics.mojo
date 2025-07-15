from utils import Variant
from parquet.data_type import ByteArray
from parquet.types import PhysicalType, Encoding, CompressionCodec
import parquet.gen.parquet.ttypes as tt

@value
struct ValueStatistics[T: Copyable & Movable]:
    var min: Optional[T]
    var max: Optional[T]
    var distinct_count: Optional[UInt64]
    var null_count: Optional[UInt64]
    var is_max_value_exact: Bool
    var is_min_value_exact: Bool
    var is_min_max_deprecated: Bool
    var is_min_max_backwards_compatible: Bool

    fn __init__(
        out self,
        min: Optional[T],
        max: Optional[T],
        distinct_count: Optional[UInt64],
        null_count: Optional[UInt64],
        is_min_max_deprecated: Bool,
    ):
        self.min = min
        self.max = max
        self.distinct_count = distinct_count
        self.null_count = null_count
        self.is_max_value_exact = Bool(max)
        self.is_min_value_exact = Bool(min)
        self.is_min_max_deprecated = is_min_max_deprecated
        self.is_min_max_backwards_compatible = is_min_max_deprecated

alias Statistics = Variant[
    ValueStatistics[Bool],
    ValueStatistics[Int32],
    ValueStatistics[Int64],
    ValueStatistics[ByteArray],
    # ValueStatistics[Float32],
    # ValueStatistics[Float64],
    # ValueStatistics[FixedLenByteArray],
]

fn boolean_statistics(
    min: Optional[Bool],
    max: Optional[Bool],
    distinct_count: Optional[UInt64],
    null_count: Optional[UInt64],
    is_deprecated: Bool,
) -> Statistics:
    return Statistics(ValueStatistics[Bool](
        min=min,
        max=max,
        distinct_count=distinct_count,
        null_count=null_count,
        is_min_max_deprecated=is_deprecated,
    ))

fn int32_statistics(
    min: Optional[Int32],
    max: Optional[Int32],
    distinct_count: Optional[UInt64],
    null_count: Optional[UInt64],
    is_deprecated: Bool,
) -> Statistics:
    return Statistics(ValueStatistics[Int32](
        min=min,
        max=max,
        distinct_count=distinct_count,
        null_count=null_count,
        is_min_max_deprecated=is_deprecated,
    ))

fn int64_statistics(
    min: Optional[Int64],
    max: Optional[Int64],
    distinct_count: Optional[UInt64],
    null_count: Optional[UInt64],
    is_deprecated: Bool,
) -> Statistics:
    return Statistics(ValueStatistics[Int64](
        min=min,
        max=max,
        distinct_count=distinct_count,
        null_count=null_count,
        is_min_max_deprecated=is_deprecated,
    ))

fn byte_array_statistics(
    min: Optional[ByteArray],
    max: Optional[ByteArray],
    distinct_count: Optional[UInt64],
    null_count: Optional[UInt64],
    is_deprecated: Bool,
) -> Statistics:
    return Statistics(ValueStatistics[ByteArray](
        min=min,
        max=max,
        distinct_count=distinct_count,
        null_count=null_count,
        is_min_max_deprecated=is_deprecated,
    ))



fn from_thrift(
    physical_type: PhysicalType,
    t_statistics: Optional[tt.Statistics],
) raises -> Optional[Statistics]:
    if t_statistics:
        var stats = t_statistics.value()
        var null_count_ = stats.null_count

        if null_count_ and null_count_.value() < 0:
            raise Error("Statistics null count is negative: ", null_count_.value())

        var null_count = UInt64(null_count_.value())
        var distinct_count = Optional(UInt64(stats.distinct_count.value())) if stats.distinct_count else None
        var old_format = not stats.min_value and not stats.max_value
        var min = stats.min if old_format else stats.min_value
        var max = stats.max if old_format else stats.max_value

        fn check_len(min: Optional[List[UInt8]], max: Optional[List[UInt8]], length: Int) raises:
            if min and len(min.value()) < length:
                raise Error("Insufficient bytes to parse min statistic")
            if max and len(max.value()) < length:
                raise Error("Insufficient bytes to parse max statistic")

        if physical_type == PhysicalType.BOOLEAN:
            check_len(min, max, 1)
        elif physical_type == PhysicalType.INT32 or physical_type == PhysicalType.FLOAT:
            check_len(min, max, 4)
        elif physical_type == PhysicalType.INT64 or physical_type == PhysicalType.DOUBLE:
            check_len(min, max, 8)

        var res = Optional[Statistics](None)

        if physical_type == PhysicalType.BOOLEAN:
            res = Optional(boolean_statistics(
                Optional(Bool(min.value()[0] != 0)) if min else None,
                Optional(Bool(max.value()[0] != 0)) if max else None,
                distinct_count,
                null_count,
                old_format,
            ))
        elif physical_type == PhysicalType.INT32:
            if min:
                var ptr = min.value()[:4].unsafe_ptr()
                var int32_ptr = ptr.bitcast[Int32]()
                int32_min = Optional[Int32](int32_ptr[])
            else:
                int32_min = None
            if max:
                var ptr = max.value()[:4].unsafe_ptr()
                var int32_ptr = ptr.bitcast[Int32]()
                int32_max = Optional[Int32](int32_ptr[])
            else:
                int32_max = None
            res = Optional(int32_statistics(
                int32_min,
                int32_max,
                distinct_count,
                null_count,
                old_format,
            ))
        elif physical_type == PhysicalType.INT64:
            if min:
                var ptr = min.value()[:8].unsafe_ptr()
                var int64_ptr = ptr.bitcast[Int64]()
                int64_min = Optional[Int64](int64_ptr[])
            else:
                int64_min = None
            if max:
                var ptr = max.value()[:8].unsafe_ptr()
                var int64_ptr = ptr.bitcast[Int64]()
                int64_max = Optional[Int64](int64_ptr[])
            else:
                int64_max = None
            res = Optional(int64_statistics(
                int64_min,
                int64_max,
                distinct_count,
                null_count,
                old_format,
            ))
        elif physical_type == PhysicalType.BYTE_ARRAY:
            if min:
                byte_array_min = Optional[ByteArray](ByteArray(min.value()))
            else:
                byte_array_min = None
            if max:
                byte_array_max = Optional[ByteArray](ByteArray(max.value()))
            else:
                byte_array_max = None
            res = Optional(byte_array_statistics(
                byte_array_min,
                byte_array_max,
                distinct_count,
                null_count,
                old_format,
            ))

        return res
    else:
        return None
