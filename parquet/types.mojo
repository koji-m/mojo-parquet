from utils import Variant
from parquet.gen.parquet.ttypes import EdgeInterpolationAlgorithm, TimeUnit

@value
struct PhysicalType:
    var value: Int

    alias BOOLEAN = PhysicalType(0)
    alias INT32 = PhysicalType(1)
    alias INT64 = PhysicalType(2)
    alias INT96 = PhysicalType(3)
    alias FLOAT = PhysicalType(4)
    alias DOUBLE = PhysicalType(5)
    alias BYTE_ARRAY = PhysicalType(6)
    alias FIXED_LEN_BYTE_ARRAY = PhysicalType(7)

@value
struct StringType:
    pass

@value
struct MapType:
    pass

@value
struct ListType:
    pass

@value
struct EnumType:
    pass

@value
struct DecimalType:
    var scale: Int32
    var precision: Int32

@value
struct DateType:
    pass

@value
struct TimeType:
    var is_adjusted_to_utc: Bool
    var unit: TimeUnit

@value
struct TimestampType:
    var is_adjusted_to_utc: Bool
    var unit: TimeUnit

@value
struct IntType:
    var bit_width: Int8
    var is_signed: Bool

@value
struct NullType:
    pass

@value
struct JsonType:
    pass

@value
struct BsonType:
    pass

@value
struct UUIDType:
    pass

@value
struct Float16Type:
    pass

@value
struct VariantType:
    var specification_version: Optional[Int8]

@value
struct GeometryType:
    var crs: Optional[String]

@value
struct GeographyType:
    var crs: Optional[String]
    var algorithm: Optional[EdgeInterpolationAlgorithm]

alias LogicalType = Variant[
    StringType,
    MapType,
    ListType,
    EnumType,
    DecimalType,
    DateType,
    TimeType,
    TimestampType,
    IntType,
    NullType,
    JsonType,
    BsonType,
    UUIDType,
    Float16Type,
    VariantType,
    GeometryType,
    GeographyType
]

@value
struct ConvertedType:
    var value: Int

    alias UTF8 = ConvertedType(0)
    alias MAP = ConvertedType(1)
    alias MAP_KEY_VALUE = ConvertedType(2)
    alias LIST = ConvertedType(3)
    alias ENUM = ConvertedType(4)
    alias DECIMAL = ConvertedType(5)
    alias DATE = ConvertedType(6)
    alias TIME_MILLIS = ConvertedType(7)
    alias TIME_MICROS = ConvertedType(8)
    alias TIMESTAMP_MILLIS = ConvertedType(9)
    alias TIMESTAMP_MICROS = ConvertedType(10)
    alias UINT8 = ConvertedType(11)
    alias UINT16 = ConvertedType(12)
    alias UINT32 = ConvertedType(13)
    alias UINT64 = ConvertedType(14)
    alias INT8 = ConvertedType(15)
    alias INT16 = ConvertedType(16)
    alias INT32 = ConvertedType(17)
    alias INT64 = ConvertedType(18)
    alias JSON = ConvertedType(19)
    alias BSON = ConvertedType(20)
    alias INTERVAL = ConvertedType(21)

@value
struct Repetition:
    var value: Int

    alias REQUIRED = Repetition(0)
    alias OPTIONAL = Repetition(1)
    alias REPEATED = Repetition(2)
