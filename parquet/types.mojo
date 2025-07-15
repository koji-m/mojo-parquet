from utils import Variant
import parquet.gen.parquet.ttypes as tt

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

    fn __eq__(self, other: PhysicalType) -> Bool:
        return self.value == other.value

    @staticmethod
    fn from_thrift(t_type: tt.Type) raises -> PhysicalType:
        if t_type == tt.Type.BOOLEAN:
            return PhysicalType.BOOLEAN
        elif t_type == tt.Type.INT32:
            return PhysicalType.INT32
        elif t_type == tt.Type.INT64:
            return PhysicalType.INT64
        elif t_type == tt.Type.INT96:
            return PhysicalType.INT96
        elif t_type == tt.Type.FLOAT:
            return PhysicalType.FLOAT
        elif t_type == tt.Type.DOUBLE:
            return PhysicalType.DOUBLE
        elif t_type == tt.Type.BYTE_ARRAY:
            return PhysicalType.BYTE_ARRAY
        elif t_type == tt.Type.FIXED_LEN_BYTE_ARRAY:
            return PhysicalType.FIXED_LEN_BYTE_ARRAY
        else:
            raise Error("Invalid physical type: ", t_type)

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
    var unit: tt.TimeUnit

@value
struct TimestampType:
    var is_adjusted_to_utc: Bool
    var unit: tt.TimeUnit

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
    var algorithm: Optional[tt.EdgeInterpolationAlgorithm]

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

fn logical_type_from_thrift(
    t_schema_element: tt.SchemaElement
) raises -> Optional[LogicalType]:
    if not t_schema_element.logicalType:
        return Optional[LogicalType](None)

    var t_logical_type = t_schema_element.logicalType.value()
    if t_logical_type.STRING:
        return Optional(LogicalType(StringType()))
    elif t_logical_type.MAP:
        return Optional(LogicalType(MapType()))
    elif t_logical_type.LIST:
        return Optional(LogicalType(ListType()))
    elif t_logical_type.ENUM:
        return Optional(LogicalType(EnumType()))
    elif t_logical_type.DECIMAL:
        var decimal = t_logical_type.DECIMAL.value()
        return Optional(LogicalType(DecimalType(decimal.scale, decimal.precision)))
    elif t_logical_type.DATE:
        return Optional(LogicalType(DateType()))
    elif t_logical_type.TIME:
        var time = t_logical_type.TIME.value()
        return Optional(LogicalType(TimeType(time.isAdjustedToUTC, time.unit)))
    elif t_logical_type.TIMESTAMP:
        var timestamp = t_logical_type.TIMESTAMP.value()
        return Optional(LogicalType(TimestampType(timestamp.isAdjustedToUTC, timestamp.unit)))
    elif t_logical_type.INTEGER:
        var int_ = t_logical_type.INTEGER.value()
        if int_.bitWidth != 8 and int_.bitWidth != 16 and int_.bitWidth != 32 and int_.bitWidth != 64:
            raise Error("Invalid bit width for INTEGER logical type: ", int_.bitWidth)
        return Optional(LogicalType(IntType(int_.bitWidth, int_.isSigned)))
    elif t_logical_type.UNKNOWN:
        return Optional(LogicalType(NullType()))
    elif t_logical_type.JSON:
        return Optional(LogicalType(JsonType()))
    elif t_logical_type.BSON:
        return Optional(LogicalType(BsonType()))
    elif t_logical_type.UUID:
        return Optional(LogicalType(UUIDType()))
    elif t_logical_type.FLOAT16:
        return Optional(LogicalType(Float16Type()))
    elif t_logical_type.VARIANT:
        var variant = t_logical_type.VARIANT.value()
        return Optional(LogicalType(VariantType(variant.specification_version)))
    elif t_logical_type.GEOMETRY:
        var geometry = t_logical_type.GEOMETRY.value()
        return Optional(LogicalType(GeometryType(geometry.crs)))
    elif t_logical_type.GEOGRAPHY:
        var geography = t_logical_type.GEOGRAPHY.value()
        return Optional(LogicalType(GeographyType(geography.crs, geography.algorithm)))
    else:
        return Optional[LogicalType](None)

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

    @staticmethod
    fn from_thrift(t_schema_element: tt.SchemaElement) -> Optional[Self]:
        if not t_schema_element.converted_type:
            return Optional[Self](None)
        return Optional(Self(Int(t_schema_element.converted_type.value().value)))


@value
struct Repetition:
    var value: Int

    alias REQUIRED = Repetition(0)
    alias OPTIONAL = Repetition(1)
    alias REPEATED = Repetition(2)

    fn __eq__(self, other: Repetition) -> Bool:
        return self.value == other.value

    @staticmethod
    fn from_thrift(t_repetition_type: tt.FieldRepetitionType) raises -> Repetition:
        if t_repetition_type == tt.FieldRepetitionType.REQUIRED:
            return Repetition.REQUIRED
        elif t_repetition_type == tt.FieldRepetitionType.OPTIONAL:
            return Repetition.OPTIONAL
        elif t_repetition_type == tt.FieldRepetitionType.REPEATED:
            return Repetition.REPEATED
        else:
            raise Error("Invalid repetition type: ", t_repetition_type)

@value
struct Encoding:
    var value: Int

    alias PLAIN = Encoding(0)
    alias PLAIN_DICTIONARY = Encoding(2)
    alias RLE = Encoding(3)
    alias BIT_PACKED = Encoding(4)
    alias DELTA_BINARY_PACKED = Encoding(5)
    alias DELTA_LENGTH_BYTE_ARRAY = Encoding(6)
    alias DELTA_BYTE_ARRAY = Encoding(7)
    alias RLE_DICTIONARY = Encoding(8)
    alias BYTE_STREAM_SPLIT = Encoding(9)

    fn __eq__(self, other: Encoding) -> Bool:
        return self.value == other.value

    @staticmethod
    fn from_thrift(t_encoding: tt.Encoding) raises -> Encoding:
        if t_encoding == tt.Encoding.PLAIN:
            return Encoding.PLAIN
        elif t_encoding == tt.Encoding.PLAIN_DICTIONARY:
            return Encoding.PLAIN_DICTIONARY
        elif t_encoding == tt.Encoding.RLE:
            return Encoding.RLE
        elif t_encoding == tt.Encoding.BIT_PACKED:
            return Encoding.BIT_PACKED
        elif t_encoding == tt.Encoding.DELTA_BINARY_PACKED:
            return Encoding.DELTA_BINARY_PACKED
        elif t_encoding == tt.Encoding.DELTA_LENGTH_BYTE_ARRAY:
            return Encoding.DELTA_LENGTH_BYTE_ARRAY
        elif t_encoding == tt.Encoding.DELTA_BYTE_ARRAY:
            return Encoding.DELTA_BYTE_ARRAY
        elif t_encoding == tt.Encoding.RLE_DICTIONARY:
            return Encoding.RLE_DICTIONARY
        elif t_encoding == tt.Encoding.BYTE_STREAM_SPLIT:
            return Encoding.BYTE_STREAM_SPLIT
        else:
            raise Error("Invalid encoding type: ", t_encoding)

@value
struct CompressionCodec:
    var value: Int

    alias UNCOMPRESSED = CompressionCodec(0)
    alias SNAPPY = CompressionCodec(1)
    alias GZIP = CompressionCodec(2)
    alias LZO = CompressionCodec(3)
    alias BROTLI = CompressionCodec(4)
    alias LZ4 = CompressionCodec(5)
    alias ZSTD = CompressionCodec(6)
    alias LZ4_RAW = CompressionCodec(7)

    @staticmethod
    fn from_thrift(t_codec: tt.CompressionCodec) raises -> CompressionCodec:
        if t_codec == tt.CompressionCodec.UNCOMPRESSED:
            return CompressionCodec.UNCOMPRESSED
        elif t_codec == tt.CompressionCodec.SNAPPY:
            return CompressionCodec.SNAPPY
        elif t_codec == tt.CompressionCodec.GZIP:
            return CompressionCodec.GZIP
        elif t_codec == tt.CompressionCodec.LZO:
            return CompressionCodec.LZO
        elif t_codec == tt.CompressionCodec.BROTLI:
            return CompressionCodec.BROTLI
        elif t_codec == tt.CompressionCodec.LZ4:
            return CompressionCodec.LZ4
        elif t_codec == tt.CompressionCodec.ZSTD:
            return CompressionCodec.ZSTD
        elif t_codec == tt.CompressionCodec.LZ4_RAW:
            return CompressionCodec.LZ4_RAW
        else:
            raise Error("Invalid compression codec: ", t_codec)

@value
struct PageType:
    var value: Int

    alias DATA_PAGE = PageType(0)
    alias INDEX_PAGE = PageType(1)
    alias DICTIONARY_PAGE = PageType(2)
    alias DATA_PAGE_V2 = PageType(3)

    fn __eq__(self, other: PageType) -> Bool:
        return self.value == other.value

    @staticmethod
    fn from_thrift(t_page_type: tt.PageType) raises -> PageType:
        if t_page_type == tt.PageType.DATA_PAGE:
            return PageType.DATA_PAGE
        elif t_page_type == tt.PageType.INDEX_PAGE:
            return PageType.INDEX_PAGE
        elif t_page_type == tt.PageType.DICTIONARY_PAGE:
            return PageType.DICTIONARY_PAGE
        elif t_page_type == tt.PageType.DATA_PAGE_V2:
            return PageType.DATA_PAGE_V2
        else:
            raise Error("Invalid page type: ", t_page_type)
