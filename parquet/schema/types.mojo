from utils import Variant
from parquet.types import ConvertedType, LogicalType, PhysicalType, Repetition

@value
struct BasicTypeInfo:
    var name: String
    var repetition: Optional[Repetition]
    var converted_type: ConvertedType
    var logical_type: Optional[LogicalType]
    var id: Optional[Int32]

@value
struct PrimitiveType:
    var basic_info: BasicTypeInfo
    var physical_type: PhysicalType
    var type_length: Int32
    var scale: Int32
    var precision: Int32

@value
struct GroupType:
    var basic_info: BasicTypeInfo
    var fields: List[Variant[PrimitiveType, GroupType]]

alias Type = Variant[
    PrimitiveType,
    GroupType,
]
