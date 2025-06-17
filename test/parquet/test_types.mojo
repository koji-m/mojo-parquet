from testing import assert_equal, assert_true, assert_false
from parquet.schema.types import (
    BasicTypeInfo,
    PrimitiveType,
    GroupType,
    Type,
)
from parquet.types import (
    ConvertedType,
    LogicalType,
    PhysicalType,
    Repetition,
    StringType,
)

fn test_basic_type_info() raises:
    var basic_info = BasicTypeInfo(
        name="test",
        repetition=Optional[Repetition](Repetition.REQUIRED),
        converted_type=ConvertedType.UTF8,
        logical_type=Optional[LogicalType](LogicalType(StringType())),
        id=Optional[Int32](1),
    )
    assert_equal(basic_info.name, "test")

fn test_schema_type() raises:
    var primitive_type = PrimitiveType(
        basic_info=BasicTypeInfo(
            name="test_primitive",
            repetition=Optional[Repetition](Repetition.OPTIONAL),
            converted_type=ConvertedType.UTF8,
            logical_type=Optional[LogicalType](None),
            id=Optional[Int32](2),
        ),
        physical_type=PhysicalType.INT32,
        type_length=4,
        scale=0,
        precision=0,
    )
    assert_equal(primitive_type.basic_info.name, "test_primitive")

    var group_type_child = GroupType(
        basic_info=BasicTypeInfo(
            name="test_group_child",
            repetition=Optional[Repetition](Repetition.REPEATED),
            converted_type=ConvertedType.MAP,
            logical_type=Optional[LogicalType](None),
            id=Optional[Int32](3),
        ),
        fields=[],
    )
    var group_type_parent = GroupType(
        basic_info=BasicTypeInfo(
            name="test_group_parent",
            repetition=Optional[Repetition](Repetition.REPEATED),
            converted_type=ConvertedType.MAP,
            logical_type=Optional[LogicalType](None),
            id=Optional[Int32](3),
        ),
        fields=[Type(primitive_type), Type(group_type_child)],
    )
    
    assert_equal(group_type_parent.basic_info.name, "test_group_parent")

    var type_variant = Type(primitive_type)
    assert_true(type_variant.isa[PrimitiveType]())
