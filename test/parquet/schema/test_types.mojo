from testing import assert_equal, assert_true, assert_false
from parquet.schema.types import (
    BasicTypeInfo,
    ColumnDescriptor,
    ColumnPath,
    PrimitiveType,
    GroupType,
    SchemaDescriptor,
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

fn test_schema_descriptor() raises:
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
    var schema_desc = SchemaDescriptor(group_type_parent)
    var leaf_to_base = schema_desc.leaf_to_base
    var expected = [0]
    for i in range(len(leaf_to_base)):
        assert_equal(leaf_to_base[i], expected[i])

    var leaves = schema_desc.leaves
    var expected_col_descs = [ColumnDescriptor(
        primitive_type=Type(primitive_type),
        max_def_level=1,
        max_rep_level=0,
        path=ColumnPath(["test_primitive"]),
    )]
    for i in range(len(leaves)):
        if leaves[i].primitive_type.isa[PrimitiveType]():
            assert_equal(leaves[i].max_def_level, expected_col_descs[i].max_def_level)
            assert_equal(leaves[i].max_rep_level, expected_col_descs[i].max_rep_level)
            var path = leaves[i].path.parts
            for j in range(len(path)):
                assert_equal(path[j], expected_col_descs[i].path.parts[j])
        else:
            pass
