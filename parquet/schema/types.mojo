from utils import Variant
from parquet.gen.parquet.ttypes import SchemaElement
from parquet.types import ConvertedType, LogicalType, PhysicalType, Repetition, logical_type_from_thrift

@value
struct BasicTypeInfo:
    var name: String
    var repetition: Optional[Repetition]
    var converted_type: Optional[ConvertedType]
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

@value
struct ColumnPath:
    var parts: List[String]

    fn __init__(out self, owned parts: List[String]):
        self.parts = parts

@value
struct ColumnDescriptor:
    var primitive_type: Type
    var max_def_level: Int16
    var max_rep_level: Int16
    var path: ColumnPath

    fn __init__(out self, owned primitive_type: Type, max_def_level: Int16, max_rep_level: Int16, owned path: ColumnPath):
        self.primitive_type = primitive_type
        self.max_def_level = max_def_level
        self.max_rep_level = max_rep_level
        self.path = path

    fn physical_type(self) raises -> PhysicalType:
        if self.primitive_type.isa[PrimitiveType]():
            return self.primitive_type[PrimitiveType].physical_type
        else:
            raise Error("ColumnDescriptor must be initialized with a PrimitiveType")

fn build_tree(
    tp: Type,
    root_idx: Int,
    max_rep_level: Int16,
    max_def_level: Int16,
    mut leaves: List[ColumnDescriptor],
    mut leaf_to_base: List[Int],
    mut path_so_far: List[String],
):
    var max_rep_level_ = max_rep_level
    var max_def_level_ = max_def_level
    if tp.isa[PrimitiveType]():
        var primitive_tp = tp[PrimitiveType]
        path_so_far.append(primitive_tp.basic_info.name)
        if primitive_tp.basic_info.repetition:
            var rep = primitive_tp.basic_info.repetition.value()
            if rep == Repetition.OPTIONAL:
                max_def_level_ += 1
            elif rep == Repetition.REPEATED:
                max_def_level_ += 1
                max_rep_level_ += 1
        leaves.append(ColumnDescriptor(
            primitive_type=tp,
            max_def_level=max_def_level_,
            max_rep_level=max_rep_level_,
            path=ColumnPath(path_so_far),
        ))
        leaf_to_base.append(root_idx)
    else:
        var group_tp = tp[GroupType]
        path_so_far.append(group_tp.basic_info.name)
        path_so_far.append(group_tp.basic_info.name)
        if group_tp.basic_info.repetition:
            var rep = group_tp.basic_info.repetition.value()
            if rep == Repetition.OPTIONAL:
                max_def_level_ += 1
            elif rep == Repetition.REPEATED:
                max_def_level_ += 1
                max_rep_level_ += 1
        var fields = group_tp.fields
        for idx in range(len(fields)):
            build_tree(
                fields[idx],
                root_idx,
                max_rep_level_,
                max_def_level_,
                leaves,
                leaf_to_base,
                path_so_far,
            )
            _ = path_so_far.pop()

fn from_thrift(elements: List[SchemaElement]) raises -> Type:
    var index = 0
    var schema_nodes = List[Type]()
    while index < len(elements):
        var t = from_thrift_helper(elements, index)
        index = t[0]
        schema_nodes.append(t[1])
    if len(schema_nodes) != 1:
        raise Error("Expected exactly one root node, but found ", len(schema_nodes))

    if not schema_nodes[0].isa[GroupType]():
        raise Error("Expected root node to be a group type")
    return schema_nodes[0]

fn from_thrift_helper(
    elements: List[SchemaElement],
    index: Int,
) raises -> Tuple[Int, Type]:
    var is_root_node = index == 0

    if index >= len(elements):
        raise Error("Index out of bound, index = ", index, ", len = ", len(elements))

    if is_root_node and (
        not elements[index].num_children or
        elements[index].num_children.value() == 0):
        var empty_type = GroupType(
            basic_info=BasicTypeInfo(
                name=elements[index].name,
                repetition=None,
                converted_type=None,
                logical_type=None,
                id= None,
            ),
            fields=[],
        )
        return Tuple(index + 1, Type(empty_type))

    var converted_type = ConvertedType.from_thrift(elements[index])

    var logical_type = logical_type_from_thrift(elements[index])

    var filed_id = elements[index].field_id

    if not elements[index].num_children or elements[index].num_children.value() == 0:
        if not elements[index].repetition_type:
            raise Error("Repetition level must be defined for a primitive type")
        var repetition = Repetition.from_thrift(elements[index].repetition_type.value())
        if elements[index].type:
            var physical_type = PhysicalType.from_thrift(elements[index].type.value())
            var type_length = elements[index].type_length.or_else(-1)
            var scale = elements[index].scale.or_else(-1)
            var precision = elements[index].precision.or_else(-1)
            var name = elements[index].name
            var primitive_type = PrimitiveType(
                basic_info=BasicTypeInfo(
                    name=name,
                    repetition=repetition,
                    converted_type=converted_type,
                    logical_type=logical_type,
                    id=filed_id,
                ),
                physical_type=physical_type,
                type_length=type_length,
                scale=scale,
                precision=precision,
            )
            return Tuple(index + 1, Type(primitive_type))
        else:
            var group_type = GroupType(
                basic_info=BasicTypeInfo(
                    name=elements[index].name,
                    repetition=None if is_root_node else Optional(repetition),
                    converted_type=converted_type,
                    logical_type=logical_type,
                    id=filed_id,
                ),
                fields=[],
            )
            return Tuple(index + 1, Type(group_type))
    else:
        if elements[index].repetition_type:
            repetition = Optional(Repetition.from_thrift(elements[index].repetition_type.value()))
        else:
            repetition = None
        var fields = List[Type]()
        var next_index = index + 1
        for _ in range(elements[index].num_children.value()):
            var child_result = from_thrift_helper(elements, next_index)
            next_index = child_result[0]
            fields.append(child_result[1])

        var group_type = GroupType(
            basic_info=BasicTypeInfo(
                name=elements[index].name,
                repetition=repetition,
                converted_type=converted_type,
                logical_type=logical_type,
                id=filed_id,
            ),
            fields=fields,
        )
        return Tuple(next_index, Type(group_type))

@value
struct SchemaDescriptor:
    var schema: Type
    var leaves: List[ColumnDescriptor]
    var leaf_to_base: List[Int]

    fn __init__(out self, tp: Type) raises:
        if not tp.isa[GroupType]():
            raise Error("SchemaDescriptor must be initialized with a GroupType")

        var leaves = List[ColumnDescriptor]()
        var leaf_to_base = List[Int]()
        var fields = tp[GroupType].fields
        for idx in range(len(fields)):
            var path = List[String]()
            build_tree(fields[idx], idx, 0, 0, leaves, leaf_to_base, path)

        self.schema = tp
        self.leaves = leaves
        self.leaf_to_base = leaf_to_base

    fn columns(self) -> List[ColumnDescriptor]:
        return self.leaves

    fn num_columns(self) -> Int:
        return len(self.leaves)
