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
