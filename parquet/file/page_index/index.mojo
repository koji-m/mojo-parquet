from utils import Variant
import parquet.gen.parquet.ttypes as tt
from parquet.gen.parquet.ttypes import BoundaryOrder
from parquet.data_type import ByteArray

@value
struct PageIndex[T: Copyable & Movable]:
    var min: Optional[T]
    var max: Optional[T]
    var null_count: Optional[Int64]

trait FromBytes:
    alias native_type: Copyable & Movable

    @staticmethod
    fn try_from_le_list(b: List[UInt8]) raises -> Self.native_type:
        pass

trait NativeWrapper(FromBytes):
    alias native_type: Copyable & Movable

@value
struct NativeBool(NativeWrapper):
    alias native_type = Bool
    var val: Self.native_type

    @staticmethod
    fn try_from_le_list(b: List[UInt8]) raises -> Self.native_type:
        if len(b) != 1:
            raise Error("Invalid byte length for NativeBool")
        if b[0] == 0:
            return False
        elif b[0] == 1:
            return True
        else:
            raise Error("Invalid byte value for NativeBool, expected 0 or 1")

@value
struct NativeInt32(NativeWrapper):
    alias native_type = Int32
    var val: Self.native_type

    @staticmethod
    fn try_from_le_list(b: List[UInt8]) raises -> Self.native_type:
        if len(b) != 4:
            raise Error("Invalid byte length for NativeInt32")
        return b.unsafe_ptr().bitcast[Int32]()[]

@value
struct NativeInt64(NativeWrapper):
    alias native_type = Int64
    var val: Self.native_type

    @staticmethod
    fn try_from_le_list(b: List[UInt8]) raises -> Self.native_type:
        if len(b) != 8:
            raise Error("Invalid byte length for NativeInt64")
        return b.unsafe_ptr().bitcast[Int64]()[]

@value
struct NativeByteArray(NativeWrapper):
    alias native_type = ByteArray
    var val: Self.native_type

    @staticmethod
    fn try_from_le_list(b: List[UInt8]) raises -> Self.native_type:
        return Self.native_type(b)


@value
struct TypedIndex[T: NativeWrapper & Copyable & Movable]:
    var indexes: List[PageIndex[T.native_type]]
    var boundary_order: BoundaryOrder

    fn __init__(out self, t_column_index: tt.ColumnIndex) raises:
        var num_pages = len(t_column_index.min_values)
        if num_pages != len(t_column_index.max_values):
            raise Error("Mismatch in number of min and max values in column index")

        var null_counts = List[Int64]()
        var set_null_counts = False
        if t_column_index.null_counts:
            set_null_counts = True
            null_counts = t_column_index.null_counts.value()
        var indexes = List[PageIndex[T.native_type]](capacity=num_pages)
        for i in range(num_pages):
            if set_null_counts:
                null_count = Optional[Int64](null_counts[i])
            else:
                null_count = Optional[Int64](None)
            var page_index = PageIndex(
                min=T.try_from_le_list(t_column_index.min_values[i]),
                max=T.try_from_le_list(t_column_index.max_values[i]),
                null_count=null_count,
            )
            indexes.append(page_index)

        self.indexes = indexes
        self.boundary_order = t_column_index.boundary_order

alias Index = Variant[
    NoneType,
    TypedIndex[NativeBool],
    TypedIndex[NativeInt32],
    TypedIndex[NativeInt64],
    TypedIndex[NativeByteArray],
]
