from parquet.data_type import ByteArray
from parquet.file.metadata import ParquetMetaDataReader
from parquet.file.page_index.index import TypedIndex, NativeInt32, NativeInt64, NativeByteArray
from parquet.file.statistics import ValueStatistics
from parquet.gen.parquet.ttypes import Type, FieldRepetitionType, ConvertedType, LogicalType
from testing import assert_equal, assert_true, assert_false

fn test_file_version() raises:
    var reader = ParquetMetaDataReader()
    with open("test/data/example_01.parquet", "r") as f:
        reader.parse(f)
        var parquet_metadata = reader.finish()
        assert_equal(parquet_metadata.file_metadata.version, 1)

fn test_num_rows() raises:
    var reader = ParquetMetaDataReader()
    with open("test/data/example_01.parquet", "r") as f:
        reader.parse(f)
        var parquet_metadata = reader.finish()
        assert_equal(parquet_metadata.file_metadata.num_rows, 4)

fn test_num_row_groups() raises:
    var reader = ParquetMetaDataReader()
    with open("test/data/example_01.parquet", "r") as f:
        reader.parse(f)
        var parquet_metadata = reader.finish()
        assert_equal(len(parquet_metadata.row_groups), 1)

fn test_created_by() raises:
    var reader = ParquetMetaDataReader()
    with open("test/data/example_01.parquet", "r") as f:
        reader.parse(f)
        var parquet_metadata = reader.finish()
        if parquet_metadata.file_metadata.created_by:
            var created_by = parquet_metadata.file_metadata.created_by.value()
            assert_equal(created_by, "parquet-rs version 55.1.0")
        else:
            raise Error("created_by is not set")

fn test_schema_name() raises:
    var reader = ParquetMetaDataReader()
    with open("test/data/example_01.parquet", "r") as f:
        reader.parse(f)
        var parquet_metadata = reader.finish()
        var expected = ["schema", "id", "name", "age"]
        for i in range(len(parquet_metadata.file_metadata.schema)):
            var schema_element = parquet_metadata.file_metadata.schema[i]
            assert_equal(schema_element.name, expected[i])

fn test_schema_type() raises:
    var reader = ParquetMetaDataReader()
    with open("test/data/example_01.parquet", "r") as f:
        reader.parse(f)
        var parquet_metadata = reader.finish()
        var expected = [Optional[Type](None), Optional[Type](Type.INT64), Optional[Type](Type.BYTE_ARRAY), Optional[Type](Type.INT32)]
        for i in range(len(parquet_metadata.file_metadata.schema)):
            var schema_element = parquet_metadata.file_metadata.schema[i]
            if expected[i]:
                schema_type = schema_element.type.value()
                assert_equal(schema_type, expected[i].value())
            else:
                assert_false(schema_element.type)

fn test_schema_type_length() raises:
    var reader = ParquetMetaDataReader()
    with open("test/data/example_01.parquet", "r") as f:
        reader.parse(f)
        var parquet_metadata = reader.finish()
        var expected = [Optional[Int32](None), Optional[Int32](None), Optional[Int32](None), Optional[Int32](None)]
        for i in range(len(parquet_metadata.file_metadata.schema)):
            var schema_element = parquet_metadata.file_metadata.schema[i]
            if expected[i]:
                type_length = schema_element.type_length.value()
                assert_equal(type_length, expected[i].value())
            else:
                assert_false(schema_element.type_length)

fn test_schema_repetition_type() raises:
    var reader = ParquetMetaDataReader()
    with open("test/data/example_01.parquet", "r") as f:
        reader.parse(f)
        var parquet_metadata = reader.finish()
        var expected = [
            Optional[FieldRepetitionType](None),
            Optional[FieldRepetitionType](FieldRepetitionType.REQUIRED),
            Optional[FieldRepetitionType](FieldRepetitionType.OPTIONAL),
            Optional[FieldRepetitionType](FieldRepetitionType.OPTIONAL),
        ]
        for i in range(len(parquet_metadata.file_metadata.schema)):
            var schema_element = parquet_metadata.file_metadata.schema[i]
            if expected[i]:
                repetition_type = schema_element.repetition_type.value()
                assert_equal(repetition_type, expected[i].value())
            else:
                assert_false(schema_element.repetition_type)

fn test_schema_converted_type() raises:
    var reader = ParquetMetaDataReader()
    with open("test/data/example_01.parquet", "r") as f:
        reader.parse(f)
        var parquet_metadata = reader.finish()
        var expected = [
            Optional[ConvertedType](None),
            Optional[ConvertedType](None),
            Optional[ConvertedType](ConvertedType.UTF8),
            Optional[ConvertedType](None),
        ]
        for i in range(len(parquet_metadata.file_metadata.schema)):
            var schema_element = parquet_metadata.file_metadata.schema[i]
            if expected[i]:
                converted_type = schema_element.converted_type.value()
                assert_equal(converted_type, expected[i].value())
            else:
                assert_false(schema_element.converted_type)

fn test_schema_logical_type() raises:
    var reader = ParquetMetaDataReader()
    with open("test/data/example_01.parquet", "r") as f:
        reader.parse(f)
        var parquet_metadata = reader.finish()
        var schema_element = parquet_metadata.file_metadata.schema[0]
        assert_false(schema_element.logicalType)
        schema_element = parquet_metadata.file_metadata.schema[1]
        assert_false(schema_element.logicalType)
        schema_element = parquet_metadata.file_metadata.schema[2]
        # converted_type is set but logicalType is not set for BYTE_ARRAY type in this file
        assert_false(schema_element.logicalType)
        schema_element = parquet_metadata.file_metadata.schema[3]
        assert_false(schema_element.logicalType)

fn test_offset_index_offset() raises:
    var reader = ParquetMetaDataReader()
    with open("test/data/example_01.parquet", "r") as f:
        reader.parse(f)
        var parquet_metadata = reader.finish()
        var row_group = parquet_metadata.row_groups[0]
        assert_equal(len(row_group.columns), 3)
        var column_chunk = row_group.columns[0]
        assert_true(column_chunk.offset_index_offset)
        assert_equal(column_chunk.offset_index_offset.value(), 353)
        column_chunk = row_group.columns[1]
        assert_true(column_chunk.offset_index_offset)
        assert_equal(column_chunk.offset_index_offset.value(), 363)
        column_chunk = row_group.columns[2]
        assert_true(column_chunk.offset_index_offset)
        assert_equal(column_chunk.offset_index_offset.value(), 377)

fn test_offset_index_length() raises:
    var reader = ParquetMetaDataReader()
    with open("test/data/example_01.parquet", "r") as f:
        reader.parse(f)
        var parquet_metadata = reader.finish()
        var row_group = parquet_metadata.row_groups[0]
        assert_equal(len(row_group.columns), 3)
        var column_chunk = row_group.columns[0]
        assert_true(column_chunk.offset_index_length)
        assert_equal(column_chunk.offset_index_length.value(), 10)
        column_chunk = row_group.columns[1]
        assert_true(column_chunk.offset_index_length)
        assert_equal(column_chunk.offset_index_length.value(), 14)
        column_chunk = row_group.columns[2]
        assert_true(column_chunk.offset_index_length)
        assert_equal(column_chunk.offset_index_length.value(), 11)

fn test_column_index_offset() raises:
    var reader = ParquetMetaDataReader()
    with open("test/data/example_01.parquet", "r") as f:
        reader.parse(f)
        var parquet_metadata = reader.finish()
        var row_group = parquet_metadata.row_groups[0]
        assert_equal(len(row_group.columns), 3)
        var column_chunk = row_group.columns[0]
        assert_true(column_chunk.column_index_offset)
        assert_equal(column_chunk.column_index_offset.value(), 267)
        column_chunk = row_group.columns[1]
        assert_true(column_chunk.column_index_offset)
        assert_equal(column_chunk.column_index_offset.value(), 298)
        column_chunk = row_group.columns[2]
        assert_true(column_chunk.column_index_offset)
        assert_equal(column_chunk.column_index_offset.value(), 326)

fn test_column_index_length() raises:
    var reader = ParquetMetaDataReader()
    with open("test/data/example_01.parquet", "r") as f:
        reader.parse(f)
        var parquet_metadata = reader.finish()
        var row_group = parquet_metadata.row_groups[0]
        assert_equal(len(row_group.columns), 3)
        var column_chunk = row_group.columns[0]
        assert_true(column_chunk.column_index_length)
        assert_equal(column_chunk.column_index_length.value(), 31)
        column_chunk = row_group.columns[1]
        assert_true(column_chunk.column_index_length)
        assert_equal(column_chunk.column_index_length.value(), 28)
        column_chunk = row_group.columns[2]
        assert_true(column_chunk.column_index_length)
        assert_equal(column_chunk.column_index_length.value(), 27)

fn test_column_index() raises:
    var reader = ParquetMetaDataReader(with_page_index=True)
    with open("test/data/example_01.parquet", "r") as f:
        reader.parse(f)
        var parquet_metadata = reader.finish()
        assert_true(parquet_metadata.column_index)
        var column_index = parquet_metadata.column_index.value()
        assert_equal(len(column_index), 1)
        assert_equal(len(column_index[0]), 3)
        assert_true(column_index[0][0].isa[TypedIndex[NativeInt64]]())
        assert_true(column_index[0][1].isa[TypedIndex[NativeByteArray]]())
        assert_true(column_index[0][2].isa[TypedIndex[NativeInt32]]())
        assert_equal(len(column_index[0][0][TypedIndex[NativeInt64]].indexes), 1)
        assert_equal(len(column_index[0][1][TypedIndex[NativeByteArray]].indexes), 1)
        assert_equal(len(column_index[0][2][TypedIndex[NativeInt32]].indexes), 1)
        assert_true(column_index[0][0][TypedIndex[NativeInt64]].indexes[0].min)
        assert_true(column_index[0][0][TypedIndex[NativeInt64]].indexes[0].max)
        assert_equal(column_index[0][0][TypedIndex[NativeInt64]].indexes[0].min.value(), 1)
        assert_equal(column_index[0][0][TypedIndex[NativeInt64]].indexes[0].max.value(), 4)
        assert_true(column_index[0][1][TypedIndex[NativeByteArray]].indexes[0].min)
        assert_true(column_index[0][1][TypedIndex[NativeByteArray]].indexes[0].max)
        assert_equal(String(bytes=column_index[0][1][TypedIndex[NativeByteArray]].indexes[0].min.value().value), String("Alice"))
        assert_equal(String(bytes=column_index[0][1][TypedIndex[NativeByteArray]].indexes[0].max.value().value), String("Dave"))
        assert_true(column_index[0][2][TypedIndex[NativeInt32]].indexes[0].min)
        assert_true(column_index[0][2][TypedIndex[NativeInt32]].indexes[0].max)
        assert_equal(column_index[0][2][TypedIndex[NativeInt32]].indexes[0].min.value(), 25)
        assert_equal(column_index[0][2][TypedIndex[NativeInt32]].indexes[0].max.value(), 35)

fn test_column_statistics() raises:
    var reader = ParquetMetaDataReader()
    with open("test/data/example_01.parquet", "r") as f:
        reader.parse(f)
        var parquet_metadata = reader.finish()
        var row_group = parquet_metadata.row_groups[0]
        assert_equal(len(row_group.columns), 3)
        var column_chunk = row_group.columns[0]
        assert_true(column_chunk.statistics.isa[ValueStatistics[Int64]]())
        var i64_stats = column_chunk.statistics[ValueStatistics[Int64]]
        assert_true(i64_stats.min)
        assert_equal(i64_stats.min.value(), 1)
        assert_true(i64_stats.max)
        assert_equal(i64_stats.max.value(), 4)
        column_chunk = row_group.columns[1]
        assert_true(column_chunk.statistics.isa[ValueStatistics[ByteArray]]())
        var ba_stats = column_chunk.statistics[ValueStatistics[ByteArray]]
        assert_true(ba_stats.min)
        assert_equal(String(bytes=ba_stats.min.value().value), String("Alice"))
        assert_true(ba_stats.max)
        assert_equal(String(bytes=ba_stats.max.value().value), String("Dave"))
        column_chunk = row_group.columns[2]
        assert_true(column_chunk.statistics.isa[ValueStatistics[Int32]]())
        var i32_stats = column_chunk.statistics[ValueStatistics[Int32]]
        assert_true(i32_stats.min)
        assert_equal(i32_stats.min.value(), 25)
        assert_true(i32_stats.max)
        assert_equal(i32_stats.max.value(), 35)

fn test_offset_index() raises:
    var reader = ParquetMetaDataReader(with_page_index=True)
    with open("test/data/example_01.parquet", "r") as f:
        reader.parse(f)
        var parquet_metadata = reader.finish()
        assert_true(parquet_metadata.offset_index)
        var offset_index = parquet_metadata.offset_index.value()
        assert_equal(len(offset_index), 1)
        assert_equal(len(offset_index[0]), 3)
        assert_equal(len(offset_index[0][0].page_locations), 1)
        assert_equal(offset_index[0][0].page_locations[0].first_row_index, 0)
        assert_equal(len(offset_index[0][1].page_locations), 1)
        assert_equal(offset_index[0][1].page_locations[0].first_row_index, 0)
        assert_equal(len(offset_index[0][2].page_locations), 1)
        assert_equal(offset_index[0][2].page_locations[0].first_row_index, 0)
