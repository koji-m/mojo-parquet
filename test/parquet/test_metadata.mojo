from parquet.file.metadata import ParquetMetaDataReader
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
        assert_equal(len(parquet_metadata.file_metadata.row_groups), 1)

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
