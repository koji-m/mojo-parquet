from parquet.file.metadata import ParquetMetaDataReader
from parquet.gen.parquet.ttypes import Type
from testing import assert_equal, assert_false

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