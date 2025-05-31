from parquet.file.metadata import ParquetMetaDataReader
from testing import assert_equal

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
