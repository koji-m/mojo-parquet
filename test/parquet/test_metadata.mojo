from parquet.file.metadata import ParquetMetaDataReader
from testing import assert_equal

fn test_file_version() raises:
    var reader = ParquetMetaDataReader()
    with open("test/data/example_01.parquet", "r") as f:
        var file_meta_data = reader.parse_metadata(f)
        assert_equal(file_meta_data.version, 1)

fn test_schema() raises:
    var reader = ParquetMetaDataReader()
    with open("test/data/example_01.parquet", "r") as f:
        var file_meta_data = reader.parse_metadata(f)
        assert_equal(file_meta_data.num_rows, 4)
