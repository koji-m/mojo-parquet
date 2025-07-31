from parquet.file.reader import FileReader
from testing import assert_equal

fn test_num_row_groups() raises:
    var file_handle = FileHandle("test/data/example_01.parquet", "r")
    var reader = FileReader(file_handle^)
    assert_equal(reader.num_row_groups(), 1)
