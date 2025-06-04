from parquet.file.metadata import ParquetMetaData, ParquetMetaDataReader

struct FileReader:
    var chunk_reader: FileHandle
    var metadata: ParquetMetaData

    fn __init__(out self, owned chunk_reader: FileHandle) raises:
        var reader = ParquetMetaDataReader()
        reader.parse(chunk_reader)
        self.metadata = reader.finish()
        self.chunk_reader = chunk_reader^
