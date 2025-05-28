import os

from parquet.gen.parquet.ttypes import FileMetaData
from thrift.protocol.compact import TCompactProtocol
from thrift.transport import TMemoryBuffer
from parquet.file.constants import FOOTER_SIZE, PARQUET_MAGIC

@value
struct ParquetMetaData:
    pass

@value
struct FooterTail:
    var metadata_length: Int

struct ParquetMetaDataReader:
    var metadata: Optional[ParquetMetaData]
    var column_index: Bool
    var offset_index: Bool
    var prefetch_hint: Optional[Int]
    var metadata_size: Optional[Int]

    fn __init__(out self):
        self.metadata = Optional[ParquetMetaData](None)
        self.column_index = False
        self.offset_index = False
        self.prefetch_hint = Optional[Int](None)
        self.metadata_size = Optional[Int](None)

    @staticmethod
    fn decode_footertail(buf: List[UInt8]) raises -> FooterTail:
        var magic = buf[4:]
        for i in range(len(PARQUET_MAGIC)):
            if magic[i] != PARQUET_MAGIC[i]:
                raise Error("Invalid Parquet file magic")
        var metadata_len = buf[:4].unsafe_ptr().bitcast[Int32]()[]
        return FooterTail(Int(metadata_len))

    @staticmethod
    # fn decode_metadata(buf: List[UInt8]) raises -> ParquetMetaData:
    fn decode_metadata(buf: List[UInt8]) raises -> FileMetaData:
        var transport = TMemoryBuffer(buf, 0)
        var protocol = TCompactProtocol(
            transport,
            Optional[Bool](None),
            0,
            List[Int16](),
            0,
            List[Int16](),
            Optional[Int16](None),
        )
        var file_meta_data = FileMetaData.read(protocol)

        return file_meta_data
        # print metadata for test
        # print("file_meta_data.version: ", file_meta_data.version)
        # print("file_meta_data.num_rows: ", file_meta_data.num_rows)
        # return ParquetMetaData()

    fn parse_metadata(mut self, chunk_reader: FileHandle) raises -> FileMetaData:
        # ToDo check file size
        _ = chunk_reader.seek(-FOOTER_SIZE, os.SEEK_END)
        var footer_bytes = chunk_reader.read_bytes(FOOTER_SIZE)
        var footer = self.decode_footertail(footer_bytes)
        var metadata_len = footer.metadata_length
        var footer_metadata_len = FOOTER_SIZE + metadata_len
        self.metadata_size = Optional[Int](footer_metadata_len)
        # ToDo check file size
        _ = chunk_reader.seek(-footer_metadata_len, os.SEEK_END)

        return self.decode_metadata(chunk_reader.read_bytes(metadata_len))