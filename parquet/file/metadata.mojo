import os

import parquet.gen.parquet.ttypes as tt
from thrift.protocol.compact import TCompactProtocol
from thrift.transport import TMemoryBuffer
from parquet.file.constants import FOOTER_SIZE, PARQUET_MAGIC
from parquet.file.page_index.offset_index import OffsetIndexMetaData

@value
struct FileMetaData:
    var version: Int32
    var num_rows: Int64
    var created_by: Optional[String]
    var schema: List[tt.SchemaElement]
    var key_value_metadata: Optional[List[tt.KeyValue]]
    var column_orders: Optional[List[tt.ColumnOrder]]

@value
struct ParquetMetaData:
    var file_metadata: FileMetaData
    var row_groups: List[tt.RowGroup]
    var offset_index: Optional[List[List[OffsetIndexMetaData]]]

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
    fn decode_metadata(buf: List[UInt8]) raises -> ParquetMetaData:
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
        var tfile_meta_data = tt.FileMetaData.read(protocol)
        var file_meta_data = FileMetaData(
            version=tfile_meta_data.version,
            num_rows=tfile_meta_data.num_rows,
            created_by=tfile_meta_data.created_by,
            schema=tfile_meta_data.schema,
            key_value_metadata=tfile_meta_data.key_value_metadata,
            column_orders=tfile_meta_data.column_orders,
        )

        return ParquetMetaData(
            file_meta_data,
            tfile_meta_data.row_groups,
            Optional[List[List[OffsetIndexMetaData]]](None),
        )

    fn parse_metadata(mut self, chunk_reader: FileHandle) raises -> ParquetMetaData:
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

    fn parse(mut self, chunk_reader: FileHandle) raises:
        var parquet_metadata = self.parse_metadata(chunk_reader)
        self.metadata = Optional[ParquetMetaData](parquet_metadata)

    fn finish(mut self) raises -> ParquetMetaData:
        if self.metadata:
            return self.metadata.take()
        else:
            raise Error("metadata could not parsed")