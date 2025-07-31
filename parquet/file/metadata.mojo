import os

import parquet.gen.parquet.ttypes as tt
from thrift.protocol.compact import TCompactProtocol
from thrift.transport import TMemoryBuffer
from parquet.file.constants import FOOTER_SIZE, PARQUET_MAGIC
from parquet.file.page_index.offset_index import OffsetIndexMetaData
from parquet.file.page_index.index import Index
from parquet.file.page_index.index_reader import decode_column_index, decode_offset_index, accumulate_range
import parquet.file.statistics as statistics
import parquet.file.page_encoding_stats as page_encoding_stats
import parquet.schema.types as types
from parquet.types import PhysicalType, Encoding, CompressionCodec
from parquet.utils import Range

alias ParquetColumnIndex = List[List[Index]]
alias ParquetOffsetIndex = List[List[OffsetIndexMetaData]]

@value
struct FileMetaData:
    var version: Int32
    var num_rows: Int64
    var created_by: Optional[String]
    var schema: List[tt.SchemaElement]
    var key_value_metadata: Optional[List[tt.KeyValue]]
    var column_orders: Optional[List[tt.ColumnOrder]]

@value
struct LevelHistogram:
    var values: List[Int64]

@value
struct ColumnChunkMetaData:
    var offset_index_offset: Optional[Int64]
    var offset_index_length: Optional[Int32]
    var column_index_offset: Optional[Int64]
    var column_index_length: Optional[Int32]
    var column_descr: types.ColumnDescriptor
    var encodings: List[Encoding]
    var file_path: Optional[String]
    var file_offset: Optional[Int64]
    var num_values: Int64
    var compression_codec: CompressionCodec
    var total_compressed_size: Int64
    var total_uncompressed_size: Int64
    var data_page_offset: Int64
    var index_page_offset: Optional[Int64]
    var dictionary_page_offset: Optional[Int64]
    var statistics: statistics.Statistics
    var encoding_stats: Optional[List[page_encoding_stats.PageEncodingStats]]
    var bloom_filter_offset: Optional[Int64]
    var bloom_filter_length: Optional[Int32]
    var unencoded_byte_array_data_bytes: Optional[Int64]
    var repetition_level_histogram: Optional[LevelHistogram]
    var definition_level_histogram: Optional[LevelHistogram]

    fn column_type(self) raises -> PhysicalType:
        return self.column_descr.physical_type()

    @staticmethod
    fn from_thrift(column_descr: types.ColumnDescriptor, t_column_chunk: tt.ColumnChunk) raises -> Self:
        if not t_column_chunk.meta_data:
            raise Error("Expected to have column metadata")
        var t_col_metadata = t_column_chunk.meta_data.value()
        var column_type = PhysicalType.from_thrift(t_col_metadata.type)
        var encodings = List[Encoding]()
        for t_encoding in t_col_metadata.encodings:
            encodings.append(Encoding.from_thrift(t_encoding[]))
        var compression_codec = CompressionCodec.from_thrift(t_col_metadata.codec)
        var file_path = t_column_chunk.file_path
        var file_offset = t_column_chunk.file_offset
        var num_values = t_col_metadata.num_values
        var total_compressed_size = t_col_metadata.total_compressed_size
        var total_uncompressed_size = t_col_metadata.total_uncompressed_size
        var data_page_offset = t_col_metadata.data_page_offset
        var index_page_offset = t_col_metadata.index_page_offset
        var dictionary_page_offset = t_col_metadata.dictionary_page_offset
        var statistics = statistics.from_thrift(column_type, t_col_metadata.statistics)
        if not statistics:
            raise Error("Failed to parse statistics")
        var encoding_stats = Optional[List[page_encoding_stats.PageEncodingStats]](None)
        if t_col_metadata.encoding_stats:
            var encoding_stats_ = List[page_encoding_stats.PageEncodingStats]()
            for i in range(len(t_col_metadata.encoding_stats.value())):
                var t_encoding_stats = t_col_metadata.encoding_stats.value()[i]
                encoding_stats_.append(page_encoding_stats.from_thrift(t_encoding_stats))
            encoding_stats = Optional(encoding_stats_)
        var bloom_filter_offset = t_col_metadata.bloom_filter_offset
        var bloom_filter_length = t_col_metadata.bloom_filter_length
        var offset_index_offset = t_column_chunk.offset_index_offset
        var offset_index_length = t_column_chunk.offset_index_length
        var column_index_offset = t_column_chunk.column_index_offset
        var column_index_length = t_column_chunk.column_index_length
        var unencoded_byte_array_data_bytes = Optional[Int64](None)
        var repetition_level_histogram_ = Optional[List[Int64]](None)
        var definition_level_histogram_ = Optional[List[Int64]](None)
        if t_col_metadata.size_statistics:
            var size_stats = t_col_metadata.size_statistics.value()
            unencoded_byte_array_data_bytes = size_stats.unencoded_byte_array_data_bytes
            repetition_level_histogram_ = size_stats.repetition_level_histogram
            definition_level_histogram_ = size_stats.definition_level_histogram

        var repetition_level_histogram = Optional[LevelHistogram](None)
        if repetition_level_histogram_:
            repetition_level_histogram = Optional(LevelHistogram(repetition_level_histogram_.value()))
        var definition_level_histogram = Optional[LevelHistogram](None)
        if definition_level_histogram_:
            definition_level_histogram = Optional(LevelHistogram(definition_level_histogram_.value()))

        return ColumnChunkMetaData(
            offset_index_offset,
            offset_index_length,
            column_index_offset,
            column_index_length,
            column_descr,
            encodings,
            file_path,
            file_offset,
            num_values,
            compression_codec,
            total_compressed_size,
            total_uncompressed_size,
            data_page_offset,
            index_page_offset,
            dictionary_page_offset,
            statistics.value(),
            encoding_stats,
            bloom_filter_offset,
            bloom_filter_length,
            unencoded_byte_array_data_bytes,
            repetition_level_histogram,
            definition_level_histogram
        )

    fn column_index_range(self) raises -> Optional[Range]:
        if not self.column_index_offset or not self.column_index_length:
            return None
        var offset = UInt64(self.column_index_offset.value())
        var length = UInt64(self.column_index_length.value())
        return Optional(Range(
            start=offset,
            end=offset + length,
        ))

    fn offset_index_range(self) raises -> Optional[Range]:
        if not self.offset_index_offset or not self.offset_index_length:
            return None
        var offset = UInt64(self.offset_index_offset.value())
        var length = UInt64(self.offset_index_length.value())
        return Optional(Range(
            start=offset,
            end=offset + length,
        ))


@value
struct RowGroupMetaData:
    var columns: List[ColumnChunkMetaData]
    var num_rows: Int64
    var sorting_columns: Optional[List[tt.SortingColumn]]
    var total_byte_size: Int64
    var schema_descr: types.SchemaDescriptor
    var file_offset: Optional[Int64]
    var ordinal: Optional[Int16]

    @staticmethod
    fn from_thrift(schema_descr: types.SchemaDescriptor, t_row_group: tt.RowGroup) raises -> Self:
        if schema_descr.num_columns() != len(t_row_group.columns):
            raise Error(
                "Column count mismatch. Schema has ", schema_descr.num_columns(),
                " columns while Row Group has ", len(t_row_group.columns),
            )
        var total_byte_size = t_row_group.total_byte_size
        var num_rows = t_row_group.num_rows
        var columns = List[ColumnChunkMetaData]()
        for i in range(len(t_row_group.columns)):
            var t_column = t_row_group.columns[i]
            var column_descr  = schema_descr.columns()[i]
            columns.append(ColumnChunkMetaData.from_thrift(column_descr, t_column))

        var sorting_columns = t_row_group.sorting_columns

        return RowGroupMetaData(
            columns,
            num_rows,
            sorting_columns,
            total_byte_size,
            schema_descr,
            t_row_group.file_offset,
            t_row_group.ordinal
        )

@value
struct ParquetMetaData:
    var file_metadata: FileMetaData
    var row_groups: List[RowGroupMetaData]
    var offset_index: Optional[ParquetOffsetIndex]
    var column_index: Optional[ParquetColumnIndex]

    fn num_row_groups(self) -> Int:
        return len(self.row_groups)

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

    fn __init__(out self, with_page_index: Bool):
        self.metadata = Optional[ParquetMetaData](None)
        self.column_index = with_page_index
        self.offset_index = with_page_index
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
        var schema = types.from_thrift(tfile_meta_data.schema)
        var schema_descr = types.SchemaDescriptor(schema)
        var row_groups = List[RowGroupMetaData]()
        for rg in tfile_meta_data.row_groups:
            row_groups.append(RowGroupMetaData.from_thrift(schema_descr, rg[]))

        return ParquetMetaData(
            file_meta_data,
            row_groups,
            Optional[List[List[OffsetIndexMetaData]]](None),
            Optional[ParquetColumnIndex](None),
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
        if self.column_index and self.offset_index:
            self.read_page_index(chunk_reader)

    fn read_page_index(mut self, chunk_reader: FileHandle) raises:
        var index_bytes_range = self.range_for_page_index()
        if not index_bytes_range:
            return
        _ = chunk_reader.seek(index_bytes_range.value().start)
        var index_bytes = chunk_reader.read_bytes(Int(index_bytes_range.value().length()))
        self.parse_column_index(index_bytes, index_bytes_range.value().start)
        self.parse_offset_index(index_bytes, index_bytes_range.value().start)

    fn range_for_page_index(self) raises -> Optional[Range]:
        if not self.metadata:
            raise Error("metadata not parsed yet")
        var metadata = self.metadata.value()
        var total_range = Optional[Range](None)
        for i in range(len(metadata.row_groups)):
            var rg = metadata.row_groups[i]
            for j in range(len(rg.columns)):
                var cc = rg.columns[j]
                var column_index_range = cc.column_index_range()
                total_range = accumulate_range(total_range, column_index_range)
                var offset_index_range = cc.offset_index_range()
                total_range = accumulate_range(total_range, offset_index_range)

        return total_range

    fn parse_single_column_index(self, bytes: List[UInt8], column_chunk: ColumnChunkMetaData) raises -> Index:
        return decode_column_index(bytes, column_chunk.column_type())

    fn parse_column_index(mut self, bytes: List[UInt8], start_offset: UInt64) raises:
        if not self.metadata:
            raise Error("metadata not parsed yet")
        var metadata = self.metadata.value()
        var col_index = ParquetColumnIndex()
        for rg_idx in range(len(metadata.row_groups)):
            var rg = metadata.row_groups[rg_idx]
            var cc_indices = List[Index]()
            for col_idx in range(len(rg.columns)):
                var cc = rg.columns[col_idx]
                if not cc.column_index_offset:
                    raise Error("Column index offset is not set for column ", col_idx, " in row group ", rg_idx)
                var ci_start = Int(cc.column_index_offset.value()) - Int(start_offset)
                if not cc.column_index_length:
                    raise Error("Column index length is not set for column ", col_idx, " in row group ", rg_idx)
                var ci_length = Int(cc.column_index_length.value())
                var index_bytes = bytes[ci_start:ci_start + ci_length]
                cc_indices.append(
                    self.parse_single_column_index(index_bytes, cc)
                )
            col_index.append(cc_indices)
        self.metadata.value().column_index = Optional[ParquetColumnIndex](col_index)

    fn parse_single_offset_index(self, bytes: List[UInt8]) raises -> OffsetIndexMetaData:
        return decode_offset_index(bytes)

    fn parse_offset_index(mut self, bytes: List[UInt8], start_offset: UInt64) raises:
        if not self.metadata:
            raise Error("metadata not parsed yet")
        var metadata = self.metadata.value()
        var offset_index = ParquetOffsetIndex()
        for rg_idx in range(len(metadata.row_groups)):
            var rg = metadata.row_groups[rg_idx]
            var oi_indices = List[OffsetIndexMetaData]()
            for col_idx in range(len(rg.columns)):
                var cc = rg.columns[col_idx]
                if not cc.offset_index_offset:
                    raise Error("Offset index offset is not set for column ", col_idx, " in row group ", rg_idx)
                var oi_start = Int(cc.offset_index_offset.value()) - Int(start_offset)
                if not cc.offset_index_length:
                    raise Error("Offset index length is not set for column ", col_idx, " in row group ", rg_idx)
                var oi_length = Int(cc.offset_index_length.value())
                var index_bytes = bytes[oi_start:oi_start + oi_length]
                oi_indices.append(
                    self.parse_single_offset_index(index_bytes)
                )
            offset_index.append(oi_indices)
        self.metadata.value().offset_index = Optional[ParquetOffsetIndex](offset_index)

    fn finish(mut self) raises -> ParquetMetaData:
        if self.metadata:
            return self.metadata.take()
        else:
            raise Error("metadata could not parsed")
