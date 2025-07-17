import parquet.gen.parquet.ttypes as tt

@value
struct OffsetIndexMetaData:
    var page_locations: List[tt.PageLocation]
    var unencoded_byte_array_data_bytes: Optional[List[Int64]]
