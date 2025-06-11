import parquet.gen.parquet.ttypes as tt

@value
struct OffsetIndexMetaData:
    var page_locations: List[tt.PageLocation]
