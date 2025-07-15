import parquet.gen.parquet.ttypes as tt
from parquet.types import Encoding, PageType

@value
struct PageEncodingStats:
    var page_type: PageType
    var encoding: Encoding
    var count: Int32

fn from_thrift(t_page_encoding_stats: tt.PageEncodingStats) raises -> PageEncodingStats:
    var page_type = PageType.from_thrift(t_page_encoding_stats.page_type)
    var encoding = Encoding.from_thrift(t_page_encoding_stats.encoding)
    return PageEncodingStats(
        page_type,
        encoding,
        t_page_encoding_stats.count
    )
