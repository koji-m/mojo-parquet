from collections.string.codepoint import Codepoint

alias FOOTER_SIZE: Int = 8
alias PARQUET_MAGIC: List[UInt8] = List[UInt8](
    UInt8(Codepoint.ord("P").to_u32()),
    UInt8(Codepoint.ord("A").to_u32()),
    UInt8(Codepoint.ord("R").to_u32()),
    UInt8(Codepoint.ord("1").to_u32()),
)