@value
struct Range:
    var start: UInt64
    var end: UInt64

    fn length(self) -> UInt64:
        return self.end - self.start
