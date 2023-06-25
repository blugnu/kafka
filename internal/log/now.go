package log

import "time"

// now provides a function reference to time.Now.  It may be replaced
// if/as required to facilitate testing that requires a predictable
// wall-clock time.
var now = time.Now
