package pagedvfs

/*
#include <stdint.h>
*/
import "C"

import (
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

// goPagedFileStatFD parses the pinned SQLite 3.53.4 Unix FILESTAT contract.
// Decode tokens rather than a map: maps silently accept duplicate h fields.
// C calls this with its new file still alive and without holding gRegMu.
//
//export goPagedFileStatFD
func goPagedFileStatFD(data *C.char, size C.int) C.int {
	if data == nil || size <= 0 || size > 64*1024 {
		return -1
	}
	body := C.GoStringN(data, size)
	if !utf8.ValidString(body) {
		return -1
	}
	d := json.NewDecoder(strings.NewReader(body))
	first, err := d.Token()
	if err != nil || first != json.Delim('{') {
		return -1
	}
	fd := int64(-1)
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return -1
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return -1
		}
		if key == "h" {
			if fd >= 0 {
				return -1
			}
			n, err := strconv.ParseUint(string(value), 10, 31)
			if err != nil {
				return -1
			}
			fd = int64(n)
		}
	}
	last, err := d.Token()
	if err != nil || last != json.Delim('}') {
		return -1
	}
	if _, err := d.Token(); err != io.EOF {
		return -1
	}
	return C.int(fd)
}
