package history

import "testing"

// R6.S3: a cursor that the plugin did not make.
func TestCursor(t *testing.T) {
	c := Cursor("0a1b2c3d4e5f", 1234)
	n, s, err := ParseCursor(c)
	if err != nil || n != "0a1b2c3d4e5f" || s != 1234 {
		t.Fatalf("parse: %s %d %v", n, s, err)
	}
	for _, bad := range []string{"abc", "", "!!", Cursor("XYZ", 1), "YzE6YWI6MA"} {
		if _, _, err := ParseCursor(bad); err == nil {
			t.Fatalf("cursor %q parses", bad)
		}
	}
}
