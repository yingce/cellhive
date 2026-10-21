package hyperdriveauth

import "testing"

func TestDeriveStableAndUnique(t *testing.T) {
	a := Derive("secret", "acme", "hd1")
	b := Derive("secret", "acme", "hd1")
	c := Derive("secret", "acme", "hd2")
	if a != b || a.User == "" || a.Password == "" {
		t.Fatalf("derive not stable: %+v %+v", a, b)
	}
	if a.User == c.User || a.Password == c.Password {
		t.Fatalf("derive collision: %+v %+v", a, c)
	}
	if len(a.User) > 32 {
		t.Fatalf("mysql user too long: %d", len(a.User))
	}
}
