package change_ip

import "testing"

func TestMieruIPRotationNeverAllocatesStaticIP(t *testing.T) {
	for _, static := range []bool{false, true} {
		for _, count := range []int{0, 1, 5} {
			got := selectIPRotation("mieru", static, count)
			want := "restart"
			if static {
				want = "release_restart"
			}
			if got != want {
				t.Fatalf("mieru static=%v count=%d: got %s, want %s", static, count, got, want)
			}
		}
	}
}
func TestOtherProtocolsRetainIPRotationPolicy(t *testing.T) {
	for _, protocol := range []string{"anytls", "vmess", ""} {
		for _, tc := range []struct {
			static bool
			count  int
			want   string
		}{{true, 0, "release"}, {false, 0, "allocate"}, {false, 5, "restart"}} {
			if got := selectIPRotation(protocol, tc.static, tc.count); got != tc.want {
				t.Fatalf("%s: got %s want %s", protocol, got, tc.want)
			}
		}
	}
}
