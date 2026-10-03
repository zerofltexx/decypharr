package server

import "testing"

func TestJSONHasKey(t *testing.T) {
	cases := []struct {
		body string
		path []string
		want bool
	}{
		{`{"repair":{"enabled":true}}`, []string{"repair", "reclaim"}, false},
		{`{"repair":{"reclaim":{}}}`, []string{"repair", "reclaim"}, true},
		{`{"repair":{"reclaim":{"enabled":false,"delete":false}}}`, []string{"repair", "reclaim"}, true},
		{`{"reclaim":{"enabled":false}}`, []string{"reclaim"}, true},
		{`{"schedule":"0 2 * * *"}`, []string{"reclaim"}, false},
		{`not json`, []string{"reclaim"}, false},
	}
	for _, c := range cases {
		if got := jsonHasKey([]byte(c.body), c.path...); got != c.want {
			t.Errorf("jsonHasKey(%s, %v) = %v, want %v", c.body, c.path, got, c.want)
		}
	}
}
