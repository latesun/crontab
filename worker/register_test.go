package worker

import "testing"

func TestGetLocalIP(t *testing.T) {
	ipv4, err := getLocalIP()
	if err != nil {
		t.Log(err)
		return
	}

	t.Log(ipv4)
}
