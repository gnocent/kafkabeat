package jolokia

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"
)

// TestRecord réenregistre testdata depuis les agents Jolokia du banc (Kafka
// 3.9 et Jolokia 1.x sur 8778, 4.4 et Jolokia 2.x sur 8779) :
// KAFKABEAT_JOLOKIA_RECORD=1 go test -run TestRecord ./internal/jolokia/
func TestRecord(t *testing.T) {
	if os.Getenv("KAFKABEAT_JOLOKIA_RECORD") == "" {
		t.Skip("KAFKABEAT_JOLOKIA_RECORD non défini")
	}
	reqs, _ := requests()
	body, _ := json.Marshal(reqs)
	for name, url := range map[string]string{"kafka39": "http://localhost:8778/jolokia/", "kafka44": "http://localhost:8779/jolokia/"} {
		resp, err := http.Post(url+"?canonicalNaming=true", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, data, "", " "); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile("testdata/"+name+".json", pretty.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
