package spanner

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestHandler_Integration(t *testing.T) {
	// このテストは実際にGoogle Cloud APIにアクセスします。
	// 実行する環境でADC (Application Default Credentials) が設定されている必要があります。
	// gcloud auth application-default login
	if os.Getenv("GCP_PROJECT") == "" {
		t.Skip("GCP_PROJECT is not set. Skipping integration test.")
	}

	req, err := http.NewRequest("GET", "/spanner/autoscaler", nil)
	if err != nil {
		t.Fatal(err)
	}

	q := req.URL.Query()
	q.Add("project", "gcpug-public-spanner")
	q.Add("instance", "merpay-sponsored-instance")
	q.Add("pu_min", "100")
	q.Add("pu_max", "100")
	q.Add("pu_step", "100")
	q.Add("scale_up_threshold", "50")
	q.Add("scale_down_threshold", "30")
	req.URL.RawQuery = q.Encode()

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(Handler)

	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
		t.Errorf("response body: %s", rr.Body.String())
	}

	// レスポンスボディの内容をログに出力して確認
	t.Logf("Response Body: %s", rr.Body.String())
}
