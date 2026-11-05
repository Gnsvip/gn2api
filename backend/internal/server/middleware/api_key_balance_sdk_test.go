//go:build unit

package middleware

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Real SDKs with their default retry policies, fake credentials and loopback.
// Each script makes two separate user calls on one client instance. A permanent
// denial must stop each call without preventing the user from sending again.
func TestAPIKeyBalanceOfficialSDKNoRetry(t *testing.T) {
	for _, sdk := range []struct {
		name, executable, script string
	}{
		{"node", os.Getenv("GN2API_TEST_NODE"), balanceSDKNodeScript},
		{"python", os.Getenv("GN2API_TEST_PYTHON"), balanceSDKPythonScript},
	} {
		t.Run(sdk.name, func(t *testing.T) {
			if sdk.executable == "" {
				t.Skip("set GN2API_TEST_NODE / GN2API_TEST_PYTHON for isolated SDK checks")
			}
			for _, streaming := range []bool{false, true} {
				for _, tc := range []struct {
					name        string
					legacy      bool
					rateLimited bool
					wantCount   int64
				}{
					{"balance", false, false, 2},
					{"legacy_missing_header_control", true, false, 6},
					{"real_rate_limit_control", false, true, 6},
				} {
					t.Run(fmt.Sprintf("%s/stream=%t", tc.name, streaming), func(t *testing.T) {
						router := newExhaustedBalanceTestRouter(t)
						var requests atomic.Int64
						server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							requests.Add(1)
							if tc.rateLimited {
								w.Header().Set("Content-Type", "application/json")
								w.WriteHeader(http.StatusTooManyRequests)
								_, _ = io.WriteString(w, `{"error":{"type":"rate_limit_exceeded","code":"rate_limit_exceeded","message":"Try later"}}`)
								return
							}
							if tc.legacy {
								// Reproduce v0.2.64's response by removing only the new
								// hint from the real middleware's response.
								recorded := httptest.NewRecorder()
								router.ServeHTTP(recorded, r)
								for key, values := range recorded.Header() {
									if strings.EqualFold(key, "X-Should-Retry") {
										continue
									}
									w.Header()[key] = values
								}
								w.WriteHeader(recorded.Code)
								_, _ = w.Write(recorded.Body.Bytes())
								return
							}
							router.ServeHTTP(w, r)
						}))
						defer server.Close()
						ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
						defer cancel()
						flag := "-c"
						if sdk.name == "node" {
							flag = "-e"
						}
						cmd := exec.CommandContext(ctx, sdk.executable, flag, sdk.script)
						code := "insufficient_quota"
						if tc.rateLimited {
							code = "rate_limit_exceeded"
						}
						cmd.Env = append(os.Environ(), "GN2API_SDK_BASE_URL="+server.URL+"/v1", "GN2API_SDK_CODE="+code, fmt.Sprintf("GN2API_SDK_STREAM=%t", streaming))
						output, err := cmd.CombinedOutput()
						require.NoError(t, ctx.Err(), "%s", output)
						require.NoError(t, err, "%s", output)
						require.Equal(t, tc.wantCount, requests.Load(), "%s", output)
						t.Logf("two explicit calls: SDK=%s requests=%d result=%s", sdk.name, requests.Load(), output)
					})
				}
			}
		})
	}
}

const balanceSDKNodeScript = `
const assert = require('node:assert/strict');
const OpenAI = require(process.env.GN2API_TEST_OPENAI_NODE);
(async () => {
  const client = new OpenAI({apiKey:'local-dummy-key', baseURL:process.env.GN2API_SDK_BASE_URL, timeout:10000});
  let denials = 0;
  for (let turn=0; turn<2; turn++) {
    try {
      await client.responses.create({model:'gpt-5.1', input:'Reply OK only.', stream:process.env.GN2API_SDK_STREAM==='true'});
      throw new Error('unexpected success');
    } catch (error) {
      assert.equal(error.status, 429);
      assert.equal(error.code, process.env.GN2API_SDK_CODE);
      denials++;
    }
  }
  console.log(JSON.stringify({denials, default_retries:client.maxRetries}));
})().catch(error => { console.error(error); process.exitCode=1; });
`

const balanceSDKPythonScript = `
import json, os, openai
client = openai.OpenAI(api_key='local-dummy-key', base_url=os.environ['GN2API_SDK_BASE_URL'], timeout=10)
denials = 0
try:
    for turn in range(2):
        try:
            client.responses.create(model='gpt-5.1', input='Reply OK only.', stream=os.environ['GN2API_SDK_STREAM']=='true')
            raise AssertionError('unexpected success')
        except openai.APIStatusError as error:
            assert error.status_code == 429
            assert error.code == os.environ['GN2API_SDK_CODE']
            denials += 1
    print(json.dumps({'denials':denials, 'default_retries':client.max_retries, 'version':openai.__version__}))
finally:
    client.close()
`
