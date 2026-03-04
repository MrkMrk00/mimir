// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	stdjson "encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_shardResourceAttributesMiddleware_RoundTrip(t *testing.T) {
	const tenantID = "test"
	const tenantShardCount = 4
	const tenantMaxShardCount = 128

	validReq := func() *http.Request {
		r := httptest.NewRequest("GET", "/api/v1/resources?match[]={job=%22test%22}&start=1688515200000&end=1688540400000", nil)
		r.Header.Set("X-Scope-OrgID", tenantID)
		return r
	}

	seriesReq := func() *http.Request {
		r := httptest.NewRequest("GET", "/api/v1/resources/series?resource.attr=service.name:test&start=1688515200000&end=1688540400000", nil)
		r.Header.Set("X-Scope-OrgID", tenantID)
		return r
	}

	makeResponse := func(series ...string) string {
		items := make([]string, len(series))
		for i, s := range series {
			items[i] = fmt.Sprintf(`{"labels":{%s},"versions":[]}`, s)
		}
		return fmt.Sprintf(`{"status":"success","data":{"series":[%s]}}`, strings.Join(items, ","))
	}

	tests := []struct {
		name           string
		request        func() *http.Request
		shardCount     int
		maxShardCount  int
		upstreamFunc   func(req *http.Request) (*http.Response, error)
		expectedShards int
		expectPassThru bool
		expectErr      string
		expectedSeries int
	}{
		{
			name:           "passes through /api/v1/resources/series",
			request:        seriesReq,
			shardCount:     tenantShardCount,
			maxShardCount:  tenantMaxShardCount,
			expectPassThru: true,
			upstreamFunc: func(req *http.Request) (*http.Response, error) {
				return newJSONResponse(makeResponse(`"job":"test"`)), nil
			},
		},
		{
			name:           "passes through when shard count < 2",
			request:        validReq,
			shardCount:     1,
			maxShardCount:  tenantMaxShardCount,
			expectPassThru: true,
			upstreamFunc: func(req *http.Request) (*http.Response, error) {
				return newJSONResponse(makeResponse(`"job":"test"`)), nil
			},
		},
		{
			name:          "returns error when shard count exceeds max",
			request:       validReq,
			shardCount:    200,
			maxShardCount: 100,
			expectErr:     "shard count 200 exceeds allowed maximum (100)",
		},
		{
			name:          "shards with 2 shards and merges results",
			request:       validReq,
			shardCount:    2,
			maxShardCount: tenantMaxShardCount,
			upstreamFunc: func(req *http.Request) (*http.Response, error) {
				// Verify each shard gets exactly one shard matcher (not accumulated).
				query := req.URL.Query()
				matchers := query["match[]"]
				shardMatcherCount := 0
				for _, m := range matchers {
					if strings.Contains(m, "__query_shard__") {
						shardMatcherCount++
					}
				}
				if shardMatcherCount != 1 {
					return nil, fmt.Errorf("expected exactly 1 shard matcher per request, got %d in match[]=%v", shardMatcherCount, matchers)
				}
				// Return different series per shard.
				for _, m := range matchers {
					if strings.Contains(m, "1_of_2") {
						return newJSONResponse(makeResponse(`"job":"shard1"`)), nil
					}
					if strings.Contains(m, "0_of_2") {
						return newJSONResponse(makeResponse(`"job":"shard0"`)), nil
					}
				}
				return newJSONResponse(makeResponse()), nil
			},
			expectedSeries: 2,
		},
		{
			name:          "upstream error propagated",
			request:       validReq,
			shardCount:    2,
			maxShardCount: tenantMaxShardCount,
			upstreamFunc: func(req *http.Request) (*http.Response, error) {
				return nil, fmt.Errorf("upstream error")
			},
			expectErr: "upstream error",
		},
		{
			name: "limit re-applied after merge",
			request: func() *http.Request {
				r := httptest.NewRequest("GET", "/api/v1/resources?match[]={job=%22test%22}&start=1688515200000&end=1688540400000&limit=1", nil)
				r.Header.Set("X-Scope-OrgID", tenantID)
				return r
			},
			shardCount:    2,
			maxShardCount: tenantMaxShardCount,
			upstreamFunc: func(req *http.Request) (*http.Response, error) {
				return newJSONResponse(makeResponse(`"job":"a"`, `"job":"b"`)), nil
			},
			expectedSeries: 1,
		},
		{
			name:          "empty responses merge to empty",
			request:       validReq,
			shardCount:    2,
			maxShardCount: tenantMaxShardCount,
			upstreamFunc: func(req *http.Request) (*http.Response, error) {
				return newJSONResponse(makeResponse()), nil
			},
			expectedSeries: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			limits := mockLimits{
				totalShards:       tc.shardCount,
				maxShardedQueries: tc.maxShardCount,
			}

			var upstreamCallCount int
			upstream := RoundTripFunc(func(req *http.Request) (*http.Response, error) {
				upstreamCallCount++
				if tc.upstreamFunc != nil {
					return tc.upstreamFunc(req)
				}
				return newJSONResponse(`{"status":"success","data":{"series":[]}}`), nil
			})

			mw := newShardResourceAttributesMiddleware(upstream, limits, log.NewNopLogger())

			req := tc.request()
			ctx := user.InjectOrgID(req.Context(), tenantID)
			req = req.WithContext(ctx)

			resp, err := mw.RoundTrip(req)

			if tc.expectErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectErr)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, resp)

			if tc.expectPassThru {
				assert.Equal(t, 1, upstreamCallCount, "expected single upstream call for pass-through")
				return
			}

			// Verify sharded requests were made.
			assert.Equal(t, tc.shardCount, upstreamCallCount, "expected %d sharded requests", tc.shardCount)

			// Parse response and check series count.
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			_ = resp.Body.Close()

			var parsed struct {
				Status string `json:"status"`
				Data   struct {
					Series []stdjson.RawMessage `json:"series"`
				} `json:"data"`
			}
			require.NoError(t, stdjson.Unmarshal(body, &parsed))
			assert.Equal(t, "success", parsed.Status)
			assert.Equal(t, tc.expectedSeries, len(parsed.Data.Series))
		})
	}
}

func newJSONResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
