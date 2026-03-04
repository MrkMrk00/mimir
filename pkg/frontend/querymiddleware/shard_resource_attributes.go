// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	stdjson "encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type shardResourceAttributesMiddleware struct {
	upstream http.RoundTripper
	limits   Limits
	logger   log.Logger
}

func newShardResourceAttributesMiddleware(upstream http.RoundTripper, limits Limits, logger log.Logger) http.RoundTripper {
	return &shardResourceAttributesMiddleware{
		upstream: upstream,
		limits:   limits,
		logger:   logger,
	}
}

func (s *shardResourceAttributesMiddleware) RoundTrip(r *http.Request) (*http.Response, error) {
	spanLog, ctx := spanlogger.New(r.Context(), s.logger, tracer, "shardResourceAttributes.RoundTrip")
	defer spanLog.Finish()

	// Don't shard /api/v1/resources/series (reverse lookup uses resource.attr filters, not series matchers).
	if IsResourceAttributesSeriesQuery(r.URL.Path) {
		return s.upstream.RoundTrip(r)
	}

	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	shardCount := s.limits.QueryShardingTotalShards(tenantID)
	if shardCount < 2 {
		spanLog.DebugLog("msg", "query sharding disabled for request")
		return s.upstream.RoundTrip(r)
	}

	if maxShards := s.limits.QueryShardingMaxShardedQueries(tenantID); shardCount > maxShards {
		return nil, apierror.New(
			apierror.TypeBadData,
			fmt.Sprintf("shard count %d exceeds allowed maximum (%d)", shardCount, maxShards),
		)
	}

	spanLog.DebugLog("msg", "sharding resource attributes query", "shardCount", shardCount)

	// Parse original request params.
	reqValues, err := util.ParseRequestFormWithoutConsumingBody(r)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	// Parse limit for re-application after merge.
	var limit int64
	if limitStr := reqValues.Get("limit"); limitStr != "" {
		limit, _ = strconv.ParseInt(limitStr, 10, 64)
	}

	// Build N sharded requests, each with an extra match[]={__query_shard__="i_of_N"}.
	reqs, err := buildShardedResourceAttributesRequests(ctx, r, shardCount, reqValues)
	if err != nil {
		return nil, apierror.New(apierror.TypeInternal, err.Error())
	}

	resps, err := doShardedRequests(ctx, reqs, s.upstream)
	if err != nil {
		for _, resp := range resps {
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
		}
		return nil, apierror.New(apierror.TypeInternal, err.Error())
	}

	return mergeResourceAttributesResponses(resps, limit)
}

func buildShardedResourceAttributesRequests(ctx context.Context, origReq *http.Request, shardCount int, origValues url.Values) ([]*http.Request, error) {
	reqs := make([]*http.Request, shardCount)
	for i := 0; i < shardCount; i++ {
		r, err := http.NewRequestWithContext(ctx, origReq.Method, origReq.URL.Path, http.NoBody)
		if err != nil {
			return nil, err
		}

		// Deep-copy all original params. A shallow copy would alias the
		// []string backing arrays; Add("match[]", ...) would then mutate
		// the shared slice, accumulating shard matchers across iterations.
		vals := make(url.Values, len(origValues))
		for k, v := range origValues {
			vals[k] = append([]string(nil), v...)
		}

		// Add shard matcher as an extra match[] entry.
		shardMatcher, err := labels.NewMatcher(
			labels.MatchEqual, sharding.ShardLabel,
			sharding.ShardSelector{ShardIndex: uint64(i), ShardCount: uint64(shardCount)}.LabelValue(),
		)
		if err != nil {
			return nil, err
		}
		vals.Add("match[]", "{"+shardMatcher.String()+"}")

		r.URL.RawQuery = vals.Encode()
		r.RequestURI = r.URL.String()
		r.Header = origReq.Header.Clone()

		if err := user.InjectOrgIDIntoHTTPRequest(ctx, r); err != nil {
			return nil, err
		}

		reqs[i] = r
	}
	return reqs, nil
}

// resourceAttributesResponse is used for JSON unmarshalling of partial shard responses.
type resourceAttributesResponse struct {
	Status string `json:"status"`
	Data   *struct {
		Series []stdjson.RawMessage `json:"series"`
	} `json:"data,omitempty"`
	Error string `json:"error,omitempty"`
}

// mergeResourceAttributesResponses concatenates series from all shard
// responses, sorts for determinism, re-applies the client limit, and
// returns a single merged JSON response. Full in-memory buffering is
// acceptable because resource attributes responses are small (typically
// 10s–100s of series, each a few hundred bytes).
func mergeResourceAttributesResponses(resps []*http.Response, limit int64) (*http.Response, error) {
	var allSeries []stdjson.RawMessage

	for _, resp := range resps {
		if resp == nil {
			continue
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("reading shard response: %w", err)
		}

		var parsed resourceAttributesResponse
		if err := stdjson.Unmarshal(body, &parsed); err != nil {
			return nil, fmt.Errorf("unmarshalling shard response: %w", err)
		}
		if parsed.Status != "success" {
			return nil, fmt.Errorf("shard returned error: %s", parsed.Error)
		}
		if parsed.Data != nil {
			allSeries = append(allSeries, parsed.Data.Series...)
		}
	}

	// Sort by the raw JSON for deterministic output.
	sort.Slice(allSeries, func(i, j int) bool {
		return string(allSeries[i]) < string(allSeries[j])
	})

	// Re-apply limit after merge.
	if limit > 0 && int64(len(allSeries)) > limit {
		allSeries = allSeries[:limit]
	}

	// Build merged response.
	merged := struct {
		Status string `json:"status"`
		Data   struct {
			Series []stdjson.RawMessage `json:"series"`
		} `json:"data"`
	}{
		Status: "success",
	}
	if allSeries == nil {
		allSeries = []stdjson.RawMessage{}
	}
	merged.Data.Series = allSeries

	mergedBody, err := stdjson.Marshal(merged)
	if err != nil {
		return nil, fmt.Errorf("marshalling merged response: %w", err)
	}

	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(mergedBody)),
	}, nil
}
