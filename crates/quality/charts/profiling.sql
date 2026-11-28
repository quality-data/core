-- Profiling (decision 0031): each field of the cut, the emptiest first,
-- with its distinct values and its commonest shape's share.
SELECT f.field, s.records, f.empty,
       CASE WHEN s.records > 0 THEN round(f.empty::numeric / s.records, 4) END AS empty_share,
       f.distinct_values, f.pattern, f.floating,
       CASE WHEN s.records > f.empty
            THEN round(f.pattern_count::numeric / (s.records - f.empty), 4) END AS pattern_share
  FROM quality.field_quality f
  JOIN quality.run_summary s USING (tenant_id, dataset, cut)
 WHERE f.dataset = :dataset AND f.cut = :cut::date
 ORDER BY empty_share DESC NULLS LAST, f.field
