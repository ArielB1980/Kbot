-- Clear Decision Traces to Force Re-Analysis
-- This will remove old regime classifications and force the system
-- to generate new ones with the updated logic

-- OPTION 1: Clear ALL decision traces (forces complete re-analysis)
-- DELETE FROM decision_traces;

-- OPTION 2: Clear only traces older than 1 hour (safer)
DELETE FROM decision_traces 
WHERE timestamp < NOW() - INTERVAL '1 hour';

-- OPTION 3: Update existing traces to mark them as stale (safest - keeps history)
-- UPDATE decision_traces 
-- SET details = jsonb_set(
--     details::jsonb, 
--     '{regime}', 
--     '"pending_reanalysis"'::jsonb
-- )
-- WHERE timestamp < NOW() - INTERVAL '1 hour';

-- After running this, the worker will generate fresh decision traces
-- with the new regime classification logic on the next analysis cycle.

-- Verify the change:
SELECT 
    regime,
    COUNT(*) as count
FROM decision_traces,
    jsonb_to_record(details::jsonb) as x(regime text)
GROUP BY regime
ORDER BY count DESC;
