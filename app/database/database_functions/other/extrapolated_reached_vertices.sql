DROP TABLE IF EXISTS temp_reached_vertices;
CREATE temp TABLE temp_reached_vertices
(
	start_vertex integer,
	node integer,
	edge integer,
	cnt integer,
	cost NUMERIC,
	geom geometry,
	objectid integer
);
DROP FUNCTION IF EXISTS extrapolate_reached_vertices;
CREATE OR REPLACE FUNCTION public.extrapolate_reached_vertices(max_cost NUMERIC, max_length_links NUMERIC, speed NUMERIC , excluded_class_id integer[], categories_no_foot text[])
RETURNS SETOF type_catchment_vertices
 LANGUAGE sql
AS $function$

WITH touching_network AS 
(
	SELECT * FROM (
		SELECT t.start_vertex, w.id, w.geom, w.SOURCE, w.target, t.cost, t.node, t.edge, 1 as cnt, w.length_m, t.objectid, w.class_id, w.foot  
		FROM temp_reached_vertices t, ways w
		WHERE t.node = w.target 
		AND t.node <> w.SOURCE
		AND t.cost + (max_length_links/speed) > max_cost
		UNION ALL 
		SELECT t.start_vertex, w.id, w.geom, w.SOURCE, w.target, t.cost, t.node, t.edge, 1 as cnt, w.length_m, t.objectid, w.class_id, w.foot 
		FROM temp_reached_vertices t, ways w
		WHERE t.node <> w.target 
		AND t.node = w.SOURCE
		AND t.cost + (max_length_links/speed) > max_cost
	) x
	WHERE NOT x.class_id = ANY(excluded_class_id)
	AND (NOT x.foot = any(categories_no_foot) OR x.foot IS NULL)
),
not_completely_reached_network AS (
	SELECT SOURCE 
	FROM (
		SELECT SOURCE 
		FROM touching_network t 
		UNION ALL 
		SELECT target 
		FROM touching_network t 
	) x
	GROUP BY x.source
	HAVING count(x.source) < 2
)
SELECT t.start_vertex::integer, 99999999 AS node, t.id::integer edges, t.cnt, max_cost AS cost, st_startpoint(st_linesubstring(geom,1-(max_cost-cost)/(t.length_m/speed),1)) geom, objectid 
FROM touching_network t, not_completely_reached_network n 
WHERE t.SOURCE = n.source 
AND 1-(max_cost-cost)/(t.length_m/speed) BETWEEN 0 AND 1
UNION ALL 
SELECT t.start_vertex::integer, 99999999 AS node, t.id::integer, t.cnt, max_cost AS cost, st_endpoint(st_linesubstring(geom,0.0,(max_cost-cost)/(t.length_m/speed))) geom, objectid
FROM touching_network t, not_completely_reached_network n
WHERE t.target = n.source 
AND (max_cost-cost)/(t.length_m/speed) BETWEEN 0 AND 1 
UNION ALL 
SELECT start_vertex, node, edge, 1 as cnt , cost, geom, objectid FROM temp_reached_vertices;

$function$;
