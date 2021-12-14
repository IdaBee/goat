DROP FUNCTION IF EXISTS compute_prd;
CREATE OR REPLACE FUNCTION compute_prd(grid_id_input integer, scenario_id_input integer)
  RETURNS SETOF void AS
$func$
DECLARE 
	airline_cost DOUBLE PRECISION;
	var_prd DOUBLE PRECISION;

BEGIN
	/*st_distance between centroids of neighbouring hexagons * walking speed 5km/h*/
	airline_cost = 187.0453151928;

		WITH neigb_hex AS
		(
		SELECT  f.geom AS geom, g.grid_id FROM grid_heatmap g, grid_heatmap f
		WHERE st_touches (g.geom,f.geom)
		)

		SELECT avg(r.PRd) AS PRd_AVG 
		INTO var_prd
		FROM ( SELECT x.* , (x.total_cost/airline_cost) as prd		 
		FROM (
		SELECT ROW_NUMBER() OVER(PARTITION BY p.geom ORDER BY  ST_CLOSESTPOINT(f.geom,st_centroid(p.geom)) <-> st_centroid(p.geom) ASc  ) AS row_number, 
			f.gridids[ f.gridids # grid_id_input],  
				ST_LineLocatePoint(f.geom, st_centroid(p.geom)) AS fractiON,
			(f.start_cost[f.gridids # grid_id_input] + (f.end_cost[f.gridids # grid_id_input] - f.start_cost[f.gridids # grid_id_input]) * ST_LineLocatePoint(f.geom, st_centroid(p.geom))) AS total_cost
		FROM reached_edges_heatmap f , grid_heatmap g , neigb_hex p
		WHERE f.geom && ST_Buffer(st_centroid(p.geom),0.002) 
		AND f.partial_edge IS FALSE
		AND (f.scenario_id = 0 OR f.scenario_id = scenario_id_input) 
		AND p.grid_id=grid_id_input
		AND g.grid_id=grid_id_input
		)x
		WHERE  x.row_number = 1) r
		GROUP BY gridids;

	IF scenario_id_input = 0 THEN
		UPDATE grid_heatmap r SET prd = var_prd
		WHERE r.grid_id = grid_id_input; 
		UPDATE grid_heatmap SET prd = 100 
		WHERE prd <= 1.0 OR prd IS null; 
	ELSE UPDATE area_isochrones_scenario SET prd = var_prd
		 WHERE grid_id = grid_id_input and scenario_id=scenario_id_input;
		 UPDATE area_isochrones_scenario SET prd = 100 
		 WHERE prd <= 1.0 OR prd IS null;
	END IF;

END;

$func$  LANGUAGE plpgsql;






