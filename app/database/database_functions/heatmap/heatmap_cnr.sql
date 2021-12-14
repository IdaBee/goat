DROP FUNCTION IF EXISTS heatmap_cnr;
CREATE OR REPLACE FUNCTION public.heatmap_cnr(modus_input text DEFAULT 'default', scenario_id_input integer DEFAULT 0)
 RETURNS TABLE(grid_id integer, percentile_connected_node_ratio smallint, link_node_rati float, geom geometry)
 LANGUAGE plpgsql
AS $function$
DECLARE
	borders_quintiles numeric[]; 
BEGIN
	
	IF modus_input IN ('default','comparison') THEN   
		DROP TABLE IF EXISTS grids_default; 
		CREATE TEMP TABLE grids_default AS 
		SELECT g.grid_id, g.per_cnr o, g.cnr, g.geom  
		FROM grid_heatmap g;	
	END IF; 

	IF modus_input IN ('scenario','comparison') THEN  
		SELECT array_agg(border)
		INTO borders_quintiles
		FROM 
		(
			SELECT min(x.cnr) border
			FROM 
			(
				SELECT ntile(5) over (order by g.cnr) AS percentile_connected_node_ratio, g.cnr
				FROM grid_heatmap g
			) x 
			GROUP BY x.percentile_connected_node_ratio
			ORDER BY x.percentile_connected_node_ratio
		) b;
		
		DROP TABLE IF EXISTS grids_scenario;
		CREATE TEMP TABLE grids_scenario AS 
		WITH grids_to_classify AS 
		(	
			SELECT a.grid_id, 
			CASE WHEN COALESCE(a.connected_node_ratio,0) = 0 THEN 0
			WHEN COALESCE(a.connected_node_ratio,0) >= borders_quintiles[1] AND COALESCE(a.connected_node_ratio,0) < borders_quintiles[2] THEN 1
			WHEN COALESCE(a.connected_node_ratio,0) >= borders_quintiles[2] AND COALESCE(a.connected_node_ratio,0) < borders_quintiles[3] THEN 2
			WHEN COALESCE(a.connected_node_ratio,0) >= borders_quintiles[3] AND COALESCE(a.connected_node_ratio,0) < borders_quintiles[4] THEN 3
			WHEN COALESCE(a.connected_node_ratio,0) >= borders_quintiles[4] AND COALESCE(a.connected_node_ratio,0) < borders_quintiles[5] THEN 4
			WHEN COALESCE(a.connected_node_ratio,0) >= borders_quintiles[5] THEN 5
			END AS percentile_connected_node_ratio, a.connected_node_ratio
			FROM link_node_scenario a 
			WHERE scenario_id = scenario_id_input
		)
		SELECT g.grid_id, 
		CASE WHEN c.percentile_connected_node_ratio IS NULL THEN g.per_cnr::SMALLINT ELSE c.percentile_connected_node_ratio::SMALLINT 
		END AS percentile_connected_node_ratio, 
		CASE WHEN c.connected_node_ratio IS NULL THEN g.cnr ELSE c.connected_node_ratio END AS connected_node_ratio, g.geom
		FROM grid_heatmap g
		LEFT JOIN grids_to_classify c
		ON g.grid_id = c.grid_id; 
	END IF;
	
	IF modus_input = 'comparison' THEN 

		ALTER TABLE grids_default ADD PRIMARY KEY(grid_id);
		ALTER TABLE grids_scenario ADD PRIMARY KEY(grid_id);
		
		DROP TABLE IF EXISTS grids_comparison;
		CREATE TEMP TABLE grids_comparison AS 
		SELECT d.grid_id, (s.percentile_connected_node_ratio - d.per_cnr) AS percentile_connected_node_ratio, 
		COALESCE(s.connected_node_ratio - d.cnr, 0) AS connected_node_ratio, d.geom
		FROM grids_default d, grids_scenario s 
		WHERE d.grid_id = s.grid_id;
	
	END IF;
	
	IF modus_input = 'default' THEN 
		RETURN query 
		SELECT * FROM grids_default;
	ELSEIF modus_input = 'scenario' THEN 
		RETURN query 
		SELECT * FROM grids_scenario;
	ELSEIF modus_input = 'comparison' THEN 
		RETURN query 
		SELECT * FROM grids_comparison;
	END IF; 
END
$function$;