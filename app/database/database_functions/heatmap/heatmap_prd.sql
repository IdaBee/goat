DROP FUNCTION IF EXISTS heatmap_prd;
CREATE OR REPLACE FUNCTION public.heatmap_prd(modus_input text DEFAULT 'default', scenario_id_input integer DEFAULT 0)
 RETURNS TABLE(grid_id integer, per_prd smallint, prd float, geom geometry)
 LANGUAGE plpgsql
AS $function$
DECLARE
	borders_quintiles numeric[]; 
BEGIN
	
	IF modus_input IN ('default','comparison') THEN   
		DROP TABLE IF EXISTS grids_default; 
		CREATE TEMP TABLE grids_default AS 
		SELECT g.grid_id, g.per_prd, g.prd, g.geom  
		FROM grid_heatmap g;	
	END IF; 

	IF modus_input IN ('scenario','comparison') THEN  
		SELECT array_agg(border)
		INTO borders_quintiles
		FROM 
		(
			SELECT min(x.prd) border
			FROM 
			(
				SELECT ntile(5) over (order by g.prd) AS per_prd, g.prd
				FROM grid_heatmap g
			) x 
			GROUP BY x.per_prd
			ORDER BY x.per_prd
		) b;
		
		DROP TABLE IF EXISTS grids_scenario;
		CREATE TEMP TABLE grids_scenario AS 
		WITH grids_to_classify AS 
		(	
			SELECT a.grid_id, 
			CASE WHEN COALESCE(a.prd,0) = 0 THEN 0
			WHEN COALESCE(a.prd,0) >= borders_quintiles[1] AND COALESCE(a.prd,0) < borders_quintiles[2] THEN 1
			WHEN COALESCE(a.prd,0) >= borders_quintiles[2] AND COALESCE(a.prd,0) < borders_quintiles[3] THEN 2
			WHEN COALESCE(a.prd,0) >= borders_quintiles[3] AND COALESCE(a.prd,0) < borders_quintiles[4] THEN 3
			WHEN COALESCE(a.prd,0) >= borders_quintiles[4] AND COALESCE(a.prd,0) < borders_quintiles[5] THEN 4
			WHEN COALESCE(a.prd,0) >= borders_quintiles[5] THEN 5
			END AS per_prd, a.prd
			FROM area_isochrones_scenario a
			WHERE scenario_id = scenario_id_input
		)
		SELECT g.grid_id, 
		CASE WHEN c.per_prd IS NULL THEN g.per_prd::SMALLINT ELSE c.per_prd::SMALLINT 
		END AS per_prd, 
		CASE WHEN c.prd IS NULL THEN g.prd ELSE c.prd END AS prd, g.geom
		FROM grid_heatmap g
		LEFT JOIN grids_to_classify c
		ON g.grid_id = c.grid_id; 
	END IF;
	
	IF modus_input = 'comparison' THEN 

		ALTER TABLE grids_default ADD PRIMARY KEY(grid_id);
		ALTER TABLE grids_scenario ADD PRIMARY KEY(grid_id);
		
		DROP TABLE IF EXISTS grids_comparison;
		CREATE TEMP TABLE grids_comparison AS 
		SELECT d.grid_id, (s.per_prd - d.per_prd) AS per_prd, 
		COALESCE(s.prd - d.prd, 0) AS prd, d.geom
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