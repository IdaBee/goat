DROP FUNCTION IF EXISTS connectivity_indicators;
CREATE OR REPLACE FUNCTION public.connectivity_indicators(grid_id_input integer, scenario_id_input integer)
	RETURNS SETOF VOID AS  
$func$
DECLARE

	all_link NUMERIC; 
	all_node NUMERIC;
	ID NUMERIC;
    SD DOUBLE PRECISION; 
    link_node_ratio DOUBLE PRECISION; 
	connected_node_ratio DOUBLE PRECISION;
    gamma_ DOUBLE PRECISION; 
    alpha_ DOUBLE PRECISION;
                      
BEGIN 

	SELECT i.node 
	INTO all_node 
	FROM (SELECT g.grid_id::integer, count(no.geom)::double precision AS node, scenario_id_input AS scenario_id
          FROM grid_heatmap g, vertices_con no 
          WHERE no.cnt!=2 and st_within(no.geom,g.geom)
          AND g.grid_id=grid_id_input 
     GROUP BY g.grid_id ) i;
	
	SELECT i.node 
	INTO ID 
	FROM (SELECT g.grid_id::integer, count(no.geom)::double precision AS node, scenario_id_input AS scenario_id
          FROM grid_heatmap g, vertices_con  no 
          WHERE no.cnt>2 and st_within(no.geom,g.geom)
          AND g.grid_id=grid_id_input 
     GROUP BY g.grid_id) i;
	 
	SELECT i.link
	INTO all_link 
	FROM (SELECT l.grid_id::integer, l.link::double precision, scenario_id_input AS scenario_id
	FROM  
	(
        SELECT g.grid_id, count(wu.geom)::double precision AS link 
        FROM grid_heatmap g, ways_lnr_con wu
        WHERE (st_within(wu.geom,g.geom)) 
              AND g.grid_id=grid_id_input 
        GROUP BY g.grid_id 
    ) l) i ;

    SELECT sum(length_m) 
    INTO SD
    FROM grid_heatmap g, ways_lnr_con i
    WHERE ST_WITHIN(i.geom, g.geom) AND g.grid_id = grid_id_input
    Group by g.grid_id;

    SELECT round(((all_link::double precision)/(all_node::double precision))::numeric,2)  
	INTO  link_node_ratio;

    SELECT (ID/all_node)::numeric
	INTO connected_node_ratio;
	
    SELECT round((all_link::double precision /((all_node::double precision * ((all_node::double precision)-1))/2))::numeric,3) as gamma 
    INTO gamma_;

    /* SELECT round((((all_link::double precision - all_node::double precision) + 1) /
    (((all_node::double precision * ((all_node::double precision)-1))/2)))::numeric,3) as alpha 
    INTO alpha_;'*/
    
	IF scenario_id_input = 0 THEN 
		UPDATE grid_heatmap SET node=all_node, real_node=ID, link=all_link, street_density=SD, lnr=link_node_ratio, cnr=connected_node_ratio, gamma=gamma_, alpha=alpha_ 
        WHERE grid_id = grid_id_input;
	ELSE
	    IF all_link is null and all_node is null THEN 
		select ; 
		ELSE 
		INSERT INTO link_node_scenario(grid_id,scenario_id, link, node, con_node, street_density, link_node_ratio, connected_node_ratio, gamma, alpha) 
		VALUES(grid_id_input, scenario_id_input, all_link, all_node, ID, SD, link_node_ratio,  connected_node_ratio, gamma_, alpha_ );
		END IF;
	END IF;

END;	
$func$  LANGUAGE plpgsql;



/*SELECT connectivity_indicators(428,1)*/

/*SELECT connectivity_indicators(g.grid_id,1) from (select DISTINCT g.grid_id from area_isochrones_scenario g, ways_modified w, grid_heatmap r
												  where r.grid_id= g.grid_id and st_intersects(r.geom,w.geom)) g*/

/*alpha index*/



