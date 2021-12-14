DROP FUNCTION IF EXISTS connectivity_network;
CREATE OR REPLACE FUNCTION public.connectivity_network(scenario_id_input integer)
RETURNS SETOF VOID AS  
$func$
BEGIN

    DROP TABLE IF EXISTS buffer_ways;
    DROP TABLE IF EXISTS vertices_con;   
  	DROP TABLE IF EXISTS ways_con;
    DROP TABLE IF EXISTS ways_lnr_con;
	DROP TABLE IF EXISTS new_ways;

IF scenario_id_input = 0 then 

    CREATE TEMP TABLE ways_con as (
    SELECT * FROM ways 
    where class_id::text NOT IN 
                (SELECT UNNEST(select_from_variable_container('excluded_class_id_walking')))
    AND (foot NOT IN 
                (SELECT UNNEST(select_from_variable_container('categories_no_foot'))) OR foot IS NULL)); 

    CREATE TEMP TABLE vertices_con AS
    ( SELECT * FROM ways_vertices_pgr);

ELSE

    CREATE temp TABLE buffer_ways AS
    (SELECT w.* 
    FROM (SELECT UNNEST(deleted_ways) AS id FROM scenarios WHERE scenario_id=scenario_id_input) r, ways_userinput w 
    WHERE w.id=r.id) ; 
  
    WITH  r AS (
    SELECT geom FROM
    ways_modified w
    WHERE w.scenario_id=scenario_id_input)

    INSERT INTO buffer_ways(geom)	
    SELECT geom FROM r;

    CREATE temp TABLE ways_con AS 
    SELECT DISTINCT(w2.*) 
    FROM
            (SELECT w.* FROM ways_userinput w
            LEFT JOIN 
            (SELECT DISTINCT(original_id) id 
            FROM ways_userinput) r
            ON w.id = r.id 
            WHERE r.id IS NULL 
            AND (w.scenario_id=scenario_id_input or w.scenario_id is null)
            AND (  
                    (w.class_id::text NOT IN 
                    (SELECT UNNEST(SELECT_FROM_variable_container('excluded_class_id_walking')))) 
                OR (w.foot NOT IN 
                    (SELECT UNNEST(SELECT_FROM_variable_container('categories_no_foot'))) OR w.foot IS NULL)
                )
            AND w.id NOT IN (SELECT UNNEST(deleted_ways) from scenarios where scenario_id=scenario_id_input)) w2,
            buffer_ways w
        WHERE w2.geom && st_buffer(w.geom,0.0019);

    CREATE INDEX ON ways_con USING gist(geom);

    UPDATE ways_userinput_vertices_pgr g SET cnt = x.cnt
    FROM (SELECT  v.id, count(w.id) AS cnt
        FROM  ways_con w, ways_userinput_vertices_pgr v 
        WHERE st_intersects(w.geom,v.geom) 
        AND (w.scenario_id=scenario_id_input OR w.scenario_id IS NULL ) 
        AND (v.scenario_id=scenario_id_input OR v.scenario_id IS NULL )
        group BY v.id) x
    WHERE  g.id=x.id; 


    CREATE temp TABLE vertices_con AS(
    SELECT * 
    FROM (SELECT DISTINCT(w.*) 
          FROM ways_userinput_vertices_pgr w , ways_con b 
          WHERE st_intersects(w.geom,b.geom)) x
    WHERE x.id NOT IN (
            SELECT w.id 
            FROM (SELECT DISTINCT(w.*) FROM ways_userinput_vertices_pgr w,  ways_con b 
                  WHERE st_intersects(w.geom,b.geom) 
                  AND (w.scenario_id IS NULL OR w.scenario_id = scenario_id_input)) w,
                 (SELECT DISTINCT(w.*) FROM ways_userinput_vertices_pgr w,  ways_con b 
                  WHERE st_intersects(w.geom,b.geom) 
                  AND (w.scenario_id IS NULL OR w.scenario_id = scenario_id_input)) ww
            WHERE st_equals(w.geom,ww.geom) AND w.id!=ww.id AND w.scenario_id IS NULL));
    CREATE INDEX on vertices_con USING gist(geom); 

    UPDATE ways_con  w
    SET Source_cnt = v.cnt 
    FROM ways_userinput_vertices_pgr v  
    WHERE  v.id=w.source AND  ((w.userid=v.userid) OR (coalesce(w.userid,v.userid) IS NULL));

    UPDATE ways_con  w
    SET target_cnt = v.cnt 
    FROM ways_userinput_vertices_pgr v 
    WHERE  v.id=w.target  AND  ((w.userid=v.userid) OR (coalesce(w.userid,v.userid) IS NULL));

END IF; 

   	CREATE TEMP TABLE new_ways AS  
	
    WITH a AS (
	SELECT *
    FROM 
        (SELECT * FROM ways_con
        WHERE (source_cnt!=2 AND target_cnt=2) 
        OR (source_cnt=2 AND target_cnt=2) 
        OR (source_cnt=2 AND target_cnt!=2)) as x),

    b AS (
    SELECT ST_CollectionExtract(ST_Collect(v.geom)) AS geom  
	FROM vertices_con v , a w
    WHERE v.cnt>2 AND st_intersects(v.geom,w.geom)),

    c AS (
    SELECT ST_LineMerge(ST_CollectionExtract(ST_Collect(w.geom), 2)) AS geom , array_agg(w.id) AS ids
    FROM a w),
    
    d AS (
    SELECT DISTINCT((ST_Dump(ST_Split(lines.geom, endpoints.geom))).geom) AS geom 
    FROM c lines, b endpoints)

    SELECT  array_agg(w.id) AS ids, t.geom AS geom,  sum(w.length_m) AS length_m  
    FROM d t, ways_con w
    WHERE st_within(w.geom,t.geom)
    GROUP BY t.geom;

    CREATE temp TABLE ways_lnr_con AS
    SELECT t.ids[scenario_id_input] AS id, t.geom, t.length_m 
    FROM new_ways t;
    INSERT INTO ways_lnr_con
    SELECT  w.id, w.geom, w.length_m
    FROM ways_con w
        LEFT JOIN (SELECT unnest(ids) AS id FROM new_ways) t ON (t.id = w.id)
    WHERE t.id IS NULL;
    CREATE INDEX on ways_lnr_con USING gist(geom);

END 		
$func$  LANGUAGE plpgsql;
