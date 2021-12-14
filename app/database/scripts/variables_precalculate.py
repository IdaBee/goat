prepare_tables = '''
	DROP TABLE IF EXISTS grid_heatmap;
	CREATE TABLE grid_heatmap AS 
	SELECT ST_TRANSFORM(hx.geom, 4326) AS geom
	FROM (SELECT ST_TRANSFORM(geom, 3857) geom FROM study_area_union) s, 
	LATERAL ST_HexagonGrid(return_meter_srid_3857() * %s, s.geom) hx
	WHERE ST_Within(hx.geom, s.geom)
	OR (
		ST_Intersects(hx.geom, st_boundary(s.geom))
		AND 
		ST_AREA(ST_Intersection(hx.geom, s.geom)) / ST_AREA(hx.geom) > 0.3
	);

	with w as(
		select way as geom from planet_osm_polygon 
		where "natural" = 'water'
	),
	g as(
		select g.geom from grid_heatmap g, w
		where st_within(g.geom,w.geom)
	)
	delete from grid_heatmap where geom in (select * from g);


	ALTER TABLE grid_heatmap add column grid_id serial;
	ALTER TABLE grid_heatmap ADD COLUMN area_isochrone float;
	ALTER TABLE grid_heatmap ADD COLUMN percentile_area_isochrone smallint; 
 	ALTER TABLE grid_heatmap ADD COLUMN pedshed double precision;
	ALTER TABLE grid_heatmap ADD COLUMN per_pedshed smallint;
	'''


sql_new_grid= '''DROP TABLE IF EXISTS grid;
select distinct grid_id, geom into grid from grid_heatmap;
ALTER TABLE grid add primary key (grid_id);
'''



sql_clean = 'DROP TABLE IF EXISTS grid; ALTER TABLE grid_heatmap add column id serial; CREATE INDEX test_index_sensitivity ON test(sensitivity);'



sql_grid_population = '''

ALTER TABLE grid_heatmap ADD COLUMN population integer;
ALTER TABLE grid_heatmap ADD COLUMN percentile_population smallint;
WITH sum_pop AS (
	SELECT g.grid_id, sum(p.population)::integer AS population  
	FROM grid_heatmap g, population p
	WHERE ST_Intersects(g.geom,p.geom)
	GROUP BY grid_id
)
UPDATE grid_heatmap SET population=s.population
FROM sum_pop s 
WHERE grid_heatmap.grid_id = s.grid_id;

UPDATE grid_heatmap 
SET percentile_population = 
(CASE WHEN population BETWEEN 1 AND 20 THEN 1 
WHEN population BETWEEN 20 AND 80 THEN 2
WHEN population BETWEEN 80 AND 200 THEN 3 
WHEN population BETWEEN 200 AND 400 THEN 4 
WHEN population > 400 THEN 5 END)
WHERE population IS NOT NULL; 

UPDATE grid_heatmap SET percentile_population = 0
WHERE percentile_population IS NULL;

UPDATE grid_heatmap SET percentile_area_isochrone = x.percentile 
FROM (
	SELECT grid_id,ntile(5) over 
	(order by area_isochrone) AS percentile
	FROM grid_heatmap
	WHERE area_isochrone IS NOT NULL 
) x
WHERE grid_heatmap.grid_id = x.grid_id;

UPDATE grid_heatmap SET percentile_area_isochrone = 0 WHERE percentile_area_isochrone IS NULL; 


UPDATE grid_heatmap SET per_pedshed = x.percentile 
FROM (
	SELECT grid_id,ntile(5) over 
	(order by pedshed) AS percentile
	FROM grid_heatmap
	WHERE pedshed IS NOT NULL 
) x
WHERE grid_heatmap.grid_id = x.grid_id;

UPDATE grid_heatmap SET per_pedshed = 0 WHERE per_pedshed IS NULL;


'''
# a bit slower setup, but using connectvity functions
sql_grid_connectivity = '''
ALTER TABLE grid_heatmap ADD column node integer;
ALTER TABLE grid_heatmap ADD column real_node integer;
ALTER TABLE grid_heatmap ADD column link integer;
ALTER TABLE grid_heatmap ADD column street_density double precision;
ALTER TABLE grid_heatmap ADD column LNR numeric;
ALTER TABLE grid_heatmap ADD column network_length numeric;
ALTER TABLE grid_heatmap ADD column CNR numeric;
ALTER TABLE grid_heatmap ADD column gamma double precision;
ALTER TABLE grid_heatmap ADD column alpha double precision;

select * from connectivity_network(0);
select connectivity_indicators(g.grid_id,0)
from grid_heatmap g;


ALTER TABLE grid_heatmap ADD COLUMN per_street_density integer;
UPDATE grid_heatmap SET per_street_density =j.per_den
from (select grid_id, ntile(5) over (order by street_density) AS per_den
	  from grid_heatmap
	  WHERE street_density IS NOT NULL) j 
where j.grid_id=grid_heatmap.grid_id;
UPDATE grid_heatmap SET per_street_density = 0 WHERE per_street_density is null;

ALTER TABLE grid_heatmap ADD COLUMN per_lnr integer;
UPDATE grid_heatmap SET per_lnr =i.link_node_perc
from (select grid_id, ntile(5) over (order by lnr) AS link_node_perc
	  FROM grid_heatmap
	  WHERE lnr IS NOT NULL) i
where i.grid_id=grid_heatmap.grid_id;
UPDATE grid_heatmap SET per_lnr = 0 WHERE per_lnr is null;

ALTER TABLE grid_heatmap ADD column per_cnr double precision; 
UPDATE grid_heatmap SET per_cnr =j.per_den
from (select grid_id,ntile(5) over (order by cnr) AS per_den
	  from grid_heatmap
	  WHERE cnr IS NOT NULL) j 
where j.grid_id=grid_heatmap.grid_id;
UPDATE grid_heatmap SET per_cnr  = 0 WHERE per_cnr is null;

ALTER TABLE grid_heatmap ADD column per_intersection_density double precision;
UPDATE grid_heatmap SET per_intersection_density =j.per_den
from (select grid_id,ntile(5) over (order by real_node) AS per_den
	  from grid_heatmap
	  WHERE real_node IS NOT NULL) j 
where j.grid_id=grid_heatmap.grid_id;
UPDATE grid_heatmap SET per_intersection_density = 0 WHERE per_intersection_density is null;

ALTER TABLE grid_heatmap ADD column per_alpha double precision;
UPDATE grid_heatmap SET per_alpha =j.alpha_perc
from (select grid_id, ntile(5) over (order by alpha) AS alpha_perc
	  from grid_heatmap
	  WHERE alpha IS NOT NULL) j 
where j.grid_id=grid_heatmap.grid_id;
UPDATE grid_heatmap SET per_alpha= 0 WHERE per_alpha is null;

ALTER TABLE grid_heatmap ADD column per_gamma double precision;
UPDATE grid_heatmap SET per_gamma =j.gamma_perc
from (select grid_id, ntile(5) over (order by gamma) AS gamma_perc
	  from grid_heatmap
	  WHERE gamma IS NOT NULL) j 
where j.grid_id=grid_heatmap.grid_id;
UPDATE grid_heatmap SET per_gamma = 0 WHERE per_gamma is null;

ALTER TABLE grid_heatmap ADD PRD double precision;
ALTER TABLE grid_heatmap ADD per_PRD smallint;
'''

sql_percentile_prd = ''' 
UPDATE grid_heatmap r SET per_prd = j.per_prd
		from (select grid_id, ntile(6) over (order by prd) AS per_prd
		from grid_heatmap 
	 	WHERE prd IS NOT NULL) j 
		where r.grid_id=j.grid_id;
  '''