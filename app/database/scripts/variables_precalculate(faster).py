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
'''
#makes setup faster (alpha included)
sql_grid_connectivity = '''
DROP TABLE IF EXISTS ways_one_default;
CREATE TEMP TABLE ways_one_default as (
SELECT * FROM ways 
WHERE (source_cnt!=2 and target_cnt=2) 
      OR(source_cnt=2 and (target_cnt=2 or target_cnt!=2))
	  AND class_id::text NOT IN 
              (SELECT UNNEST(select_from_variable_container('excluded_class_id_walking')))
      AND (foot NOT IN 
              (SELECT UNNEST(select_from_variable_container('categories_no_foot'))) OR foot IS NULL));

DROP TABLE IF EXISTS endpoints_default;
CREATE TEMP TABLE endpoints_default AS (
SELECT v.* FROM ways_vertices_pgr v , ways_one_default w
WHERE v.cnt>2 and st_intersects(v.geom,w.geom));

DROP TABLE IF EXISTS t1_default;
CREATE TEMP TABLE t1_default as (
SELECT ST_LineMerge(ST_CollectionExtract(ST_Collect(geom), 2)) as geom , array_agg(id) as ids
From ways_one_default);

DROP TABLE IF EXISTS t2_default;
CREATE TEMP TABLE t2_default as (
Select ST_CollectionExtract(ST_Collect(geom)) as geom 
From endpoints_default);

DROP TABLE IF EXISTS t5_default;
CREATE TEMP TABLE t5_default as (
Select distinct((ST_Dump(ST_Split(lines.geom, endpoints.geom))).geom) as geom 
From t1_default lines, t2_default endpoints);

DROP TABLE IF EXISTS t6_default;
CREATE TEMP TABLE t6_default as (
select  array_agg(w.id) as ids, t.geom as geom,  sum(w.length_m) as length_m  from t5_default t, ways w
where st_within(w.geom,t.geom)
group by t.geom);

DROP TABLE IF EXISTS ways_lnr_default;
CREATE Table ways_lnr_default as 
select ids[1] as id, geom, length_m from t6_default;
INSERT INTO ways_lnr_default
SELECT  B.id, B.geom, B.length_m
FROM ways B 
    LEFT JOIN (select unnest(ids) as id from t6_default) A ON (A.id = B.id)
WHERE A.id IS NULL;
create index on ways_lnr_default using gist(geom);
create index on ways_lnr_default using btree(id);
create index on ways_lnr_default using btree(length_m);

ALTER TABLE grid_heatmap ADD column node integer;
UPDATE grid_heatmap SET node = x.node
from( select g.grid_id, count(w.geom) AS node
	  FROM ways_vertices_pgr w, grid_heatmap g 
	  WHERE w.cnt!=2 and st_within(w.geom,g.geom)
	  group by g.grid_id) x
where grid_heatmap.grid_id=x.grid_id;
/*UPDATE grid_heatmap SET node = 0 WHERE node is null;*/

ALTER TABLE grid_heatmap ADD column real_node integer;
UPDATE grid_heatmap SET real_node = i.node
from (SELECT g.grid_id::integer, count(no.geom)::double precision AS node
      FROM grid_heatmap g, ways_vertices_pgr  no 
      WHERE no.cnt>2 and st_within(no.geom,g.geom)
      GROUP BY g.grid_id) i
      where grid_heatmap.grid_id=i.grid_id;

ALTER TABLE grid_heatmap ADD column link integer;
UPDATE grid_heatmap SET link = x.link
from( select g.grid_id, count(w.geom) as link  
	  FROM ways_lnr_default w, grid_heatmap g 
	  WHERE st_within(w.geom,g.geom)
	  group by g.grid_id) x
where grid_heatmap.grid_id=x.grid_id;
/*UPDATE grid_heatmap SET link = 0 WHERE link is null;*/

ALTER TABLE grid_heatmap ADD column street_density double precision;
UPDATE grid_heatmap SET street_density=y.street_length
from(select g.grid_id, sum(length_m)::double precision as street_length 
	 from grid_heatmap g, ways_lnr_default w
	where st_within(w.geom,g.geom)
	group by g.grid_id) y
where grid_heatmap.grid_id=y.grid_id;
/*UPDATE grid_heatmap SET street_density = 0 WHERE street_density is null;*/

ALTER TABLE grid_heatmap ADD column LNR numeric;
UPDATE grid_heatmap SET LNR =z.lnr
from (select grid_id, 
	  round(((link::double precision)/(node::double precision))::numeric,2) as LNR
	  from grid_heatmap) z
where grid_heatmap.grid_id=z.grid_id;


UPDATE grid_heatmap SET CNR =z.cnr
from (select g.grid_id, round(((g.real_node::double precision)/(g.node::double precision))::numeric,2) as cnr
	  from grid_heatmap g ) z
where grid_heatmap.grid_id=z.grid_id;

ALTER TABLE grid_heatmap ADD column gamma double precision;
UPDATE grid_heatmap SET gamma = l.gamma
from (select grid_id, 
	  case when node >1 then (link::double precision /((node::double precision * ((node::double precision)-1))/2))
	  else 0 END as gamma
	  from grid_heatmap ) l 
where grid_heatmap.grid_id=l.grid_id;

ALTER TABLE grid_heatmap ADD column alpha double precision;
UPDATE grid_heatmap SET alpha = l.alpha
from (select grid_id, 
	  case when node >1 then (link::double precision - node::double precision + 1) /((node::double precision * ((node::double precision)-1))/2)
	  when node = link then 0
	  else 0 END as alpha
	  from grid_heatmap ) l 
where grid_heatmap.grid_id=l.grid_id;

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

ALTER TABLE grid_heatmap ADD COLUMN per_lnrw integer;
UPDATE grid_heatmap SET per_lnrw =i.link_node_perc
from (select grid_id,ntile(5) over (order by lnrw) AS link_node_perc
	  FROM grid_heatmap
	  WHERE lnrw IS NOT NULL) i
where i.grid_id=grid_heatmap.grid_id;
UPDATE grid_heatmap SET per_lnrw  = 0 WHERE per_lnrw  is null;

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

ALTER TABLE grid_heatmap ADD column per_gamma double precision;
UPDATE grid_heatmap SET per_gamma =j.gamma_perc
from (select grid_id, ntile(5) over (order by gamma) AS gamma_perc
	  from grid_heatmap
	  WHERE gamma IS NOT NULL) j 
where j.grid_id=grid_heatmap.grid_id;
UPDATE grid_heatmap SET per_gamma = 0 WHERE per_gamma is null;

ALTER TABLE grid_heatmap ADD PRD double precision;
ALTER TABLE grid_heatmap ADD per_PRD smallint;
ALTER TABLE grid_heatmap ADD prd_int double precision;
ALTER TABLE grid_heatmap ADD per_prd_int smallint; 

'''

sql_percentile_prd = ''' 
UPDATE grid_heatmap r SET per_prd = j.per_prd
		from (select grid_id, ntile(5) over (order by prd) AS per_prd
		from grid_heatmap 
	 	WHERE prd IS NOT NULL) j 
		where r.grid_id=j.grid_id;
  
  UPDATE grid_heatmap r SET per_prd_int = j.per_prd_int
		from (select grid_id, ntile(5) over (order by prd_int) AS per_prd_int
		from grid_heatmap 
	 	WHERE prd_int IS NOT NULL) j 
		where r.grid_id=j.grid_id;
  '''