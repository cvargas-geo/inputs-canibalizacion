/*
Eliminar las columnas inecesarias del group by como los 
admn lvl 1 2 3 
recoba
lat long 
shape
 */
select 
    z.id,
	z.block_id as geo_id,
	z.administrative_area_level_1,
	z.administrative_area_level_2,
	z.administrative_area_level_3,
	z.recoba_id,
	(
		z."seis" + z."cinco" + z."cuatro" + z."tres" + z."dos" + z."uno"
	) as gasto,
	z.longitud,
	z.latitud,
	z.shape
from (
		select 
		    a.id,
			a.block_id,
			a.administrative_area_level_1,
			a.administrative_area_level_2,
			a.administrative_area_level_3,
			a.recoba_id,
			a.longitud,
			a.latitud,
			ST_AsText(st_centroid(ST_GeometryFromText(a.shape_wkt))) as shape,
			a.total_households,
			sum(
				case
					when b.gse_id = 1 then a.hog_gse1 * b.monto
				end
			) as "seis",
			sum(
				case
					when b.gse_id = 2 then a.hog_gse2 * b.monto
				end
			) as "cinco",
			sum(
				case
					when b.gse_id = 3 then a.hog_gse3 * b.monto
				end
			) as "cuatro",
			sum(
				case
					when b.gse_id = 4 then a.hog_gse4 * b.monto
				end
			) as "tres",
			sum(
				case
					when b.gse_id = 5 then a.hog_gse5 * b.monto
				end
			) as "dos",
			sum(
				case
					when b.gse_id = 6 then a.hog_gse6 * b.monto
				end
			) as "uno"
		 
		from prod_countries.country_co_view_blocks a
	    left join prod_countries.country_co_canastas_total b on a.recoba_id = b.id_recoba
		where b.canasta_categoria_id = 20  
		AND a.shape_wkt  LIKE '%POLYGON((%))'
		group by
		    a.id,
			a.block_id,
			a.administrative_area_level_1,
			a.administrative_area_level_2,
			a.administrative_area_level_3,
			a.recoba_id,
			a.longitud,
			a.latitud,
			ST_AsText(st_centroid(ST_GeometryFromText(a.shape_wkt))),
-- 			st_centroid(a.shape_wkt) as shape,
			a.total_households --628mb
-- 		    1,
-- 			2,
-- 			3,
-- 			4,
-- 			5,
-- 			6,
-- 			7,
-- 			8,
-- 			9
--  	    10
	) z;