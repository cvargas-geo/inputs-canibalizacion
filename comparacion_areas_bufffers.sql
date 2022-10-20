/*Comparacion de areas por metodos de buffer
country  --  buffer    --   % max diff
mx              1000            16.8
mx              5000            16.8

cl              1000            72.9
cl              5000            72.9

-- VALIDAR FIGURAS
SELECT a.id, a.block_id,
    a.administrative_area_level_1,
    a.administrative_area_level_2,
    a.administrative_area_level_3,
    ST_GeometryFromText( b.buffer_5000 ) AS  BUFF1 ,
    ST_Buffer(
            ST_POINT( b.longitud, b.latitud )    ,
            5000.0 * 360.0 / (2.0 * pi() * cos( radians( b.latitud  ) )* 6400000.0)
        ) AS  BUFF2
FROM prod_countries.country_mx_view_blocks_buffers b
left join prod_countries.country_mx_view_blocks  a on a.id =b.id and a.block_id =b.block_id
where
    a.administrative_area_level_1 = 'BAJA CALIFORNIA'
    AND a.administrative_area_level_2 = 'MEXICALI'
    AND   a.administrative_area_level_3 = 'VICENTE GUERRERO'
--   and block_id = 11111
 */
WITH AREAS AS (
    SELECT
        a.administrative_area_level_1,
        a.administrative_area_level_2,
        a.administrative_area_level_3,
        AVG( ST_Area(ST_GeometryFromText( b.buffer_1000 ) ) ) as prom_area_1 ,
        AVG( ST_Area(
            /*Pseudo buffer no es 100% circular , se alarga en los polos ,  pero es una buena aproximacion pues respeta a la latitud*/
            ST_Buffer(
                ST_POINT( b.longitud, b.latitud )    ,
                1000.0 * 360.0 / (2.0 * pi() * cos( radians( b.latitud  ) )* 6400000.0)
            )
        ) ) as prom_area_2
    FROM prod_countries.country_mx_view_blocks_buffers b
    left join prod_countries.country_mx_view_blocks  a on a.id =b.id and a.block_id =b.block_id
    where b.buffer_1000  LIKE '%POLYGON((%))'
    group by  a.administrative_area_level_1 , a.administrative_area_level_2, a.administrative_area_level_3
    )
    select
        administrative_area_level_1,
        administrative_area_level_2,
        administrative_area_level_3,
        prom_area_1,
        prom_area_2,
        prom_area_1 - prom_area_2 as diff,
        100 *  abs( (prom_area_1 - prom_area_2) / ( (prom_area_1+prom_area_2)/2 ) ) as percent_diff
    from  AREAS
    order by 7 desc