/*
Nota la salida retorna una columna shape_wkt, esta se actualiza posteriomente por el etl

*/
with
    GASTO_REPARTO as (
        select
        	bg.block_id ,
        	z.id_pois ,
        	CAST(bg.gasto AS DECIMAL(22,5)) as gasto,
            b.shape_wkt
           /*  0 as np */
            /*case when
                ST_intersects(
                    ST_Centroid(ST_GeometryFromText(b.shape_wkt))  ,
                    ST_GeometryFromText('{{cannibalization_shape}}') )
                then   1.0 else 0.0 end as np */
        from {{db}}_cannibalization.{{report_name}}_{{schema}}_zonas_reparto z
        join {{db}}_cannibalization.{{report_name}}_{{schema}}_{{ETL_NAME}}_gasto_por_block bg on 
                ST_intersects(ST_GeometryFromText(bg.shape), ST_GeometryFromText(z.shape_wkt))
        join {{db}}_countries.country_{{schema}}_view_blocks b on b.block_id = bg.block_id
        where b.shape_wkt like '%POLYGON((%))'
        and  z.shape_wkt like '%POLYGON((%))'
    )

    SELECT * FROM GASTO_REPARTO