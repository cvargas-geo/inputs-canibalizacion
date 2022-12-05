WITH 
    GASTO_POR_ZONA_REPARTO AS (
        select 
        	z.id_pois , 
        	SUM(CAST(g.gasto AS DECIMAL(22, 5))) as gasto,  
        	z.shape_wkt
         
        from  prod_cannibalization.gyn_cl_zonas_reparto z
        join prod_cannibalization.gyn_cl_delivery_gasto_por_block g on --(lo mismo de gasto local)
        	ST_intersects(
        		ST_GeometryFromText(g.shape),
        		ST_GeometryFromText(z.shape_wkt)
        	) 
        where z.shape_wkt like '%POLYGON((%))'
        group by z.id_pois   , z.shape_wkt
    )
    -- SELECT COUNT(*) FROM GASTO_POR_ZONA_REPARTO --343  
    ,GASTO_REPARTO AS (
        select 
        	lp.pois , 
        	gzr.id_pois as id_poisd, 
        	gzr.gasto 
         
        from prod_cannibalization.gyn_cl_locales_propios lp
        join GASTO_POR_ZONA_REPARTO gzr on
        	ST_intersects(
        		ST_GeometryFromText(lp.shape_wkt),
        		ST_GeometryFromText(gzr.shape_wkt)
        	) 
        where lp.shape_wkt like 'POINT(%)' 
         
    )
    SELECT * FROM GASTO_REPARTO  
    -- SELECT COUNT(*) FROM GASTO_REPARTO --2674
    