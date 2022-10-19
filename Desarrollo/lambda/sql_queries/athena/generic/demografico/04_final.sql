	WITH DEMOGRAFICO AS (
        SELECT   
            A.geo_id,
            A.gse1,
            A.gse2,
            A.gse3,
            A.gse4,
            A.gse5,
            A.gse6,
            A.gse7,
            A.pob_gse1,
            A.pob_gse2,
            A.pob_gse3,
            A.pob_gse4,
            A.pob_gse5,
            A.pob_gse6,
            A.pob_gse7,
            A.pob_edad1,
            A.pob_edad2,
            A.pob_edad3,
            A.pob_edad4,
            A.pob_edad5,
            A.pob_edad6,
            A.pob_edad7,
            A.pob_edad8,
            A.pob_edad9,
            A.pob_edad10,
            A.pob_edad11,
            A.pob_edad12,
            A.pob_edad13,
            A.pob_edad14,
            A.pob_edad15,
            A.pob_edad16,
            A.pob_edad17,
            A.pob_edad18,
            A.pob_edad19,
            A.pob_edad20,
            A.pob_edad21,
            A.total_men,
            A.total_women,
            A.total_households,
            A.total_population,
            A.pxq,
            A.recoba_id,
            /*  CS.geo_id, */
            CS.comercios,
            CS.servicios,
            CS.sba_comercios,
            CS.sba_servicios,
            CS.ssa_comercios,
            CS.ssa_servicios,
            /* E.geo_id, */
            E.empresas,
            E.empleados, 
            BB.longitud longitud_mz ,
            BB.latitud latitud_mz
            /* step_customer_co_demografico_intersect_blocks_buffers_b600 */
        FROM      prod_inputs_estudios.{{params.CUSTOMER_NAME}}_{{params.COUNTRY}}_demografico_intersect_blocks_buffers_b{{params.BUFFER}}     A
        LEFT JOIN prod_inputs_estudios.{{params.CUSTOMER_NAME}}_{{params.COUNTRY}}_demografico_intersect_pois_buffers_b{{params.BUFFER}}       CS ON CS.geo_id = A.geo_id
        LEFT JOIN prod_inputs_estudios.{{params.CUSTOMER_NAME}}_{{params.COUNTRY}}_demografico_intersect_empresas_buffers_b{{params.BUFFER}}   E ON E.geo_id = A.geo_id
        LEFT JOIN prod_countries.country_{{params.COUNTRY}}_view_blocks_buffers BB  ON  BB.block_id = A.geo_id  
    )

    /* seccion de atractores  reglas en params.rules */
    ,ATRACTORES AS (
        
        SELECT P.id , ST_GeometryFromText(P.shape_wkt ) as shape
        FROM  prod_countries.country_{{params.COUNTRY}}_pois_comercios_servicios_view P
        WHERE P.pois_state_id IN( 1 , 3 )

    {% if params.COUNTRY not in ['ar','uy'] %}
        UNION ALL
        
        SELECT   
            /* las empresas tienen formatos distintos  */
            {% if params.COUNTRY in ['cl'] %}
                CAST(E.rut as INTEGER)          AS id
            {% elif params.COUNTRY in ['mx'] %}
                CAST(E.objectid as INTEGER)     AS id
            {% elif params.COUNTRY in ['co'] %}
                CAST(E.nit   as INTEGER)        AS id
            {% elif params.COUNTRY in ['pe'] %}
                CAST(E.idgeo as INTEGER)         AS id
            {% endif %}
            ,
           /* SOLO PE CO OCUPAN Latitud longitud */
            {% if params.COUNTRY in ['pe' ,'co'] %}
                ST_Point(E.longitud,E.latitud )
            {% else %}
                ST_GeometryFromText(E.shape_wkt )
            {% endif %} as shape
        FROM  prod_countries.country_{{params.COUNTRY}}_empresas E
    {% endif %}
    ) 
	,INTERSECCION_BLOCKS_ATRACTORES AS (
		SELECT
			B.block_id  ,
			COUNT(AT.id ) AS total_atractores
		FROM prod_countries.country_{{params.COUNTRY}}_view_blocks    B
        JOIN ATRACTORES AT
		ON ST_intersects(  B.shape_wkt  ,  AT.shape  )
		WHERE B.shape_wkt  LIKE '%POLYGON((%))'
		group by B.block_id 

    )
    ,ATTRACTORS_LAGGING AS (
        /*Si no se encontraron pois o empresas dentro de los 100 metros , se busca 200 ,se entrega el la primera coincidencia*/
		SELECT
			B.block_id  ,
			AT.id ,  
            ST_Distance(
                ST_Point(B.longitud , B.latitud),
                AT.shape 
            ) * 111139.0  as distancia  
		FROM prod_countries.country_{{params.COUNTRY}}_view_blocks_buffers   B
        JOIN ATRACTORES AT
		ON ST_intersects(  B.buffer_100  , AT.shape   )
		WHERE B.block_id NOT IN (SELECT DISTINCT block_id FROM INTERSECCION_BLOCKS_ATRACTORES )
    )
    ,GET_DEDUPLICATED_ATTRACTORS AS (
        /* Solo es necesario el primer elemento que aparezca */
        SELECT 
            GDD.block_id,
            GDD.id
        FROM (
            SELECT A.*,
                ROW_NUMBER() OVER (
                    partition BY A.id
                    ORDER BY A.distancia ASC
                ) rn
            FROM ATTRACTORS_LAGGING A
            ) GDD
        WHERE GDD.rn = 1
    )
    ,COUNT_ATTRACTORS AS (
        SELECT block_id , total_atractores FROM INTERSECCION_BLOCKS_ATRACTORES 
        UNION ALL 
        SELECT block_id, count(id) AS total_atractores   FROM ATTRACTORS_LAGGING GROUP BY block_id
    )
    ,SALIDA AS (
        SELECT 
            VB.block_id,
            VB.administrative_area_level_1 as region,
            
            VB.administrative_area_level_3 AS comuna,
            /* ST_GeometryFromText(VB.shape_wkt ) AS shape,  */
            coalesce(A.total_atractores, 0.0 )  as total_atractores 
        FROM prod_countries.country_{{params.COUNTRY}}_view_blocks  VB  
        LEFT JOIN COUNT_ATTRACTORS A ON    VB.block_id = A.block_id 
    )
    ,REGLAS_ATRACTORES AS (
         {{params.atractor_rule}}   
    )


    SELECT 
        D.*,
        RA.categoria,
        RA.ponderador
    FROM DEMOGRAFICO D 
    LEFT JOIN REGLAS_ATRACTORES RA ON RA.block_id = D.geo_id