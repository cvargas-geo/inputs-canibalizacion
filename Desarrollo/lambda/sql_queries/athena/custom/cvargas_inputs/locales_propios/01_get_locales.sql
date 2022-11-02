WITH

    BLOCKS AS (
        SELECT 
            block_id, 
            B.shape_wkt 
        FROM "prod_countries"."country_{{params.COUNTRY}}_view_blocks"  B
        WHERE 
        /* Limpieza para que no fallen las funciones geoespaciales */
        B.shape_wkt  LIKE '%POLYGON((%))'
    ) 



	,POIS_CON_BUFFER AS (
	
		SELECT
			p.id,
            /*Pseudo buffer_search no es 100% circular , se alarga en los polos ,  pero es una buena aproximacion pues respeta el radio*/
            ST_Buffer(
                ST_GeometryFromText( P.shape_wkt ) ,
                    {{params.search_distance_in_meters}}.0 * 360.0 / (2.0 * pi() * cos( radians(ST_Y(P.shape_wkt )) )* 6400000.0)    
            )  AS search_buffer,
            ST_GeometryFromText(P.shape_wkt)  AS shape
		FROM prod_countries.country_{{params.COUNTRY}}_pois_comercios_servicios_view P 
		WHERE 
			P.pois_state_id = {{params.parametros.pois_state_id}}
        {# AGREGA LISTA DE SUBSTRINGS #}
        {%  if  params.parametros.substring_id   %}
            AND P.substring_id IN (
            {% for id in params.parametros.substring_id %}
                {{id}} {% if not loop.last %},{% endif %}
            {% endfor %}
        {%  endif %} )
        
    ) 
    , GET_DUPLICATED_DISTANCES  AS (
        /*
        INTERSECCION_CON_VIEW_BLOCKS 
        El local (buffer generado ) se cruza con el shape del block */

        SELECT
            P.id, 
            B.block_id   ,
            /*  Distancia en metros a la manzana (Restriccion de negocio)*/
            CASE 
                WHEN 
                    /*Si local cae dentro del block   */
                    ST_Intersects(
                        ST_GeometryFromText(B.shape_wkt),
                        P.shape 
                    ) THEN  0.0  
                ELSE    
                    /*Sino calcula distancia en metros   
                    ST_Distance entrega la distancia como grados decimales , LUEGO ES  CONVERTIDA A METROS CON UN FACTOR DE LATIDUD POR GRADO*/
                    ST_Distance(
                        ST_GeometryFromText(B.shape_wkt),
                        P.shape 
                    ) * 111139.0
                END as distancia 

        FROM POIS_CON_BUFFER P 
        JOIN BLOCKS  B
        ON  ST_intersects( 
                ST_GeometryFromText(B.shape_wkt ) ,
                P.search_buffer 
            )   
            
        /*Si se entregan comunas para optimizar o filtrar se aplican
        WHERE 
            VB.administrative_area_level_3 IN ( )
        */ 
    )
/*     SELECT * FROM GET_DUPLICATED_DISTANCES WHERE id  = 3096 and block_id = 1151036711
    SELECT AVG(distancia) AS prom_dist FROM GET_DUPLICATED_DISTANCES */
    
    ,GET_DEDUPLICATED_DISTANCES AS (
        /* SI UN LOCAL CAE EN MEDIO DE 2 MZ ESTO LO CORRIGE */
        SELECT 
            GDD.id,
            GDD.block_id,
            GDD.distancia
        FROM (
            SELECT
                A.*,
                ROW_NUMBER() OVER (
                    partition BY id
                    ORDER BY distancia asc
                ) rn
            FROM GET_DUPLICATED_DISTANCES A
            ) GDD
        WHERE GDD.rn = 1
    )
    /*  SELECT  *  FROM GET_DISTANCIAS_DEDUPLICADAS */
    ,GET_MIN_DISTANCE  AS (   
        SELECT 
            id,
            block_id as geo_id , /*para que athena puede generar un solo archivo por esta columna  */
            ROUND(MIN(distancia) , 2)  as distancia
        FROM GET_DEDUPLICATED_DISTANCES
        GROUP BY 
            id,
            block_id 
    )

   /*  Se agregan las columnas header manualmente y las coordenadas del local  */
    SELECT 
        1           as header, 
        'pois_id'   as pois_id , 
        'geo_id'    as geo_id , 
        'distancia' as distancia , 
        'longitud'  as longitud , 
        'latitud'   as latitud  , 
        'change'    as change  /*change*/
    
    UNION ALL
    /*Este select es solo para grupar el order by */
    SELECT * FROM (
        SELECT  
            0 AS header , 
            CAST(G.id as VARCHAR ) AS id , 
            CAST(G.geo_id as VARCHAR ) AS geo_id , 
            CAST(G.distancia as VARCHAR ) AS distancia ,  
            CAST(PP.longitud as VARCHAR ) as longitud,
            CAST(PP.latitud as VARCHAR ) as latitud,
            CAST(PP.latitud as VARCHAR ) as change /*change*/
            /* ,PP.shape_wkt as shape_pois
               ,B.shape_wkt as shape_block */
        FROM  GET_MIN_DISTANCE G
        LEFT JOIN  prod_countries.country_{{params.COUNTRY}}_pois_comercios_servicios_view PP ON PP.id = G.id
        /* LEFT JOIN prod_countries.country_{{params.COUNTRY}}_view_blocks B ON B.block_id = G.block_id */
        ORDER BY G.distancia DESC 
    )TBL