WITH  
	INTERSECCION_BUFFERS_POIS AS (
	
		SELECT
			B.block_id geo_id, 
			P.tipo , 
			p.sales_area ,
			p.box_area  

		FROM  prod_countries.country_{{params.COUNTRY}}_view_blocks_buffers   B  
		JOIN  prod_countries.country_{{params.COUNTRY}}_pois_comercios_servicios_view P
		ON ST_intersects(  B.buffer_{{params.BUFFER}}  , ST_GeometryFromText(P.shape_wkt ) )  
		WHERE 
			P.pois_state_id = 1  

    {# OPCION DE PUNTOS A EVALUAR DESDE PLATAFORMA AGREGA LISTA DE block_id #}
        {%  if  params.parametros.block_id is defined   %}
            AND B.block_id IN (
            {% for id in params.parametros.block_id %}
                {{id}} {% if not loop.last %},{% endif %} {% if loop.last %}){% endif %}
            {% endfor %}
        {%  endif %}


    )
    
    
    /* SBA = SUMA BOX AREA
	 * SSA = SUMA SALES AREA*/
	SELECT
		geo_id,
		SUM(CASE WHEN  tipo  = 1 THEN 1  ELSE 0 END )   AS comercios,
		SUM(CASE WHEN  tipo  = 2 THEN 1  ELSE 0 END )   AS servicios ,

		SUM(CASE WHEN  tipo  = 1 THEN IBP.box_area    ELSE 0 END )  AS sba_comercios,
		SUM(CASE WHEN  tipo  = 2 THEN IBP.box_area    ELSE 0 END )  AS sba_servicios ,

		SUM(CASE WHEN  tipo  = 1 THEN IBP.sales_area  ELSE 0 END )  AS ssa_comercios,
		SUM(CASE WHEN  tipo  = 2 THEN IBP.sales_area  ELSE 0 END )  AS ssa_servicios

	FROM INTERSECCION_BUFFERS_POIS IBP
	GROUP BY  geo_id
