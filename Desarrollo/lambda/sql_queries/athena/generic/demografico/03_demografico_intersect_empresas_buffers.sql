WITH  
	INTERSECCION_BUFFERS_EMPRESAS AS (
		SELECT
			B.block_id  AS geo_id,

	    /* las empresas tienen formatos distintos TODO:normalizar */
		{% if params.COUNTRY in ['cl'] %}
			E.rut            AS empresa_id  , 
			E.n_trabajadores AS empleados    
		{% elif params.COUNTRY in ['mx'] %}
			E.objectid  AS empresa_id  , 
			E.empleados AS empleados    
		{% elif params.COUNTRY in ['co'] %}
			E.nit  AS empresa_id  , 
			0      AS empleados    
		{% elif params.COUNTRY in ['pe','ar','uy'] %}
			E.idgeo    AS empresa_id  , 
			E.n_trabaja  AS empleados    
		{% endif %}  

		FROM  prod_countries.country_{{params.COUNTRY}}_view_blocks_buffers   B  
		JOIN  prod_countries.country_{{params.COUNTRY}}_empresas    E 
		ON 
			/* B.buffer_4500 like 'POLYGON((%))' SOLO PARA ATHENA  NO NECESARIO 
 		     AND E.shape_wkt like 'POINT((%))'  SOLO PARA ATHENA */
			ST_intersects(
				B.buffer_{{params.BUFFER}}  , 
				/* SOLO PE CO OCUPAN LATitud longitud */
				{% if params.COUNTRY in ['pe' ,'co','ar','uy'] %}
					ST_Point(E.longitud,E.latitud ) 
				{% else %}
					ST_GeometryFromText(E.shape_wkt )
				{% endif %}
			)

    {# OPCION DE PUNTOS A EVALUAR DESDE PLATAFORMA AGREGA LISTA DE block_id #}
        {%  if  params.parametros.block_id is defined   %}
            WHERE B.block_id IN (
            {% for id in params.parametros.block_id %}
                {{id}} {% if not loop.last %},{% endif %} {% if loop.last %}){% endif %}
            {% endfor %}
        {%  endif %}
	) 

	SELECT
		geo_id,
		COUNT( empresa_id ) empresas  , 
		SUM( empleados )  empleados  
	FROM INTERSECCION_BUFFERS_EMPRESAS
	GROUP BY  geo_id 