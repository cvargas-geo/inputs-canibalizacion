WITH  
	INTERSECCION_VIEW_BLOCKS_BUFFERS AS (
		SELECT
            B.block_id       AS geo_id,
            SUM(VB.hog_gse1) AS gse1,
            SUM(VB.hog_gse2) AS gse2,
            SUM(VB.hog_gse3) AS gse3,
            SUM(VB.hog_gse4) AS gse4,
            SUM(VB.hog_gse5) AS gse5,
            SUM(VB.hog_gse6) AS gse6,
            SUM(VB.hog_gse7) AS gse7,

            SUM(VB.pob_gse1) AS pob_gse1,
            SUM(VB.pob_gse2) AS pob_gse2,
            SUM(VB.pob_gse3) AS pob_gse3,
            SUM(VB.pob_gse4) AS pob_gse4,
            SUM(VB.pob_gse5) AS pob_gse5,
            SUM(VB.pob_gse6) AS pob_gse6,
            SUM(VB.pob_gse7) AS pob_gse7,

            SUM(VB.pob_edad1) AS pob_edad1,
            SUM(VB.pob_edad2) AS pob_edad2,
            SUM(VB.pob_edad3) AS pob_edad3,
            SUM(VB.pob_edad4) AS pob_edad4,
            SUM(VB.pob_edad5) AS pob_edad5,
            SUM(VB.pob_edad6) AS pob_edad6,
            SUM(VB.pob_edad7) AS pob_edad7,
            SUM(VB.pob_edad8) AS pob_edad8,
            SUM(VB.pob_edad9) AS pob_edad9,
            SUM(VB.pob_edad10) AS pob_edad10,
            SUM(VB.pob_edad11) AS pob_edad11,
            SUM(VB.pob_edad12) AS pob_edad12,
            SUM(VB.pob_edad13) AS pob_edad13,
            SUM(VB.pob_edad14) AS pob_edad14,
            SUM(VB.pob_edad15) AS pob_edad15,
            SUM(VB.pob_edad16) AS pob_edad16,
            SUM(VB.pob_edad17) AS pob_edad17,
            SUM(VB.pob_edad18) AS pob_edad18,
            SUM(VB.pob_edad19) AS pob_edad19, 
            SUM(VB.pob_edad20) AS pob_edad20,
            SUM(VB.pob_edad21) AS pob_edad21,
            SUM(VB.pob_edad22) AS pob_edad22,
            SUM(VB.pob_edad23) AS pob_edad23,
            SUM(VB.pob_edad24) AS pob_edad24,
            SUM(VB.pob_edad25) AS pob_edad25,
            SUM(VB.pob_edad26) AS pob_edad26,
            SUM(VB.pob_edad27) AS pob_edad27,
            SUM(VB.pob_edad28) AS pob_edad28,
            SUM(VB.pob_edad29) AS pob_edad29, 
            SUM(VB.pob_edad30) AS pob_edad30,
            SUM(VB.pob_edad31) AS pob_edad31,
            SUM(VB.pob_edad32) AS pob_edad32,
            SUM(VB.pob_edad33) AS pob_edad33,
            SUM(VB.pob_edad34) AS pob_edad34,
            SUM(VB.pob_edad35) AS pob_edad35,
            SUM(VB.pob_edad36) AS pob_edad36,
            SUM(VB.pob_edad37) AS pob_edad37,
            SUM(VB.pob_edad38) AS pob_edad38,
            SUM(VB.pob_edad39) AS pob_edad39, 
            SUM(VB.pob_edad40) AS pob_edad40,
            SUM(VB.pob_edad41) AS pob_edad41,
            SUM(VB.pob_edad42) AS pob_edad42,
            SUM(VB.pob_edad43) AS pob_edad43,
            SUM(VB.pob_edad44) AS pob_edad44,
            SUM(VB.pob_edad45) AS pob_edad45,
            SUM(VB.pob_edad46) AS pob_edad46,
            SUM(VB.pob_edad47) AS pob_edad47,
            SUM(VB.pob_edad48) AS pob_edad48,
            SUM(VB.pob_edad49) AS pob_edad49,

		    SUM(VB.total_men) AS total_men,
		    SUM(VB.total_women) AS total_women,
		    SUM(VB.total_households) AS total_households,
		    SUM(VB.total_population) AS total_population,
		    SUM( VB.pxq )  AS pxq  
		
		FROM  prod_countries.country_{{params.COUNTRY}}_view_blocks_buffers   B  
		JOIN  prod_countries.country_{{params.COUNTRY}}_view_blocks    VB
		ON 
            /*B.buffer_{{params.BUFFER}} like 'POLYGON((%))' SOLO PARA ATHENA,*/
	    VB.shape_wkt like '%POLYGON((%))' /*SOLO PARA ATHENA,  OJO CON VIEW_BLOKCS ALGUNOS POLIGONOS MAL FORMADOS  */
	    AND  ST_intersects(
	        ST_GeometryFromText(B.buffer_{{params.BUFFER}})   ,
	        ST_Centroid( ST_GeometryFromText(VB.shape_wkt))
	     )

    {#  regiones  #}
        {%  if  params.parametros.administrative_area_level_1 is defined    %}
            /*REGIONES*/
            AND B.administrative_area_level_1 IN (
            {% for area in params.parametros.administrative_area_level_1 %}
                '{{area}}' {% if not loop.last %},{% endif %} {% if loop.last %}){% endif %}
            {% endfor %} 
        {%  endif %}

    {#  regiones  #}
        {%  if  params.parametros.administrative_area_level_2 is defined    %}
            /*PROVIANCIAS*/
            AND B.administrative_area_level_2 IN (
            {% for area in params.parametros.administrative_area_level_2 %}
                '{{area}}' {% if not loop.last %},{% endif %} {% if loop.last %}){% endif %}
            {% endfor %} 
        {%  endif %}

    {#  regiones  #}
        {%  if  params.parametros.administrative_area_level_3 is defined    %}
            /*COMUNAS*/
            AND B.administrative_area_level_3 IN (
            {% for area in params.parametros.administrative_area_level_3 %}
                '{{area}}' {% if not loop.last %},{% endif %} {% if loop.last %}){% endif %}
            {% endfor %} 
        {%  endif %}


    {# OPCION DE PUNTOS A EVALUAR DESDE PLATAFORMA AGREGA LISTA DE block_id #}
        {%  if  params.parametros.block_id is defined  and  params.ETL_NAME == 'demografico' %}
            WHERE B.block_id IN (
            {% for id in params.parametros.block_id %}
                {{id}} {% if not loop.last %},{% endif %} {% if loop.last %}){% endif %}
            {% endfor %}
        {%  endif %}


        GROUP BY B.block_id
    
	) 
    SELECT  
        A.*,
        /* PARA ETL DE GASTOS */
        B.recoba_id
        /*  ,
        SUM(A.gse1) gse1,
        SUM(A.gse2) gse2,
        SUM(A.gse3) gse3,
        SUM(A.gse4) gse4,
        SUM(A.gse5) gse5,
        SUM(A.gse6) gse6,
        SUM(A.gse7) gse7,

        SUM(A.pob_gse1) pob_gse1,
        SUM(A.pob_gse2) pob_gse2,
        SUM(A.pob_gse3) pob_gse3,
        SUM(A.pob_gse4) pob_gse4,
        SUM(A.pob_gse5) pob_gse5,
        SUM(A.pob_gse6) pob_gse6,
        SUM(A.pob_gse7) pob_gse7,

        SUM(A.pob_edad1) pob_edad1 ,
        SUM(A.pob_edad2) pob_edad2 ,
        SUM(A.pob_edad3) pob_edad3 ,
        SUM(A.pob_edad4) pob_edad4 ,
        SUM(A.pob_edad5) pob_edad5 ,
        SUM(A.pob_edad6) pob_edad6 ,
        SUM(A.pob_edad7) pob_edad7 ,
        SUM(A.pob_edad8) pob_edad8 ,
        SUM(A.pob_edad9) pob_edad9 ,
        SUM(A.pob_edad10) pob_edad10,
        SUM(A.pob_edad11) pob_edad11,
        SUM(A.pob_edad12) pob_edad12,
        SUM(A.pob_edad13) pob_edad13,
        SUM(A.pob_edad14) pob_edad14,
        SUM(A.pob_edad15) pob_edad15,
        SUM(A.pob_edad16) pob_edad16,
        SUM(A.pob_edad17) pob_edad17,
        SUM(A.pob_edad18) pob_edad18,
        SUM(A.pob_edad19) pob_edad19,
        SUM(A.pob_edad20) pob_edad20,
        SUM(A.pob_edad21) pob_edad21,
        SUM(A.pob_edad22) pob_edad22,
        SUM(A.pob_edad23) pob_edad23,
        SUM(A.pob_edad24) pob_edad24,
        SUM(A.pob_edad25) pob_edad25,
        SUM(A.pob_edad26) pob_edad26,
        SUM(A.pob_edad27) pob_edad27,
        SUM(A.pob_edad28) pob_edad28,
        SUM(A.pob_edad29) pob_edad29,
        SUM(A.pob_edad30) pob_edad30,
        SUM(A.pob_edad31) pob_edad31,
        SUM(A.pob_edad32) pob_edad32,
        SUM(A.pob_edad33) pob_edad33,
        SUM(A.pob_edad34) pob_edad34,
        SUM(A.pob_edad35) pob_edad35,
        SUM(A.pob_edad36) pob_edad36,
        SUM(A.pob_edad37) pob_edad37,
        SUM(A.pob_edad38) pob_edad38,
        SUM(A.pob_edad39) pob_edad39,
        SUM(A.pob_edad40) pob_edad40,
        SUM(A.pob_edad41) pob_edad41,
        SUM(A.pob_edad42) pob_edad42,
        SUM(A.pob_edad43) pob_edad43,
        SUM(A.pob_edad44) pob_edad44,
        SUM(A.pob_edad45) pob_edad45,
        SUM(A.pob_edad46) pob_edad46,
        SUM(A.pob_edad47) pob_edad47,
        SUM(A.pob_edad48) pob_edad48,
        SUM(A.pob_edad49) pob_edad49,
        SUM(A.total_men) total_men,
        SUM(A.total_women) total_women,
        SUM(A.total_households) total_households,
        SUM(A.total_population) total_population,
        SUM(A.pxq) pxq */
    FROM INTERSECCION_VIEW_BLOCKS_BUFFERS A 
    LEFT JOIN prod_countries.country_{{params.COUNTRY}}_view_blocks    B ON B.block_id = A.geo_id