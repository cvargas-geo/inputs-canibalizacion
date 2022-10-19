SELECT
    B.block_id as geo_id ,
    P.id as pois_id,
    /* P.categoria,*/
    /* P.subcadena, */
    regexp_replace(P.subcadena, '[^a-zA-Z0-9_]' , '_') AS subcadena ,
    P.substring_id, 
    P.box_area,
    P.sales_area 
FROM   prod_countries.country_{{params.COUNTRY}}_view_blocks_buffers B 
JOIN   prod_countries.country_{{params.COUNTRY}}_pois_comercios_servicios_view P
ON ST_intersects( B.buffer_{{params.BUFFER}}  , ST_GeometryFromText(P.shape_wkt ) )  
WHERE 
    P.pois_state_id = {{params.parametros.pois_state_id}}

{# AGREGA LISTA DE SUBSTRINGS #}
{%  if  params.parametros.substring_id  %}
    AND P.substring_id IN (
    {% for id in params.parametros.substring_id %}
        {{id}} {% if not loop.last %},{% endif %}
    {% endfor %}
{%  endif %} )


{# Se incluyen las categorias com serv,  si se envian desde el front #}
{%  if  params.parametros.category_id is defined   %}
    UNION ALL
    /*TECNICAMENTE SUBCADENA Y CATEGORIAS SON COSAS DISTINTAS POR LO QUE NO DEBERIA AFECTAR EL PIVOTEO*/
    SELECT
        B.block_id as geo_id ,
        P.id as pois_id,
        regexp_replace(P.categoria, '[^a-zA-Z0-9_]' , '_') as categoria , 
        /* P.categoria ,  */
        P.substring_id, 
        P.box_area,
        P.sales_area 
    FROM   prod_countries.country_{{params.COUNTRY}}_view_blocks_buffers B 
    JOIN   prod_countries.country_{{params.COUNTRY}}_pois_comercios_servicios_view P
    ON ST_intersects( B.buffer_{{params.BUFFER}}  , ST_GeometryFromText(P.shape_wkt ) )  
    WHERE 
        P.pois_state_id = {{params.parametros.pois_state_id}}
        {# AGREGA LISTA DE category_id #}
            AND P.category_id IN (
            {% for id in params.parametros.category_id %}
                {{id}} {% if not loop.last %},{% endif %}
            {% endfor %})
        /*SE DEBERIA FILTRAR POR TIPO COMERCIOS Y SERV ? tipo in 1 y 2*/ 
{%  endif %}






{#
/*
Aca seguir agregando más condiciones, segun vengan en los parametros 
{%  if  params.parametros.substring_id   %}
    AND P.substring_id IN (
    {% for id in params.parametros.substring_id %}
        {{id}} {% if not loop.last %},{% endif %}
    {% endfor %}
{%  endif %} )

*/
#}