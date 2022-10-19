/*change*/
SELECT
    B.block_id as geo_id ,
    P.id as pois_id,
    /* P.categoria,*/
    P.subcadena,
    P.substring_id, 
    P.box_area,
    P.sales_area 
FROM   prod_countries.country_{{params.COUNTRY}}_view_blocks_buffers B 
JOIN   prod_countries.country_{{params.COUNTRY}}_pois_comercios_servicios_view P
ON ST_intersects( B.buffer_{{params.BUFFER}}  , ST_GeometryFromText(P.shape_wkt ) )  
WHERE 
    P.pois_state_id = {{params.parametros.pois_state_id}}

{# AGREGA LISTA DE SUBSTRINGS #}
{%  if  params.parametros.substring_id   %}
    AND P.substring_id IN (
    {% for id in params.parametros.substring_id %}
        {{id}} {% if not loop.last %},{% endif %}
    {% endfor %}
{%  endif %} )


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