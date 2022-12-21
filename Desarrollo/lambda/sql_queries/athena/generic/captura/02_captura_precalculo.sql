/*
Si aplica centroide a b.shape_wkt

*/

WITH
    /*INICIO DEL SP_CREATE_TABLE_PRECALCULO*/
    TABLA_PRECALCULO AS (
        select
            b.block_id  ,
            p.id as pois_id,
            p.sales_area as superficie,
            b.gasto as gasto,
			/*distancia del local al local con gastos*/
            /* ST_Distance( ST_GeometryFromText(p.shape_wkt), ST_GeometryFromText(b.shape_wkt) ) * 111139.0 as distancia , */

            POWER(
                ST_Distance(
                    ST_GeometryFromText(p.shape_wkt),
                    st_centroid(ST_GeometryFromText(b.shape_wkt))
                ) * 111139.0,
                {{distance_factor}}
            )
            *
            POWER(
                p.sales_area, {{surface_factor}}
            ) AS num,
            b.shape_wkt  as shape
        FROM {{db}}_countries.country_{{schema}}_pois_comercios_servicios_view   p
        INNER JOIN {{db}}_{{project_db}}.{{tabla_anterior_gastos}} b
        /*  BUSQUEDA POR BUFFER EN METROS */
        ON
        ST_intersects(
            ST_GeometryFromText(p.shape_wkt),
            /*Pseudo buffer_search no es 100% circular , se alarga en los polos ,  pero es una buena aproximacion pues respeta el radio
            Daniel artigas aprobo esta solucion*/
            ST_Buffer(
                st_centroid(ST_GeometryFromText( b.shape_wkt  )) ,
                {{buffer_search|int}}.0 * 360.0 / (2.0 * pi() * cos( radians(ST_Y(st_centroid(ST_GeometryFromText(b.shape_wkt)) )) )* 6400000.0)
            )
        )
        WHERE p.sales_area > 0 AND b.gasto > 0
    {%  if  pois_category_id is defined    %}
        AND p.category_id IN (
                {% for id in pois_category_id %}
                    {{id}} {% if not loop.last %},{% endif %}
                {% endfor %})
    {%  endif %}
    )

    SELECT * FROM TABLA_PRECALCULO