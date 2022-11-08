/*
PRINCIPALES CAMBIOS
SE USA UNA ESTIMACION DE BUFFER EN FUNCION DE LA LATITUD
EL CALCULO DEL NUM SE REALIZA DIRECTAMENTE
EL SPCANIBALIZACION_CREATE_TABLE SE UNIFICA, LISTA POIS Y TABLA PRECALCULO ,  ID (LEFT)

SE filtran las tablas por like '%POLYGON%' PUES EL DMS TRUNCA LOS SHAPES GRANDES

Rendimento en athena con MX
20 MIN CON BUFFER DE BUSQUEDA DE 20 KM
26 MIN CON BUFFER DE BUSQUEDA DE 30 KM
time out CON BUFFER DE BUSQUEDA DE SOBRE 33 KM

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
            /* ST_Distance( ST_GeometryFromText(p.shape_wkt), ST_GeometryFromText(b.shape) ) * 111139.0 as distancia , */

            POWER(
                ST_Distance(
                    ST_GeometryFromText(p.shape_wkt),
                    ST_GeometryFromText(b.shape)
                ) * 111139.0,
                {{distance_factor}}
            )
            *
            POWER(
                p.sales_area, {{surface_factor}}
            ) AS num
        FROM {{db}}_countries.country_{{schema}}_pois_comercios_servicios_view   p
        INNER JOIN {{db}}_{{project_db}}.{{tabla_anterior_gastos}} b
        /*  BUSQUEDA POR BUFFER EN METROS */
        ON
        ST_intersects(
            ST_GeometryFromText(p.shape_wkt),
            /*Pseudo buffer_search no es 100% circular , se alarga en los polos ,  pero es una buena aproximacion pues respeta el radio
            Daniel artigas aprobo esta solucion*/
            ST_Buffer(
                ST_GeometryFromText( b.shape  ) ,
                {{buffer_search|int}}.0 * 360.0 / (2.0 * pi() * cos( radians(ST_Y(b.shape )) )* 6400000.0)
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