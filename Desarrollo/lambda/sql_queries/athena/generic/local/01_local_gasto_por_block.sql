/*
blocks con gastos y el centroide
*/
    select
        b.id,
        b.block_id,
        b.administrative_area_level_1,
        b.administrative_area_level_2,
        b.administrative_area_level_3,
        b.recoba_id,
        ( 
            g."seis" + g."cinco" + g."cuatro" + g."tres" + g."dos" + g."uno"

            ) as gasto,
        b.longitud,
        b.latitud,
        b.centroid_wkt as shape
    from  {{db}}_countries.country_{{schema}}_view_blocks b
    LEFT JOIN
    (
            select
                a.id,
                a.block_id,

       {% for index_b in  range(params.max_gse)  %}
        G{{ index }}.{{ params.lista_canastas[index].lower() }}_gasto_gse{{ index_b + 1 }} ,
            sum(
                case
                    when b.gse_id = {{ index }} then a.hog_gse{{ index }} * b.monto
                end
            ) as "{{ index }}" {% if not loop.last %},{% endif %} {% if loop.last %}){% endif %}
        {% endfor %}


            from {{db}}_countries.country_{{schema}}_view_blocks a
            left join {{db}}_countries.country_{{schema}}_canastas_total b on a.recoba_id = b.id_recoba
            where b.canasta_categoria_id IN (
                {% for id in canasta_categoria_id %}
                    {{id}} {% if not loop.last %},{% endif %}
                {% endfor %})
            AND a.shape_wkt LIKE '%POLYGON((%))'
            AND ST_IsValid(ST_GeometryFromText(a.shape_wkt))
            group by
                a.id,
                a.block_id
        ) g on b.id = g.id and g.block_id =  b.block_id