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
        {% for index in  range(max_gse)  %}
                g."{{ index+1 }}" {% if not loop.last %}+{% endif %} {% if loop.last %}{% endif %}
        {% endfor %}
        ) as gasto,
        b.longitud,
        b.latitud,
        b.centroid_wkt as shape_wkt
    from  {{db}}_countries.country_{{schema}}_view_blocks b
    LEFT JOIN
    (
            select
                a.id,
                a.block_id,
        {% for index in  range(max_gse)  %}
            sum(
                case
                    when b.gse_id = {{ index+1 }} then a.hog_gse{{ index+1 }} * b.monto
                end
            ) as "{{ index+1 }}" {% if not loop.last %},{% endif %} {% if loop.last %}{% endif %}
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