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
        ( g."seis" + g."cinco" + g."cuatro" + g."tres" + g."dos" + g."uno" ) as gasto,
        b.longitud,
        b.latitud,
        b.centroid_wkt as shape
    from  {{db}}_countries.country_{{schema}}_view_blocks b
    LEFT JOIN
    (
            select
                a.id,
                a.block_id,
                sum(
                    case
                        when b.gse_id = 1 then a.hog_gse1 * b.monto
                    end
                ) as "seis",
                sum(
                    case
                        when b.gse_id = 2 then a.hog_gse2 * b.monto
                    end
                ) as "cinco",
                sum(
                    case
                        when b.gse_id = 3 then a.hog_gse3 * b.monto
                    end
                ) as "cuatro",
                sum(
                    case
                        when b.gse_id = 4 then a.hog_gse4 * b.monto
                    end
                ) as "tres",
                sum(
                    case
                        when b.gse_id = 5 then a.hog_gse5 * b.monto
                    end
                ) as "dos",
                sum(
                    case
                        when b.gse_id = 6 then a.hog_gse6 * b.monto
                    end
                ) as "uno"

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