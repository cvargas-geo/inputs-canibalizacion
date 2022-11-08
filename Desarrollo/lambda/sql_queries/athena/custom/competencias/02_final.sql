
{# 
    Tabla de pivotes para obtener las sumas y recuentos de sales_area y box_area por subcadena 
    Athena no admite PIVOT por lo que se ocupa esta variante
    https://medium.com/analytics-vidhya/pivot-table-in-aws-athena-or-presto-764661eaa0fd
    https://stackoverflow.com/questions/48013254/how-to-pivot-rows-into-columns-in-aws-athena
    Se reemplazan los espacios de las subcadenas para los alias
    
#}

SELECT
    geo_id ,  
    {% for subcadena in params.lista_subcadenas %}
    reduce(ssa['{{subcadena}}'], 0.0, (s, x) -> s + x, s -> s)     AS "ssa_{{subcadena.replace(" ","_")}}", 
    {% endfor %}
    {% for subcadena in params.lista_subcadenas %} 
    reduce(sba['{{subcadena}}'], 0.0, (s, x) -> s + x, s -> s)     AS "sba_{{subcadena.replace(" ","_")}}", 
    {% endfor %}
    {% for subcadena in params.lista_subcadenas %} 
    COALESCE(cardinality(c['{{subcadena}}'] ),0)                   AS "c_{{subcadena.replace(" ","_")}}" 
    {% if not loop.last %},{% endif %}
    {% endfor %}

FROM (
    SELECT 
        geo_id,
        multimap_agg(subcadena, sales_area) as ssa,
        multimap_agg(subcadena, box_area) as sba,
        multimap_agg(subcadena, geo_id) as c
    FROM {{params.nombre_tabla_anterior}}
    GROUP BY geo_id
)  