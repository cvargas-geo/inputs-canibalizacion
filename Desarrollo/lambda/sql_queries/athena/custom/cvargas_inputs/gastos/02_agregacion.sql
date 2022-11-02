{# Todos los -1 para el control de la ultima coma #}
/*change*/
WITH BASE AS (
	SELECT 
		bs.geo_id, 
        {% for index_b in  range(params.max_gse -1 )  %} 
		CASE 
			WHEN il.name = 'HOG_GSE{{index_b + 1 }}'
				THEN bs.gse{{index_b + 1 }} * g.monto
			END AS gasto_gse{{index_b + 1 }}, 
			{% set last_index = index_b %}
        {% endfor %}
       
		CASE 
			WHEN il.name = 'HOG_GSE{{range(params.max_gse)| length  }}'
				THEN bs.gse{{range(params.max_gse )| length  }} * g.monto
			END AS gasto_gse{{range(params.max_gse)| length  }}
		 

	FROM  prod_inputs_estudios.{{params.report_name}}_{{params.COUNTRY}}_gastos_intersect_blocks_buffers_b{{params.BUFFER}}  bs 
	INNER JOIN  prod_countries.country_{{params.COUNTRY}}_view_gastos  AS g ON g.id_recoba = bs.recoba_id 
	INNER JOIN  prod_countries.country_{{params.COUNTRY}}_income_levels AS il ON il.id = g.id_gse 
	WHERE g.nombre in ( '{{ params.CANASTA.upper() }}') /* nota iterar por canasta y agregar todas las columas con nuevos nombres  */

)

SELECT
    	geo_id ,  
	{% for index_b in  range(params.max_gse   )  %}  
		SUM(gasto_gse{{index_b + 1 }})  {{ params.CANASTA }}_gasto_gse{{index_b + 1 }},
	{% endfor %}

	{% for index_b in  range(params.max_gse - 1  )  %}  
		SUM(gasto_gse{{index_b + 1 }})+
	{% endfor %}
		SUM(gasto_gse{{range(params.max_gse)| length  }})  
 	    AS {{ params.CANASTA }}_gasto_gse_total

FROM BASE 
GROUP BY geo_id