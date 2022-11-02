/*change*/
{# Para la ultima coma ocupar: {% if not loop.last %},{% endif %} #}
SELECT 
    1 as columnas,
    {% for columna in params.columnas_demografico %}
    '{{ columna }}'  AS  {{ columna }} , 
    {% endfor %}  
    {% for columna in params.columnas_gastos %}
    '{{ columna }}'  AS  {{ columna }} , 
    {% endfor %}  
    {% for columna in params.columnas_competencias %}
    '{{ columna }}'  AS  {{ columna }} {% if not loop.last %},{% endif %}
    {% endfor %}
    
UNION ALL 

SELECT 
    0 as columnas,
    {# DEMOGRAFICO #}
    {% for columna in params.columnas_demografico %}
    CAST( COALESCE( D.{{ columna }} , 0 ) AS VARCHAR) AS  "{{ columna }}" , 
    {% endfor %}
    {# GASTOS #}
    {% for columna in params.columnas_gastos %}
    CAST( COALESCE( G.{{ columna }} , 0 ) AS VARCHAR) AS  "{{ columna }}" ,
    {% endfor %}
    {# COMPETENCIAS #}
    {% for columna in params.columnas_competencias %}
    CAST( COALESCE( C.{{ columna }} , 0 ) AS VARCHAR) AS  "{{ columna }}" {% if not loop.last %},{% endif %}
    {% endfor %}

/*DEMOGRAFICO*/
FROM {{params.TARGET_DB}}.{{params.report_name}}_{{params.COUNTRY}}_demografico_final_b{{params.BUFFER}} D
/*GASTOS*/ 
FULL OUTER JOIN {{params.TARGET_DB}}.{{params.report_name}}_{{params.COUNTRY}}_gastos_final_b{{params.BUFFER}} G  ON G.geo_id = D.geo_id
/*COMPETENCIAS*/ 
FULL OUTER JOIN {{params.TARGET_DB}}.{{params.report_name}}_{{params.COUNTRY}}_competencias_final_b{{params.BUFFER}} C  ON C.geo_id = D.geo_id
