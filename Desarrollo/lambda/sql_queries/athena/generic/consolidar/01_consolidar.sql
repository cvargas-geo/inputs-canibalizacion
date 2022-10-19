{# 
ETLS:
DEMOGRAFICO:    OBLICATORIO
GASTOS:         OPCIONAL
COMPETENCIAS:   OPCIONAL 
*** no ayuda cuando hay muchas condicionales entre los etls , se hace fix para remover las 2 ultimas comas que sobran
Para SALTAR la ultima coma ocupar: {% if not loop.last %},{% endif %} 
#}
SELECT 
    1 as header,
    {% for columna in params.columnas_demografico %}
    '{{ columna }}'  AS  {{ columna }} ,
    {% endfor %}  
    
    
    {# GASTOS  #}
    {%  if  params.parametros['gastos'][params.COUNTRY].id_gastos is defined   %}
    {% for columna in params.columnas_gastos %}
    '{{ columna }}'  AS  {{ columna }} ,
    {% endfor %}  
    {%  endif %}


    {# COMPETENCIAS , 2 CASOS DESDE EL FRONT   #}
    {%  if  params.parametros['competencias'][params.COUNTRY].substring_id is defined or 
            params.parametros['competencias'][params.COUNTRY].category_id is defined
    %}
    {% for columna in params.columnas_competencias %}
    '{{ columna }}'  AS  {{ columna }} ,
    {% endfor %}
    {%  endif %}
   /*split_for_comma*/
UNION ALL 

SELECT 
    0 as header,
    
    {# DEMOGRAFICO #}
    {% for columna in params.columnas_demografico %}
    CAST( COALESCE( D.{{ columna }} , 0 ) AS VARCHAR) AS  "{{ columna }}" ,
    {% endfor %}
    
    
    {# GASTOS  #}
    {%  if  params.parametros['gastos'][params.COUNTRY].id_gastos is defined   %}
    {% for columna in params.columnas_gastos %}
    CAST( COALESCE( G.{{ columna }} , 0 ) AS VARCHAR) AS  "{{ columna }}",
    {% endfor %}
    {%  endif %}
 
    {# COMPETENCIAS , 2 CASOS DESDE EL FRONT   #}
    {%  if  params.parametros['competencias'][params.COUNTRY].substring_id is defined or params.parametros['competencias'][params.COUNTRY].category_id is defined
    %}
    {% for columna in params.columnas_competencias %}
    CAST( COALESCE( C.{{ columna }} , 0 ) AS VARCHAR) AS  "{{ columna }}",
    {% endfor %}
    {%  endif %}
/*split_for_comma*/
/*DEMOGRAFICO*/
FROM {{params.TARGET_DB}}.{{params.CUSTOMER_NAME}}_{{params.COUNTRY}}_demografico_final_b{{params.BUFFER}} D

{# AGREGAR O NO GASTOS  #}
{%  if  params.parametros['gastos'][params.COUNTRY].id_gastos is defined   %}

/*GASTOS*/ 
LEFT JOIN {{params.TARGET_DB}}.{{params.CUSTOMER_NAME}}_{{params.COUNTRY}}_gastos_final_b{{params.BUFFER}} G  ON G.geo_id = D.geo_id

{%  endif %}

{# AGREGAR O NO COMPETENCIAS , 2 CASOS DESDE EL FRONT   #}
{%  if  params.parametros['competencias'][params.COUNTRY].substring_id is defined or 
        params.parametros['competencias'][params.COUNTRY].category_id is defined
%}

/*COMPETENCIAS*/ 
LEFT JOIN {{params.TARGET_DB}}.{{params.CUSTOMER_NAME}}_{{params.COUNTRY}}_competencias_final_b{{params.BUFFER}} C  ON C.geo_id = D.geo_id

{%  endif %}


 

{# OPCION DE PUNTOS A EVALUAR DESDE PLATAFORMA AGREGA LISTA DE block_id #}
    {%  if  params.parametros['demografico'][params.COUNTRY].block_id is defined   %}
        WHERE D.geo_id IN (
        {% for id in params.parametros['demografico'][params.COUNTRY].block_id %}
            {{id}} {% if not loop.last %},{% endif %} {% if loop.last %}){% endif %}
        {% endfor %}
    {%  endif %}