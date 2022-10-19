SELECT  
    G0.geo_id,
    {% for index in range( params.lista_canastas| length)  %}
        {% for index_b in  range(params.max_gse)  %}
        G{{ index }}.{{ params.lista_canastas[index].lower() }}_gasto_gse{{ index_b + 1 }} ,
        {% endfor %}
        G{{ index }}.{{ params.lista_canastas[index].lower() }}_gasto_gse_total ,
    {% endfor %}

FROM 
{% for index in range( params.lista_canastas| length)  %}
    {% if index > 0 %}
    FULL OUTER JOIN
    {% endif %}
    {{params.TARGET_DB}}.{{params.CUSTOMER_NAME}}_{{params.COUNTRY}}_gastos_agregacion_{{params.lista_canastas[index].lower()}}_b{{params.BUFFER}} G{{index}} 
    {% if index > 0 %}
    ON G{{ index-1 }}.geo_id = G{{ index }}.geo_id
    {% endif %}
{% endfor %}