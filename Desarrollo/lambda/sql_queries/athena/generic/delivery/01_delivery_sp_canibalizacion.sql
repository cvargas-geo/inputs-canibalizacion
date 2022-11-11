/*


		update customer_gastronomia_negocios.local_manzana_doggis set np = 0;
		update customer_gastronomia_negocios.local_manzana_doggis set np = 1 where st_contains(ST_GeomFromText(_polygon,4326), st_centroid(shape));
EXECUTE format('
		INSERT INTO customer_gastronomia_negocios.sp_canibalizacion_doggis_reparto_' || _today || ' 
		select a.id_pois as pois_id, 
		l.local_,
		l.direccion,
		l.region1,
		l.comuna,
		l.venta_total_ultimos_12_meses::float as venta_mes, 
		l.delivery_ultimos_12_meses::float as venta_delivery,
		l.counter_ultimos_12_meses::float as venta_local,
		(sum(gasto_comida_rapida*np) / sum(gasto_comida_rapida))*100 as canibalizacion 
		from customer_gastronomia_negocios.local_manzana_doggis a, 
			customer_gastronomia_negocios.reparto_gyn_idpois b, 
			customer_gastronomia_negocios.locales_gyn_idpois l
		where a.id_pois = b.pois and l.pois::int=a.id_pois and l.marca_gestion = ''Doggis''
		  and a.id_pois in (
			select pois 
			from customer_gastronomia_negocios.reparto_gyn_idpois 
				where ST_3DIntersects(shape::geometry,ST_GeomFromText($1,4326)::geometry) and pois!=0)
		group by a.id_pois,l.local_,l.direccion,l.region1,l.comuna,l.venta_total_ultimos_12_meses,l.delivery_ultimos_12_meses,l.counter_ultimos_12_meses;	


DE CARA A LA SALIDA FINAL

  select pois_id as id,
  nombre as nombre,
  direccion as direccion,
  region as region,
  comuna as comuna,
  venta_mes as venta_mensual,
  venta_delivery as venta_delivery,
  venta_local as venta_local,
  canibalizacion::float as canibalizacion
  from customer_gastronomia_negocios.sp_canibalizacion_doggis_reparto_%s 
  where canibalizacion > 0.00
  order by canibalizacion desc

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
        b.centroid_wkt as shape
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