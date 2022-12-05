EXECUTE format('INSERT INTO customer_chedraui.sp_captura_chedraui_' || _today || ' 
		SELECT l.id::integer,l.nombre, l.sales_area, l.venta, round(( 1 - (SUM(gij2) / SUM(gij1))   )::numeric,6)*100 as canibalizacion
		  FROM (SELECT p.*, s.den_sin, c.den_con , (p.num / s.den_sin) * p.gasto gij1 , (p.num / c.den_con) * p.gasto  gij2
					FROM customer_chedraui.sp_paso_1_chedraui_' || _today || ' p , customer_chedraui.sp_den_sin_chedraui_' || _today || ' s , customer_chedraui.sp_den_con_chedraui_' || _today || ' c
					WHERE p.geo_id = s.geo_id
					AND p.geo_id = c.geo_id ) tabla_final , customer_chedraui.supermercados_venta l
		 WHERE  tabla_final.pois_id = l.id::integer
         AND l.nombre_cadena <> ''CHEDRAUI''
		 AND ST_Intersects(ST_SetSRID( ST_Point( l.longitud, l.latitud), 4326), $1)
		 GROUP BY l.id::integer,l.nombre, l.sales_area, l.venta') using _shape;