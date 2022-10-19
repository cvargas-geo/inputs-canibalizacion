/*PEOR CASO CHEDRAUI
GATOS 34 35 37 

PRINCIPALES CAMBIOS 
-- SE USA UNA ESTIMACION DE BUFFER EN FUNCION DE LA LATITUD
-- EL CALCULO DEL NUM SE REALIZA DIRECTAMENTE 
-- EL SPCANIBALIZACION_CREATE_TABLE SE SEPARA EN 2 CTE , LISTA POIS Y TABLA PRECALCULO , UNIENOD LA PRIMERA CON LA ULTIMA POR SUS ID (LEFT) 
-- TODO 
-- UNIFICAR EL CALCULO DEL GASTO EN UNA SOLA OP SUM 

ojo 

SE filtran las tablas por like '%POLYGON%' PUES EL DMS TRUNCA LOS SHAPES GRANDES 
*/



WITH
    TABLA_GASTOS AS (
    /*blocks con gastos y el centroide*/
        select 
            z.id,
        	z.block_id  ,--as geo_id
        	z.administrative_area_level_1,
        	z.administrative_area_level_2,
        	z.administrative_area_level_3,
        	z.recoba_id,
        	(
        		z."seis" + z."cinco" + z."cuatro" + z."tres" + z."dos" + z."uno"
        	) as gasto,
        	z.longitud,
        	z.latitud,
        	z.shape
        -- 	z.buffer_1500
        	
        from (
        		select 
        		    a.id,
        			a.block_id,
        			a.administrative_area_level_1,
        			a.administrative_area_level_2,
        			a.administrative_area_level_3,
        			a.recoba_id,
        			a.longitud,
        			a.latitud,
        -- 			ST_AsText(st_centroid(ST_GeometryFromText(a.shape_wkt))) as shape,
        			a.centroid_wkt as shape ,
        			a.total_households,
        -- 			vbb.buffer_1500,
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
        		 
        		from prod_countries.country_mx_view_blocks a
        	    left join prod_countries.country_mx_canastas_total b on a.recoba_id = b.id_recoba
        	   -- left join prod_countries.country_mx_view_blocks_buffers vbb on vbb.block_id = a.block_id 
        	    
        		where b.canasta_categoria_id = 34  -- 20 tomado de colombia ni idea cual era para gyn doggys
        		AND a.shape_wkt  LIKE '%POLYGON((%))' --para poligonos truncados por ser grandes 
        -- 		AND vbb.buffer_1500  LIKE '%POLYGON((%))' --para poligonos truncados por ser grandes 
        		AND ST_IsValid(ST_GeometryFromText(a.shape_wkt))
        -- 		AND ST_IsValid(ST_GeometryFromText(vbb.buffer_1500))
        		group by
        		    a.id,
        			a.block_id,
        			a.administrative_area_level_1,
        			a.administrative_area_level_2,
        			a.administrative_area_level_3,
        			a.recoba_id,
        			a.longitud,
        			a.latitud,
        -- 			ST_AsText(st_centroid(ST_GeometryFromText(a.shape_wkt))),
        			a.centroid_wkt,
        -- 			st_centroid(a.shape_wkt) as shape,
        			a.total_households --628mb
        -- 			vbb.buffer_1500
        -- 		    1,
        -- 			2,
        -- 			3,
        -- 			4,
        -- 			5,
        -- 			6,
        -- 			7,
        -- 			8,
        -- 			9
        --  	    10
        	) z
    )
    -- SELECT COUNT(*) FROM TABLA_GASTOS --241211
    

/*INICIO DEL SP_CREATE_TABLE_PRECALCULO*/
    ,LISTA_POIS AS (
        select
            id
        from country_mx_pois_comercios_servicios_view p
        where p.category_id in (10008)
        and p.pois_state_id = 1
        -- and subcadena in ('ACHOCLONADOS','BUFALO BEEF','BUFFET EXPRESS','BURGER KING','CARLS JR',
        -- 	  'CHINA WOK','DELICIAS','DOGGIS','DOMINO','DOMINOS PIZZA','DONY DONER',
        -- 	  'FRUTOS','FUENTE NICANOR','JUAN MAESTRO','JUST BURGER','KFC',
        -- 	  'LITTLE CAESARS PIZZA','LOMITON','LOVDO','MAMMA MIA',
        -- 	  'MAMMATERRA','MCDONALDS','MELT','NIU SUSHI','PAGODA',
        -- 	  'PAPA JOHNS','PEDRO JUAN Y DIEGO','PIZZA HUT','PIZZA PIZZA',
        -- 	  'PLATON','POLLO STOP','ROOF BURGER','SUBWAY','SUSHI BLUES',
        -- 	  'TACO BELL','TARRAGONA','TELEPIZZA','TOMMY BEANS','WENDYS')
        --   order by p.id asc -- ESTO ESTA DE MAS
    )
    -- SELECT * FROM LISTA_POIS
    , TABLA_PRECALCULO AS (
        select
            b.block_id as geo_id,
            p.id as pois_id,
            p.sales_area as superficie,
            b.gasto as gasto,
			/*distancia del local al local con gastos*/
            ST_Distance( ST_GeometryFromText(p.shape_wkt), ST_GeometryFromText(b.shape) ) * 111139.0 as distancia , 
            
            POWER(
                ST_Distance( ST_GeometryFromText(p.shape_wkt), ST_GeometryFromText(b.shape) ) * 111139.0, -1.95	) 
                * 
            POWER(p.sales_area, 1.1) AS num
            
        from LISTA_POIS a
        left join (SELECT id , sales_area  ,  shape_wkt from  country_mx_pois_comercios_servicios_view  where   ST_IsValid(ST_GeometryFromText(shape_wkt))  ) p on a.id = p.id
        -- left join country_mx_pois_comercios_servicios_view  p on a.id=p.id
        INNER join TABLA_GASTOS b 
        -- inner join customer_gastronomia_negocios.gasto_crap b 
        -- on ST_intersects(p.shape, b.buffer_1500 )  
        -- on ST_Distance(p.shape, b.shape, true) between 1 and 1500
        --BUSQUEDA LOS 20  KM 
        ON 
        ST_intersects(
            ST_GeometryFromText(p.shape_wkt), 
            -- ST_GeometryFromText(b.buffer_1500) 
            /*Pseudo buffer no es 100% circular , se alarga en los polos ,  pero es una buena aproximacion pues respeta el radio*/
            ST_Buffer(
                ST_GeometryFromText( b.shape  ) ,
                    20000.0 * 360.0 / (2.0 * pi() * cos( radians(ST_Y(b.shape )) )* 6400000.0)    
            )   
        )  
        where
        --   p.shape_wkt  LIKE 'POINT(%)' AND 
        -- ST_intersects(p.shape, b.buffer_1500 )  
        -- and 
        p.sales_area > 0 and b.gasto > 0 
        -- and p.id = $1 -- pasa a lef join con A subselect 
    )
    
     
    

    SELECT  *   FROM TABLA_PRECALCULO  --173183501 1 GB  
    
    --ORDER BY gasto DESC  limit 1000