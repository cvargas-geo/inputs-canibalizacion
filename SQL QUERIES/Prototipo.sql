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
        	z.shape,
        	z.buffer_1500
        	
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
        			ST_AsText(st_centroid(ST_GeometryFromText(a.shape_wkt))) as shape,
        			a.total_households,
        			vbb.buffer_1500,
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
        		 
        		from prod_countries.country_cl_view_blocks a
        	    left join prod_countries.country_cl_canastas_total b on a.recoba_id = b.id_recoba
        	    left join prod_countries.country_cl_view_blocks_buffers vbb on vbb.block_id = a.block_id 
        	    
        		where b.canasta_categoria_id = 20  -- 20 tomado de colombia ni idea cual era para gyn doggys
        		AND a.shape_wkt  LIKE '%POLYGON((%))'
        		group by
        		    a.id,
        			a.block_id,
        			a.administrative_area_level_1,
        			a.administrative_area_level_2,
        			a.administrative_area_level_3,
        			a.recoba_id,
        			a.longitud,
        			a.latitud,
        			ST_AsText(st_centroid(ST_GeometryFromText(a.shape_wkt))),
        -- 			st_centroid(a.shape_wkt) as shape,
        			a.total_households, --628mb
        			vbb.buffer_1500
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
    
    ,LISTA_POIS AS (
        select
            id
        from country_cl_pois_comercios_servicios_view p
           where p.category_id in (10011, 10004)
        and p.pois_state_id = 1
        and subcadena in ('ACHOCLONADOS','BUFALO BEEF','BUFFET EXPRESS','BURGER KING','CARLS JR',
        	  'CHINA WOK','DELICIAS','DOGGIS','DOMINO','DOMINOS PIZZA','DONY DONER',
        	  'FRUTOS','FUENTE NICANOR','JUAN MAESTRO','JUST BURGER','KFC',
        	  'LITTLE CAESARS PIZZA','LOMITON','LOVDO','MAMMA MIA',
        	  'MAMMATERRA','MCDONALDS','MELT','NIU SUSHI','PAGODA',
        	  'PAPA JOHNS','PEDRO JUAN Y DIEGO','PIZZA HUT','PIZZA PIZZA',
        	  'PLATON','POLLO STOP','ROOF BURGER','SUBWAY','SUSHI BLUES',
        	  'TACO BELL','TARRAGONA','TELEPIZZA','TOMMY BEANS','WENDYS')
        --   order by p.id asc -- ESTO ESTA DE MAS
    )
    -- SELECT * FROM LISTA_POIS
    , TABLA_PRECALCULO AS (
        select
            b.block_id as geo_id,
            p.id as pois_id,
            p.sales_area as superficie,
            b.gasto as gasto
            -- ST_Distance(p.shape, b.shape, true) as distancia
        from LISTA_POIS a
        left JOIN country_cl_pois_comercios_servicios_view p on a.id=p.id
        INNER join TABLA_GASTOS b 
        -- inner join customer_gastronomia_negocios.gasto_crap b 
        -- on ST_intersects(p.shape, b.buffer_1500 )  
        -- on ST_Distance(p.shape, b.shape, true) between 1 and 1500
        ON ST_intersects(p.shape_wkt, b.buffer_1500 )  
        where
        -- ST_intersects(p.shape, b.buffer_1500 )  
        -- and 
        p.sales_area > 0 and b.gasto > 0 
        -- and p.id = $1 -- pasa a lef join con A subselect 
    )

    SELECT * FROM TABLA_PRECALCULO