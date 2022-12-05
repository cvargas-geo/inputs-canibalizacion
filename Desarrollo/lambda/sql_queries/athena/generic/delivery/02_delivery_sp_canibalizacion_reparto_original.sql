
with
    local_manzana as (
        select
        	bg.block_id as geo_id,
        	z.pois as id_pois,
        	CAST(bg.gasto AS DECIMAL(22,5)) as gasto,
        	ST_GeometryFromText(b.shape_wkt) as shape,
           /*  0 as np */
            case when
                ST_intersects(
                    ST_Centroid(ST_GeometryFromText(b.shape_wkt))  ,
                    /* ST_GeometryFromText('POLYGON((-70.5943113357452 -33.4763075468465,-70.5943352310499 -33.4764831778805,-70.5943995539812 -33.4766515275262,-70.5945018328414 -33.4768061261358,-70.5946381372352 -33.4769410324808,-70.5948032290932 -33.4770510620836,-70.5949907639637 -33.4771319864705,-70.5951935348359 -33.4771806956833,-70.5954037491242 -33.4771953178066,-70.5956133281658 -33.4771752909104,-70.5958142177162 -33.4771213846475,-70.5959986975062 -33.4770356706707,-70.5961596779572 -33.4769214430107,-70.5962909726485 -33.4767830914747,-70.596387536063 -33.4766259329326,-70.5964456574739 -33.4764560069781,-70.5964631035224 -33.4762798438185,-70.5964392040125 -33.4761042133147,-70.5963748776282 -33.4759358648184,-70.5962725965929 -33.4757812678024,-70.5961362916331 -33.4756463632528,-70.5959712009042 -33.4755363353734,-70.5957836686862 -33.475455412376,-70.5955809015859 -33.4754067040069,-70.5953706916148 -33.4753920820534,-70.5951611167784 -33.4754121084193,-70.5949602306809 -33.4754660135328,-70.594775753066 -33.4755517259159,-70.594614773181 -33.4756659517807,-70.5944834773606 -33.4758043015932,-70.5943869112937 -33.4759614587459,-70.5943287861109 -33.4761313838565,-70.5943113357452 -33.4763075468465))') )  */
                    ST_GeometryFromText('{{cannibalization_shape}}') )
                then   1.0 else 0.0 end as np
        from {{db}}_cannibalization.{{report_name}}_{{schema}}_zonas_reparto z
        join {{db}}_cannibalization.{{report_name}}_{{schema}}_{{ETL_NAME}}_gasto_por_block bg on 
                ST_intersects(ST_GeometryFromText(bg.shape), ST_GeometryFromText(z.shape))
        join {{db}}_countries.country_{{schema}}_view_blocks b on b.block_id = bg.block_id
        where b.shape_wkt like '%POLYGON((%))'
        and  z.shape like '%POLYGON((%))'
    )
    /* SELECT COUNT(*) FROM locales_manzana */
    
    ,sp_canibalizacion_reparto as (
        /* SE AGREGAL INFO ENVIADA POR EL CLIENTE (PODRIA CAMBIAR)*/
        select
            a.id_pois as pois_id,
    		l.local_  as nombre,
    		l.direccion,
    		l.region1 as region ,
    		l.comuna,
    		l.venta_total_ultimos_12_meses  as venta_mes,
    		l.delivery_ultimos_12_meses  as venta_delivery,
    		l.counter_ultimos_12_meses  as venta_local,
    		(sum(gasto*np) / sum(gasto))*100.0 as canibalizacion
    		from local_manzana a, 
    			{{db}}_cannibalization.{{report_name}}_{{schema}}_zonas_reparto b,
    			{{db}}_cannibalization.{{report_name}}_{{schema}}_locales_propios l
    		where a.id_pois = b.pois and CAST(l.pois as int) = a.id_pois and l.marca_gestion = 'Doggis'
    		  and a.id_pois in (
    			select pois
    			from {{db}}_cannibalization.{{report_name}}_{{schema}}_zonas_reparto
    				where ST_intersects(
    				    ST_GeometryFromText(shape),
    				    /* ST_GeometryFromText('POLYGON((-70.5943113357452 -33.4763075468465,-70.5943352310499 -33.4764831778805,-70.5943995539812 -33.4766515275262,-70.5945018328414 -33.4768061261358,-70.5946381372352 -33.4769410324808,-70.5948032290932 -33.4770510620836,-70.5949907639637 -33.4771319864705,-70.5951935348359 -33.4771806956833,-70.5954037491242 -33.4771953178066,-70.5956133281658 -33.4771752909104,-70.5958142177162 -33.4771213846475,-70.5959986975062 -33.4770356706707,-70.5961596779572 -33.4769214430107,-70.5962909726485 -33.4767830914747,-70.596387536063 -33.4766259329326,-70.5964456574739 -33.4764560069781,-70.5964631035224 -33.4762798438185,-70.5964392040125 -33.4761042133147,-70.5963748776282 -33.4759358648184,-70.5962725965929 -33.4757812678024,-70.5961362916331 -33.4756463632528,-70.5959712009042 -33.4755363353734,-70.5957836686862 -33.475455412376,-70.5955809015859 -33.4754067040069,-70.5953706916148 -33.4753920820534,-70.5951611167784 -33.4754121084193,-70.5949602306809 -33.4754660135328,-70.594775753066 -33.4755517259159,-70.594614773181 -33.4756659517807,-70.5944834773606 -33.4758043015932,-70.5943869112937 -33.4759614587459,-70.5943287861109 -33.4761313838565,-70.5943113357452 -33.4763075468465))') */
    				    ST_GeometryFromText('{{cannibalization_shape}}')
    				    )
    				and pois!=0
    				)
    		group by a.id_pois,l.local_,l.direccion,l.region1,l.comuna,l.venta_total_ultimos_12_meses,l.delivery_ultimos_12_meses,l.counter_ultimos_12_meses
    		order by l.venta_total_ultimos_12_meses desc
    )
    /* SELECT * FROM sp_canni_reparto */

    ,SALIDA AS (
        select
            0 as block_id,
            pois_id as id,
            nombre as nombre,
            direccion as direccion,
            region as region,
            comuna as comuna,
            venta_mes as venta_mensual,
            venta_delivery as venta_delivery,
            venta_local as venta_local,
            canibalizacion
        from sp_canibalizacion_reparto
        where canibalizacion > 0.0
        /* para 1 caso se aplico el siguente filtro */
        /*and venta_delivery > 0*/
        order by canibalizacion desc
    )

    SELECT * FROM SALIDA