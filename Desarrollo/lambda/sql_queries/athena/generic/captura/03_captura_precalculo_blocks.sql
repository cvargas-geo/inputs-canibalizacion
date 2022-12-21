/*
Primera tabla optimizada

create table customer_little_caesar_pizza_mx.precalculo_lcp_blocks as
select geo_id, sum(num) as num
from customer_little_caesar_pizza_mx.precalculo_little_caesars
group by geo_id
*/

WITH
    PRECALCULO_BLOCKS AS (
        select 
            block_id,
            sum(num) as num ,
            b.shape /* esto estara bien ? */
        /* from customer_little_caesar_pizza_mx.precalculo_little_caesars */
        from {{db}}_{{project_db}}.{{tabla_anterior}} b
        group by block_id
    )

    SELECT * FROM PRECALCULO_BLOCKS