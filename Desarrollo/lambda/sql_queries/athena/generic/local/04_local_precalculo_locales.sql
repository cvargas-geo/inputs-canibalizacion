/*
Segunda tabla optimizada

create table customer_little_caesar_pizza_mx.precalculo_lcp_locales as
select
p.geo_id, p.pois_id, p.num, p.gasto
from customer_little_caesar_pizza_mx.precalculo_little_caesars p
join customer_little_caesar_pizza_mx.locales_propios_lcp l
on l.id_pois::int = p.pois_id

*/

WITH
    PRECALCULO_LOCALES AS (
        select
            p.block_id,
            p.pois_id,
            p.num,
            p.gasto
        /* from customer_little_caesar_pizza_mx.precalculo_little_caesars p */
        from {{db}}_{{project_db}}.{{tabla_anterior}} p
        /*Ojo esta tabla se tiene que traer en el mismo proceso  */
        /* join customer_little_caesar_pizza_mx.locales_propios_lcp l */
        /* join qa_cannibalization.little_caesar_pizza_mx_local_locales_propios l */
        join {{db}}_{{project_db}}.{{report_name}}_{{schema}}_locales_propios l
        on CAST(l.id_pois AS INT ) = p.pois_id
        /* cast l.id_pois */
    )

    SELECT * FROM PRECALCULO_LOCALES