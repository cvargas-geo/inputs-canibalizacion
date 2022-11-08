    SELECT
    A.block_id,
    A.total_atractores ,
    CASE
        /* CORTES NATURALES MX*/
        WHEN 748  <= A.total_atractores                               THEN 10
        WHEN 265  <= A.total_atractores AND A.total_atractores <= 747 THEN 9
        WHEN 94   <= A.total_atractores AND A.total_atractores <= 264 THEN 8
        WHEN 64   <= A.total_atractores AND A.total_atractores <= 93  THEN 7
        WHEN 26   <= A.total_atractores AND A.total_atractores <= 63  THEN 6
        WHEN 11   <= A.total_atractores AND A.total_atractores <= 25  THEN 5
        WHEN 6    <= A.total_atractores AND A.total_atractores <= 10  THEN 4
        WHEN 4    <= A.total_atractores AND A.total_atractores <= 5   THEN 3
        WHEN 2    <= A.total_atractores AND A.total_atractores <= 3   THEN 2
        WHEN 0    <= A.total_atractores AND A.total_atractores <= 1   THEN 1
        ELSE 1 END AS categoria,
    CASE
        /* PONDERADORES MX*/ 
        WHEN 748  <= A.total_atractores                               THEN 1.0
        WHEN 265  <= A.total_atractores AND A.total_atractores <= 747 THEN 0.9
        WHEN 94   <= A.total_atractores AND A.total_atractores <= 264 THEN 0.8
        WHEN 64   <= A.total_atractores AND A.total_atractores <= 93  THEN 0.7
        WHEN 26   <= A.total_atractores AND A.total_atractores <= 63  THEN 0.65
        WHEN 11   <= A.total_atractores AND A.total_atractores <= 25  THEN 0.6
        WHEN 6    <= A.total_atractores AND A.total_atractores <= 10  THEN 0.55
        WHEN 4    <= A.total_atractores AND A.total_atractores <= 5   THEN 0.5
        WHEN 2    <= A.total_atractores AND A.total_atractores <= 3   THEN 0.45
        WHEN 0    <= A.total_atractores AND A.total_atractores <= 1   THEN 0.4
        ELSE 1 END AS ponderador
    FROM     SALIDA   A 