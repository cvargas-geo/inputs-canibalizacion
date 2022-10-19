    SELECT
    A.block_id,
    A.total_atractores ,
    CASE
        /* CORTES NATURALES CO*/
        WHEN 111  <= A.total_atractores                               THEN 10
        WHEN 75   <= A.total_atractores AND A.total_atractores <= 110 THEN 9
        WHEN 61   <= A.total_atractores AND A.total_atractores <= 74  THEN 8
        WHEN 47   <= A.total_atractores AND A.total_atractores <= 60  THEN 7
        WHEN 37   <= A.total_atractores AND A.total_atractores <= 46  THEN 6
        WHEN 27   <= A.total_atractores AND A.total_atractores <= 36  THEN 5
        WHEN 19   <= A.total_atractores AND A.total_atractores <= 26  THEN 4
        WHEN 13   <= A.total_atractores AND A.total_atractores <= 18  THEN 3
        WHEN 5    <= A.total_atractores AND A.total_atractores <= 12  THEN 2
        WHEN 0    <= A.total_atractores AND A.total_atractores <= 4   THEN 1

        ELSE 1   END AS categoria,
    CASE
        /* PONDERADORES CO*/ 
        WHEN 111  <= A.total_atractores                               THEN 10
        WHEN 75   <= A.total_atractores AND A.total_atractores <= 110 THEN 9
        WHEN 61   <= A.total_atractores AND A.total_atractores <= 74  THEN 8
        WHEN 47   <= A.total_atractores AND A.total_atractores <= 60  THEN 7
        WHEN 37   <= A.total_atractores AND A.total_atractores <= 46  THEN 6
        WHEN 27   <= A.total_atractores AND A.total_atractores <= 36  THEN 5
        WHEN 19   <= A.total_atractores AND A.total_atractores <= 26  THEN 4
        WHEN 13   <= A.total_atractores AND A.total_atractores <= 18  THEN 3
        WHEN 5    <= A.total_atractores AND A.total_atractores <= 12  THEN 2
        WHEN 0    <= A.total_atractores AND A.total_atractores <= 4   THEN 1

        ELSE 1 END AS ponderador
    FROM     SALIDA   A 