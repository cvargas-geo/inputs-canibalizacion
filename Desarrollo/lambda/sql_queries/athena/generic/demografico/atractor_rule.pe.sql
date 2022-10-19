    SELECT
    A.block_id,
    A.total_atractores ,
    CASE
        /* CORTES NATURALES LIMA - CALLAO */
        WHEN 71 <= A.total_atractores                                AND A.region IN ('LIMA','CALLAO') THEN 10
        WHEN 56  <= A.total_atractores AND A.total_atractores <= 70  AND A.region IN ('LIMA','CALLAO') THEN 9
        WHEN 48  <= A.total_atractores AND A.total_atractores <= 55  AND A.region IN ('LIMA','CALLAO') THEN 8
        WHEN 43  <= A.total_atractores AND A.total_atractores <= 47  AND A.region IN ('LIMA','CALLAO') THEN 7
        WHEN 37  <= A.total_atractores AND A.total_atractores <= 42  AND A.region IN ('LIMA','CALLAO') THEN 6
        WHEN 30  <= A.total_atractores AND A.total_atractores <= 36  AND A.region IN ('LIMA','CALLAO') THEN 5
        WHEN 26  <= A.total_atractores AND A.total_atractores <= 29  AND A.region IN ('LIMA','CALLAO') THEN 4
        WHEN 20  <= A.total_atractores AND A.total_atractores <= 25  AND A.region IN ('LIMA','CALLAO') THEN 3
        WHEN 11  <= A.total_atractores AND A.total_atractores <= 19  AND A.region IN ('LIMA','CALLAO') THEN 2
        WHEN 0   <= A.total_atractores AND A.total_atractores <= 10  AND A.region IN ('LIMA','CALLAO') THEN 1
        /* CORTES NATURALES DEL RESTO DE REGIONES  */
        WHEN 104 <= A.total_atractores                                AND A.region NOT IN ('LIMA','CALLAO') THEN 10
        WHEN 51  <= A.total_atractores AND A.total_atractores <= 103  AND A.region NOT IN ('LIMA','CALLAO') THEN 9
        WHEN 31  <= A.total_atractores AND A.total_atractores <= 50   AND A.region NOT IN ('LIMA','CALLAO') THEN 8
        WHEN 21  <= A.total_atractores AND A.total_atractores <= 30   AND A.region NOT IN ('LIMA','CALLAO') THEN 7
        WHEN 13  <= A.total_atractores AND A.total_atractores <= 20   AND A.region NOT IN ('LIMA','CALLAO') THEN 6
        WHEN 10  <= A.total_atractores AND A.total_atractores <= 12   AND A.region NOT IN ('LIMA','CALLAO') THEN 5
        WHEN 8   <= A.total_atractores AND A.total_atractores <= 9    AND A.region NOT IN ('LIMA','CALLAO') THEN 4
        WHEN 5   <= A.total_atractores AND A.total_atractores <= 7    AND A.region NOT IN ('LIMA','CALLAO') THEN 3
        WHEN 2   <= A.total_atractores AND A.total_atractores <= 4    AND A.region NOT IN ('LIMA','CALLAO') THEN 2
        WHEN 0   <= A.total_atractores AND A.total_atractores <= 1    AND A.region NOT IN ('LIMA','CALLAO') THEN 1

        ELSE 1 END AS categoria,
    CASE
        /* PONDERADORES LIMA - CALLAO */
        WHEN 71 <= A.total_atractores                                AND A.region IN ('LIMA','CALLAO') THEN 1.0
        WHEN 56  <= A.total_atractores AND A.total_atractores <= 70  AND A.region IN ('LIMA','CALLAO') THEN 0.9
        WHEN 48  <= A.total_atractores AND A.total_atractores <= 55  AND A.region IN ('LIMA','CALLAO') THEN 0.8
        WHEN 43  <= A.total_atractores AND A.total_atractores <= 47  AND A.region IN ('LIMA','CALLAO') THEN 0.7
        WHEN 37  <= A.total_atractores AND A.total_atractores <= 42  AND A.region IN ('LIMA','CALLAO') THEN 0.65
        WHEN 30  <= A.total_atractores AND A.total_atractores <= 36  AND A.region IN ('LIMA','CALLAO') THEN 0.6
        WHEN 26  <= A.total_atractores AND A.total_atractores <= 29  AND A.region IN ('LIMA','CALLAO') THEN 0.55
        WHEN 20  <= A.total_atractores AND A.total_atractores <= 25  AND A.region IN ('LIMA','CALLAO') THEN 0.5
        WHEN 11  <= A.total_atractores AND A.total_atractores <= 19  AND A.region IN ('LIMA','CALLAO') THEN 0.45
        WHEN 0   <= A.total_atractores AND A.total_atractores <= 10  AND A.region IN ('LIMA','CALLAO') THEN 0.4
        /* PONDERADORES DEL RESTO DE REGIONES  */
        WHEN 104 <= A.total_atractores                                AND A.region NOT IN ('LIMA','CALLAO') THEN 1.0
        WHEN 51  <= A.total_atractores AND A.total_atractores <= 103  AND A.region NOT IN ('LIMA','CALLAO') THEN 0.9
        WHEN 31  <= A.total_atractores AND A.total_atractores <= 50   AND A.region NOT IN ('LIMA','CALLAO') THEN 0.8
        WHEN 21  <= A.total_atractores AND A.total_atractores <= 30   AND A.region NOT IN ('LIMA','CALLAO') THEN 0.7
        WHEN 13  <= A.total_atractores AND A.total_atractores <= 20   AND A.region NOT IN ('LIMA','CALLAO') THEN 0.65
        WHEN 10  <= A.total_atractores AND A.total_atractores <= 12   AND A.region NOT IN ('LIMA','CALLAO') THEN 0.6
        WHEN 8   <= A.total_atractores AND A.total_atractores <= 9    AND A.region NOT IN ('LIMA','CALLAO') THEN 0.55
        WHEN 5   <= A.total_atractores AND A.total_atractores <= 7    AND A.region NOT IN ('LIMA','CALLAO') THEN 0.5
        WHEN 2   <= A.total_atractores AND A.total_atractores <= 4    AND A.region NOT IN ('LIMA','CALLAO') THEN 0.45
        WHEN 0   <= A.total_atractores AND A.total_atractores <= 1    AND A.region NOT IN ('LIMA','CALLAO') THEN 0.4
        ELSE 1 END AS ponderador
    FROM     SALIDA   A 