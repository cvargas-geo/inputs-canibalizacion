    SELECT
    A.block_id,
    A.total_atractores ,
    CASE
        /* CORTES NATURALES RM */
        WHEN 1155 <= A.total_atractores                                AND A.region in ('METROPOLITANA') THEN 10
        WHEN 361  <= A.total_atractores AND A.total_atractores <= 1154 AND A.region in ('METROPOLITANA') THEN 9
        WHEN 291  <= A.total_atractores AND A.total_atractores <= 360  AND A.region in ('METROPOLITANA') THEN 8
        WHEN 211  <= A.total_atractores AND A.total_atractores <= 290  AND A.region in ('METROPOLITANA') THEN 7
        WHEN 141  <= A.total_atractores AND A.total_atractores <= 210  AND A.region in ('METROPOLITANA') THEN 6
        WHEN 69   <= A.total_atractores AND A.total_atractores <= 140  AND A.region in ('METROPOLITANA') THEN 5
        WHEN 35   <= A.total_atractores AND A.total_atractores <= 68   AND A.region in ('METROPOLITANA') THEN 4
        WHEN 16   <= A.total_atractores AND A.total_atractores <= 34   AND A.region in ('METROPOLITANA') THEN 3
        WHEN 8    <= A.total_atractores AND A.total_atractores <= 15   AND A.region in ('METROPOLITANA') THEN 2
        WHEN 0    <= A.total_atractores AND A.total_atractores <= 7    AND A.region in ('METROPOLITANA') THEN 1

        /* CORTES NATURALES ZONA NORTE */
        WHEN 161 <= A.total_atractores                                AND A.region in ('ARICA Y PARINACOTA','TARAPACA','TARAPACA','ANTOFAGASTA','ATACAMA') THEN 10
        WHEN 76  <= A.total_atractores AND A.total_atractores <= 160  AND A.region in ('ARICA Y PARINACOTA','TARAPACA','TARAPACA','ANTOFAGASTA','ATACAMA') THEN 9
        WHEN 54  <= A.total_atractores AND A.total_atractores <= 75   AND A.region in ('ARICA Y PARINACOTA','TARAPACA','TARAPACA','ANTOFAGASTA','ATACAMA') THEN 8
        WHEN 32  <= A.total_atractores AND A.total_atractores <= 53   AND A.region in ('ARICA Y PARINACOTA','TARAPACA','TARAPACA','ANTOFAGASTA','ATACAMA') THEN 7
        WHEN 24  <= A.total_atractores AND A.total_atractores <= 31   AND A.region in ('ARICA Y PARINACOTA','TARAPACA','TARAPACA','ANTOFAGASTA','ATACAMA') THEN 6
        WHEN 15  <= A.total_atractores AND A.total_atractores <= 23   AND A.region in ('ARICA Y PARINACOTA','TARAPACA','TARAPACA','ANTOFAGASTA','ATACAMA') THEN 5
        WHEN 10  <= A.total_atractores AND A.total_atractores <= 14   AND A.region in ('ARICA Y PARINACOTA','TARAPACA','TARAPACA','ANTOFAGASTA','ATACAMA') THEN 4
        WHEN 4   <= A.total_atractores AND A.total_atractores <= 9    AND A.region in ('ARICA Y PARINACOTA','TARAPACA','TARAPACA','ANTOFAGASTA','ATACAMA') THEN 3
        WHEN 2   <= A.total_atractores AND A.total_atractores <= 3    AND A.region in ('ARICA Y PARINACOTA','TARAPACA','TARAPACA','ANTOFAGASTA','ATACAMA') THEN 2
        WHEN 0   <= A.total_atractores AND A.total_atractores <= 1    AND A.region in ('ARICA Y PARINACOTA','TARAPACA','TARAPACA','ANTOFAGASTA','ATACAMA') THEN 1
        /* CORTES NATURALES CENTRO SUR */
        WHEN 248 <= A.total_atractores                                AND A.region in ('LIBERTADOR GENERAL BERNARDO O HIGGINS','DEL MAULE','DEL BIOBIO','NUBLE') THEN 10
        WHEN 119  <= A.total_atractores AND A.total_atractores <= 247 AND A.region in ('LIBERTADOR GENERAL BERNARDO O HIGGINS','DEL MAULE','DEL BIOBIO','NUBLE') THEN 9
        WHEN 84   <= A.total_atractores AND A.total_atractores <= 118 AND A.region in ('LIBERTADOR GENERAL BERNARDO O HIGGINS','DEL MAULE','DEL BIOBIO','NUBLE') THEN 8
        WHEN 61   <= A.total_atractores AND A.total_atractores <= 83  AND A.region in ('LIBERTADOR GENERAL BERNARDO O HIGGINS','DEL MAULE','DEL BIOBIO','NUBLE') THEN 7
        WHEN 44   <= A.total_atractores AND A.total_atractores <= 60  AND A.region in ('LIBERTADOR GENERAL BERNARDO O HIGGINS','DEL MAULE','DEL BIOBIO','NUBLE') THEN 6
        WHEN 28   <= A.total_atractores AND A.total_atractores <= 43  AND A.region in ('LIBERTADOR GENERAL BERNARDO O HIGGINS','DEL MAULE','DEL BIOBIO','NUBLE') THEN 5
        WHEN 17   <= A.total_atractores AND A.total_atractores <= 27  AND A.region in ('LIBERTADOR GENERAL BERNARDO O HIGGINS','DEL MAULE','DEL BIOBIO','NUBLE') THEN 4
        WHEN 10   <= A.total_atractores AND A.total_atractores <= 16  AND A.region in ('LIBERTADOR GENERAL BERNARDO O HIGGINS','DEL MAULE','DEL BIOBIO','NUBLE') THEN 3
        WHEN 4    <= A.total_atractores AND A.total_atractores <= 9   AND A.region in ('LIBERTADOR GENERAL BERNARDO O HIGGINS','DEL MAULE','DEL BIOBIO','NUBLE') THEN 2
        WHEN 0    <= A.total_atractores AND A.total_atractores <= 3   AND A.region in ('LIBERTADOR GENERAL BERNARDO O HIGGINS','DEL MAULE','DEL BIOBIO','NUBLE') THEN 1

        /* CORTES NATURALES SUR */
        WHEN 141 <= A.total_atractores                                AND A.region in ('LA ARAUCANIA','LOS LAGOS','AISEN DEL GENERAL CARLOS IBANEZ DEL CAMPO','MAGALLANES Y DE LA ANTARTICA CHILENA','DE LOS RIOS') THEN 10
        WHEN 87  <= A.total_atractores AND A.total_atractores <= 140  AND A.region in ('LA ARAUCANIA','LOS LAGOS','AISEN DEL GENERAL CARLOS IBANEZ DEL CAMPO','MAGALLANES Y DE LA ANTARTICA CHILENA','DE LOS RIOS') THEN 9
        WHEN 60  <= A.total_atractores AND A.total_atractores <= 86   AND A.region in ('LA ARAUCANIA','LOS LAGOS','AISEN DEL GENERAL CARLOS IBANEZ DEL CAMPO','MAGALLANES Y DE LA ANTARTICA CHILENA','DE LOS RIOS') THEN 8
        WHEN 40  <= A.total_atractores AND A.total_atractores <= 59   AND A.region in ('LA ARAUCANIA','LOS LAGOS','AISEN DEL GENERAL CARLOS IBANEZ DEL CAMPO','MAGALLANES Y DE LA ANTARTICA CHILENA','DE LOS RIOS') THEN 7
        WHEN 26  <= A.total_atractores AND A.total_atractores <= 39   AND A.region in ('LA ARAUCANIA','LOS LAGOS','AISEN DEL GENERAL CARLOS IBANEZ DEL CAMPO','MAGALLANES Y DE LA ANTARTICA CHILENA','DE LOS RIOS') THEN 6
        WHEN 15  <= A.total_atractores AND A.total_atractores <= 25   AND A.region in ('LA ARAUCANIA','LOS LAGOS','AISEN DEL GENERAL CARLOS IBANEZ DEL CAMPO','MAGALLANES Y DE LA ANTARTICA CHILENA','DE LOS RIOS') THEN 5
        WHEN 10  <= A.total_atractores AND A.total_atractores <= 14   AND A.region in ('LA ARAUCANIA','LOS LAGOS','AISEN DEL GENERAL CARLOS IBANEZ DEL CAMPO','MAGALLANES Y DE LA ANTARTICA CHILENA','DE LOS RIOS') THEN 4
        WHEN 5   <= A.total_atractores AND A.total_atractores <= 9    AND A.region in ('LA ARAUCANIA','LOS LAGOS','AISEN DEL GENERAL CARLOS IBANEZ DEL CAMPO','MAGALLANES Y DE LA ANTARTICA CHILENA','DE LOS RIOS') THEN 3
        WHEN 3   <= A.total_atractores AND A.total_atractores <= 4    AND A.region in ('LA ARAUCANIA','LOS LAGOS','AISEN DEL GENERAL CARLOS IBANEZ DEL CAMPO','MAGALLANES Y DE LA ANTARTICA CHILENA','DE LOS RIOS') THEN 2
        WHEN 0   <= A.total_atractores AND A.total_atractores <= 2    AND A.region in ('LA ARAUCANIA','LOS LAGOS','AISEN DEL GENERAL CARLOS IBANEZ DEL CAMPO','MAGALLANES Y DE LA ANTARTICA CHILENA','DE LOS RIOS') THEN 1

        /* CORTES ZONA CENTRO  */
        WHEN 192 <= A.total_atractores                                AND A.region in ('VALPARAISO','COQUIMBO') THEN 10
        WHEN 97  <= A.total_atractores AND A.total_atractores <= 191  AND A.region in ('VALPARAISO','COQUIMBO') THEN 9
        WHEN 72  <= A.total_atractores AND A.total_atractores <= 96  AND A.region in ('VALPARAISO','COQUIMBO') THEN 8
        WHEN 53  <= A.total_atractores AND A.total_atractores <= 71  AND A.region in ('VALPARAISO','COQUIMBO') THEN 7
        WHEN 37  <= A.total_atractores AND A.total_atractores <= 52  AND A.region in ('VALPARAISO','COQUIMBO') THEN 6
        WHEN 24  <= A.total_atractores AND A.total_atractores <= 36  AND A.region in ('VALPARAISO','COQUIMBO') THEN 5
        WHEN 13  <= A.total_atractores AND A.total_atractores <= 23  AND A.region in ('VALPARAISO','COQUIMBO') THEN 4
        WHEN 5   <= A.total_atractores AND A.total_atractores <= 12  AND A.region in ('VALPARAISO','COQUIMBO') THEN 3
        WHEN 3   <= A.total_atractores AND A.total_atractores <= 4  AND A.region in ('VALPARAISO','COQUIMBO') THEN 2
        WHEN 0   <= A.total_atractores AND A.total_atractores <= 2  AND A.region in ('VALPARAISO','COQUIMBO') THEN 1

        ELSE 1 END AS categoria,
    CASE
        /* PONDERADORES RM*/
        WHEN 1155 <= A.total_atractores                                AND A.region in ('METROPOLITANA') THEN 1.0
        WHEN 361  <= A.total_atractores AND A.total_atractores <= 1154 AND A.region in ('METROPOLITANA') THEN 0.9
        WHEN 291  <= A.total_atractores AND A.total_atractores <= 360  AND A.region in ('METROPOLITANA') THEN 0.8
        WHEN 211  <= A.total_atractores AND A.total_atractores <= 290  AND A.region in ('METROPOLITANA') THEN 0.7
        WHEN 141  <= A.total_atractores AND A.total_atractores <= 210  AND A.region in ('METROPOLITANA') THEN 0.65
        WHEN 69   <= A.total_atractores AND A.total_atractores <= 140  AND A.region in ('METROPOLITANA') THEN 0.6
        WHEN 35   <= A.total_atractores AND A.total_atractores <= 68   AND A.region in ('METROPOLITANA') THEN 0.55
        WHEN 16   <= A.total_atractores AND A.total_atractores <= 34   AND A.region in ('METROPOLITANA') THEN 0.5
        WHEN 8    <= A.total_atractores AND A.total_atractores <= 15   AND A.region in ('METROPOLITANA') THEN 0.45
        WHEN 0    <= A.total_atractores AND A.total_atractores <= 7    AND A.region in ('METROPOLITANA') THEN 0.4

        /* PONDERADORES ZONA NORTE */
        WHEN 161 <= A.total_atractores                                AND A.region in ('ARICA Y PARINACOTA','TARAPACA','TARAPACA','ANTOFAGASTA','ATACAMA') THEN 1.0
        WHEN 76  <= A.total_atractores AND A.total_atractores <= 160  AND A.region in ('ARICA Y PARINACOTA','TARAPACA','TARAPACA','ANTOFAGASTA','ATACAMA') THEN 0.9
        WHEN 54  <= A.total_atractores AND A.total_atractores <= 75   AND A.region in ('ARICA Y PARINACOTA','TARAPACA','TARAPACA','ANTOFAGASTA','ATACAMA') THEN 0.8
        WHEN 32  <= A.total_atractores AND A.total_atractores <= 53   AND A.region in ('ARICA Y PARINACOTA','TARAPACA','TARAPACA','ANTOFAGASTA','ATACAMA') THEN 0.7
        WHEN 24  <= A.total_atractores AND A.total_atractores <= 31   AND A.region in ('ARICA Y PARINACOTA','TARAPACA','TARAPACA','ANTOFAGASTA','ATACAMA') THEN 0.65
        WHEN 15  <= A.total_atractores AND A.total_atractores <= 23   AND A.region in ('ARICA Y PARINACOTA','TARAPACA','TARAPACA','ANTOFAGASTA','ATACAMA') THEN 0.6
        WHEN 10  <= A.total_atractores AND A.total_atractores <= 14   AND A.region in ('ARICA Y PARINACOTA','TARAPACA','TARAPACA','ANTOFAGASTA','ATACAMA') THEN 0.55
        WHEN 4   <= A.total_atractores AND A.total_atractores <= 9    AND A.region in ('ARICA Y PARINACOTA','TARAPACA','TARAPACA','ANTOFAGASTA','ATACAMA') THEN 0.5
        WHEN 2   <= A.total_atractores AND A.total_atractores <= 3    AND A.region in ('ARICA Y PARINACOTA','TARAPACA','TARAPACA','ANTOFAGASTA','ATACAMA') THEN 0.45
        WHEN 0   <= A.total_atractores AND A.total_atractores <= 1    AND A.region in ('ARICA Y PARINACOTA','TARAPACA','TARAPACA','ANTOFAGASTA','ATACAMA') THEN 0.4
        /* PONDERADORES CENTRO SUR */
        WHEN 248 <= A.total_atractores                                AND A.region in ('LIBERTADOR GENERAL BERNARDO O HIGGINS','DEL MAULE','DEL BIOBIO','NUBLE') THEN 1.0
        WHEN 119  <= A.total_atractores AND A.total_atractores <= 247 AND A.region in ('LIBERTADOR GENERAL BERNARDO O HIGGINS','DEL MAULE','DEL BIOBIO','NUBLE') THEN 0.9
        WHEN 84   <= A.total_atractores AND A.total_atractores <= 118 AND A.region in ('LIBERTADOR GENERAL BERNARDO O HIGGINS','DEL MAULE','DEL BIOBIO','NUBLE') THEN 0.8
        WHEN 61   <= A.total_atractores AND A.total_atractores <= 83  AND A.region in ('LIBERTADOR GENERAL BERNARDO O HIGGINS','DEL MAULE','DEL BIOBIO','NUBLE') THEN 0.7
        WHEN 44   <= A.total_atractores AND A.total_atractores <= 60  AND A.region in ('LIBERTADOR GENERAL BERNARDO O HIGGINS','DEL MAULE','DEL BIOBIO','NUBLE') THEN 0.65
        WHEN 28   <= A.total_atractores AND A.total_atractores <= 43  AND A.region in ('LIBERTADOR GENERAL BERNARDO O HIGGINS','DEL MAULE','DEL BIOBIO','NUBLE') THEN 0.6
        WHEN 17   <= A.total_atractores AND A.total_atractores <= 27  AND A.region in ('LIBERTADOR GENERAL BERNARDO O HIGGINS','DEL MAULE','DEL BIOBIO','NUBLE') THEN 0.55
        WHEN 10   <= A.total_atractores AND A.total_atractores <= 16  AND A.region in ('LIBERTADOR GENERAL BERNARDO O HIGGINS','DEL MAULE','DEL BIOBIO','NUBLE') THEN 0.5
        WHEN 4    <= A.total_atractores AND A.total_atractores <= 9   AND A.region in ('LIBERTADOR GENERAL BERNARDO O HIGGINS','DEL MAULE','DEL BIOBIO','NUBLE') THEN 0.45
        WHEN 0    <= A.total_atractores AND A.total_atractores <= 3   AND A.region in ('LIBERTADOR GENERAL BERNARDO O HIGGINS','DEL MAULE','DEL BIOBIO','NUBLE') THEN 0.4

        /* PONDERADORES SUR */
        WHEN 141 <= A.total_atractores                                AND A.region in ('LA ARAUCANIA','LOS LAGOS','AISEN DEL GENERAL CARLOS IBANEZ DEL CAMPO','MAGALLANES Y DE LA ANTARTICA CHILENA','DE LOS RIOS') THEN 1.0
        WHEN 87  <= A.total_atractores AND A.total_atractores <= 140  AND A.region in ('LA ARAUCANIA','LOS LAGOS','AISEN DEL GENERAL CARLOS IBANEZ DEL CAMPO','MAGALLANES Y DE LA ANTARTICA CHILENA','DE LOS RIOS') THEN 0.9
        WHEN 60  <= A.total_atractores AND A.total_atractores <= 86   AND A.region in ('LA ARAUCANIA','LOS LAGOS','AISEN DEL GENERAL CARLOS IBANEZ DEL CAMPO','MAGALLANES Y DE LA ANTARTICA CHILENA','DE LOS RIOS') THEN 0.8
        WHEN 40  <= A.total_atractores AND A.total_atractores <= 59   AND A.region in ('LA ARAUCANIA','LOS LAGOS','AISEN DEL GENERAL CARLOS IBANEZ DEL CAMPO','MAGALLANES Y DE LA ANTARTICA CHILENA','DE LOS RIOS') THEN 0.7
        WHEN 26  <= A.total_atractores AND A.total_atractores <= 39   AND A.region in ('LA ARAUCANIA','LOS LAGOS','AISEN DEL GENERAL CARLOS IBANEZ DEL CAMPO','MAGALLANES Y DE LA ANTARTICA CHILENA','DE LOS RIOS') THEN 0.65
        WHEN 15  <= A.total_atractores AND A.total_atractores <= 25   AND A.region in ('LA ARAUCANIA','LOS LAGOS','AISEN DEL GENERAL CARLOS IBANEZ DEL CAMPO','MAGALLANES Y DE LA ANTARTICA CHILENA','DE LOS RIOS') THEN 0.6
        WHEN 10  <= A.total_atractores AND A.total_atractores <= 14   AND A.region in ('LA ARAUCANIA','LOS LAGOS','AISEN DEL GENERAL CARLOS IBANEZ DEL CAMPO','MAGALLANES Y DE LA ANTARTICA CHILENA','DE LOS RIOS') THEN 0.55
        WHEN 5   <= A.total_atractores AND A.total_atractores <= 9    AND A.region in ('LA ARAUCANIA','LOS LAGOS','AISEN DEL GENERAL CARLOS IBANEZ DEL CAMPO','MAGALLANES Y DE LA ANTARTICA CHILENA','DE LOS RIOS') THEN 0.5
        WHEN 3   <= A.total_atractores AND A.total_atractores <= 4    AND A.region in ('LA ARAUCANIA','LOS LAGOS','AISEN DEL GENERAL CARLOS IBANEZ DEL CAMPO','MAGALLANES Y DE LA ANTARTICA CHILENA','DE LOS RIOS') THEN 0.45
        WHEN 0   <= A.total_atractores AND A.total_atractores <= 2    AND A.region in ('LA ARAUCANIA','LOS LAGOS','AISEN DEL GENERAL CARLOS IBANEZ DEL CAMPO','MAGALLANES Y DE LA ANTARTICA CHILENA','DE LOS RIOS') THEN 0.4

        /* PONDERADORES ZONA CENTRO  */
        WHEN 192 <= A.total_atractores                                AND A.region in ('VALPARAISO','COQUIMBO') THEN 1.0
        WHEN 97  <= A.total_atractores AND A.total_atractores <= 191  AND A.region in ('VALPARAISO','COQUIMBO') THEN 0.9
        WHEN 72  <= A.total_atractores AND A.total_atractores <= 96   AND A.region in ('VALPARAISO','COQUIMBO') THEN 0.8
        WHEN 53  <= A.total_atractores AND A.total_atractores <= 71   AND A.region in ('VALPARAISO','COQUIMBO') THEN 0.7
        WHEN 37  <= A.total_atractores AND A.total_atractores <= 52   AND A.region in ('VALPARAISO','COQUIMBO') THEN 0.65
        WHEN 24  <= A.total_atractores AND A.total_atractores <= 36   AND A.region in ('VALPARAISO','COQUIMBO') THEN 0.6
        WHEN 13  <= A.total_atractores AND A.total_atractores <= 23   AND A.region in ('VALPARAISO','COQUIMBO') THEN 0.55
        WHEN 5   <= A.total_atractores AND A.total_atractores <= 12   AND A.region in ('VALPARAISO','COQUIMBO') THEN 0.5
        WHEN 3   <= A.total_atractores AND A.total_atractores <= 4    AND A.region in ('VALPARAISO','COQUIMBO') THEN 0.45
        WHEN 0   <= A.total_atractores AND A.total_atractores <= 2    AND A.region in ('VALPARAISO','COQUIMBO') THEN 0.4

        ELSE 1 END AS ponderador
    FROM     SALIDA   A 