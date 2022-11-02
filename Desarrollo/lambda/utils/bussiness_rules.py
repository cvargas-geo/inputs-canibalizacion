

def get_limit_gse_by_country(schema):
    """Dado un pais retorna el numero de indices socioeconommicos
    TODO: implementar una funcion que retorne el limite de indices por pais por sql 
    select count(*) from country_pais.income_levels
    """
    
    if schema in ['cl','pe','rd','pr','pa','cr','ar']:
        return 5
    if schema in ['co']:
        return 6
    if schema in ['mx','gtm']:
        return 7
    if schema in ['uy']:
        return 3