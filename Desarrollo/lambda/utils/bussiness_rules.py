

def get_limit_gse_by_country(country_name):
    """Dado un pais retorna el numero de indices socioeconommicos
    TODO: implementar una funcion que retorne el limite de indices por pais por sql 
    select count(*) from country_pais.income_levels
    """
    
    if country_name in ['cl','pe','rd','pr','pa','cr','ar']:
        return 5
    if country_name in ['co']:
        return 6
    if country_name in ['mx','gtm']:
        return 7
    if country_name in ['uy']:
        return 3