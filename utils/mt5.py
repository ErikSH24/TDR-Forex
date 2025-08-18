import MetaTrader5 as mt5

def init_mt5():
    """
    Aquesta funció intenta establir connexió amb el terminal MetaTrader 5 (MT5).
    Si falla la inicialització, mostra el codi d'error, tanca la connexió
    i atura l'execució del programa per evitar errors posteriors.
    """    
    if not mt5.initialize():
        print(f"initialize() failed, error code: {mt5.lat_error()}")
        mt5.shutdown()
        exit()
