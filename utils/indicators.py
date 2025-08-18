import pandas as pd

def get_EMAs(data, BarShift=1):
    """
    Calcula múltiples mitjanes mòbils exponencials (EMAs) sobre les dades de tancament,
    amb opció de desplaçament temporal.

    Args:
        data (pd.DataFrame): DataFrame amb columnes ['time', 'close'] com a mínim.
        BarShift (int, optional): Nombre de períodes a desplaçar les EMAs cap al passat. 
                                Default=1 (desplaçament d'un període).

    Returns:
        pd.DataFrame: DataFrame original amb 3 noves columnes:
            - ema18: EMA de 18 períodes.
            - ema30: EMA de 30 períodes.
            - ema200: EMA de 200 períodes.
    """
    data['ema18'] = data.close.shift(BarShift).ewm(span=18, adjust=False).mean()
    data['ema30'] = data.close.shift(BarShift).ewm(span=30, adjust=False).mean()
    data['ema200'] = data.close.shift(BarShift).ewm(span=200, adjust=False).mean()
    return data
