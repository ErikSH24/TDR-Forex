import pandas as pd

def get_EMAs(data):
    """
    Calcula múltiples mitjanes mòbils exponencials (EMAs) sobre les dades de tancament,
    amb opció de desplaçament temporal.

    Args:
        data (pd.DataFrame): DataFrame amb columnes ['time', 'close'] com a mínim.

    Returns:
        pd.DataFrame: DataFrame original amb 3 noves columnes:
            - ema18: EMA de 18 períodes.
            - ema30: EMA de 30 períodes.
            - ema200: EMA de 200 períodes.
    """
    data['ema18'] = data.close.ewm(span=18, adjust=False).mean()
    data['ema30'] = data.close.ewm(span=30, adjust=False).mean()
    data['ema200'] = data.close.ewm(span=200, adjust=False).mean()
    return data
