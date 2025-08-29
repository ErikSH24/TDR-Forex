import pandas as pd
import json
import os
import MetaTrader5 as mt5

from utils.data import load_data
from utils.indicators import get_EMAs

def CalcLotSize(DynamicLotSize, AccountEquity, EquityPercent, StopLoss, FixedLotSize, minLotSize=0.01, maxLotSize=5.0):
    """
    Calcula la mida del lot per a una operació basant-se en la gestió de risc.

    Args:
        DynamicLotSize (bool): Si True, calcula la mida dinàmicament segons el risc.
        AccountEquity (float): Saldo actual del compte de trading.
        EquityPercent (float): Percentatge d'equity a arriscar per operació.
        StopLoss (float): Distància en pips del stop loss.
        FixedLotSize (float): Mida fixa del lot si no s'utilitza càlcul dinàmic.
        minLotSize (float, optional): Mida mínima permesa del lot. Default=0.01.
        maxLotSize (float, optional): Mida màxima permesa del lot. Default=5.0.

    Returns:
        float: Mida del lot calculada, arrodonida a 2 decimals.
    """
    
    # Càlcul dinàmic de la mida del lot
    if DynamicLotSize and (StopLoss > 0):
        # Calcula l'import màxim a arriscar
        MaxRiskAmount = AccountEquity * EquityPercent / 100
        
        # Fórmula de mida de lot: (Risc / StopLoss) * factor de conversió
        LotSize = (MaxRiskAmount / StopLoss)
    else:
        LotSize = FixedLotSize
    
    # Aplica límits de mida de lot
    if LotSize < minLotSize:
        LotSize = minLotSize
    if LotSize > maxLotSize:
        LotSize = maxLotSize
    
    # Arrodoniment a 2 decimals (mida estàndard de lots)
    LotSize = round(LotSize, 2)
    
    return LotSize    


def load_strategy_parameters(strategy_name, symbol, timeframe, path_root):
    """, 
    Carrega i combina els paràmetres d'una estratègia de trading des d'un fitxer JSON.

    - strategy_name: nom de l'estratègia (clau principal del diccionari)
    - symbol: parell de divises (per ex. 'EURUSD')
    - timeframe: marc temporal (per ex. 'M1', 'H1', 'D1')
    - path_root (str): Path arrel per carregar fitxers de configuració i dades

    Retorna un diccionari "pla" amb:
      - MagicNumber i descripció generals de l’estratègia
      - El símbol i el timeframe seleccionats
      - Tots els paràmetres específics d’aquell símbol i timeframe
    """
    path = os.path.join(path_root, "input/Forex/strategies")
    filename = os.path.join(path, "dict_strategies.json")
    with open(filename, "r", encoding="utf-8") as f:
        dict_strategies = json.load(f)
    dict_parameters = dict_strategies[strategy_name]
    flat = {
        "MagicNumber": dict_parameters["MagicNumber"],
        "Description": dict_parameters["Description"],
        "Symbol": symbol,
        "TimeFrame": timeframe
    }
    flat.update(dict_parameters["pairs"][symbol][timeframe])
    return flat
    
def get_strategy(strategy_name, symbol, timeframe, path_root, realtime=False):
    """
    Obté dades i genera senyals d'estratègia per a un símbol i timeframe donats.

    Parameters:
    strategy_name (str): Nom de l'estratègia (ex: 'Mitjanes')
    symbol (str): Símbol de l'actiu (ex: 'EURUSD')
    timeframe (str): Marc temporal (ex: 'M1', 'H1')
    path_root (str): Path arrel per carregar fitxers de configuració i dades
    realtime (bool): Si és True, obté dades en temps real de MT5

    Returns:
    DataFrame: Dades amb senyals d'operativa o None en cas d'error
    """
    
    # Diccionari de conversió de timeframes a valors de MT5
    dict_timeframes = {
        "M1": mt5.TIMEFRAME_M1, 
        "M15": mt5.TIMEFRAME_M15, 
        "H1": mt5.TIMEFRAME_H1, 
        "D1": mt5.TIMEFRAME_D1, 
    }

    # Carreguem paràmetres de l'estratègia des de fitxer
    strategy_params = load_strategy_parameters(strategy_name, symbol, timeframe, path_root)
    timeframe_mt5 = dict_timeframes[strategy_params["TimeFrame"]]
    nbars = strategy_params["NBars"]

    # Mode temps real
    if realtime:
        try:
            if not mt5.initialize():
                print(f"Error MT5: {mt5.last_error()}")
                return None  # Sortida anticipada en cas d'error
            
            # Obtenir dades històriques des de la posició actual
            rates = mt5.copy_rates_from_pos(symbol, timeframe_mt5, 0, nbars+5)
            if rates is None or len(rates) == 0:
                print(f"No s'han trobat dades per {symbol} {timeframe}")
                return None
                
        except Exception as e:
            print(f"Error dades MT5: {e}")
            return None
        
        # Convertir les dades a DataFrame i netejar columnes no necessàries
        data = pd.DataFrame(rates).drop(['real_volume', 'spread'], axis=1, errors='ignore')
        data['time'] = pd.to_datetime(data['time'], unit='s')  # Convertir timestamp a datetime
        
    # Mode backtest: carregar dades des de fitxer
    else:
        data = load_data(symbol, timeframe, path_root)
        if data is None or len(data) == 0:
            print(f"No s'han trobat dades per {symbol} {timeframe}")
            return None

    #########################################################################
    # Definim signal, cond_close_long i cond_close_short per a les diferents estratègies
    #########################################################################
    if strategy_name == "Mitjanes":
        indicator_cols = ['open', 'high', 'low', 'close', 'ema18', 'ema30', 'ema200']
        data = get_EMAs(data)

        long_entry_cond = (data.ema18 > data.ema30) & (data.ema18.shift(1) < data.ema30.shift(1))
        short_entry_cond = (data.ema18 < data.ema30) & (data.ema18.shift(1) > data.ema30.shift(1))

        data["signal"] = long_entry_cond * 1 - short_entry_cond * 1
        data['cond_close_long'] = 0
        data['cond_close_short'] = 0

    # Retardar senyals una barra
    for col in ['signal', 'cond_close_long', 'cond_close_short']:
        data[col] = data[col].shift(1)
        
    # Processament específic segons mode (realtime vs backtest)
    if realtime:
        # Mode realtime: només necessitem l'última barra
        data = data[-1:].reset_index(drop=True)
    else:
        data = data[data.time >= pd.to_datetime("2000-01-01")].reset_index(drop=True)
        
        # Mostrar estadístiques de senyals generats
        print(f"Ordres de compra: {sum(data.signal==1)}")
        print(f"Ordres de venda: {sum(data.signal==-1)}")
        print(f"Ratio compra/venda: {sum(data.signal==1)/max(1, sum(data.signal==-1)):.2f}")
        
    cols = ['time', 'signal', 'cond_close_long', 'cond_close_short'] + indicator_cols
    data = data[cols]

    return data
    
def preparing_data_backtest(data, symbol, timeframe, path_root):
    """
    Prepara el dataset per fer backtest. Si el timeframe ja és M1, retorna el DataFrame tal qual.
    Si no és M1, propaga les senyals del timeframe original a les barres M1 corresponents.

    Parameters:
        data (DataFrame): Dades del timeframe original amb senyals generades
        symbol (str): Símbol per carregar les dades M1
        timeframe (str): Timeframe de les dades originals
        path_root (str): Path arrel per carregar fitxers

    Returns:
        DataFrame: Dades en timeframe M1 amb senyals propagades, o el mateix DataFrame si timeframe='M1'
    """
    # Treballem amb una còpia per evitar warnings
    data = data.copy()

    if timeframe == 'M1':
        return data.copy()
    
    # Creem columna amb l'hora (sense minuts) per al merge
    data['time_hour'] = data['time'].dt.floor('h')
    data = data.drop('time', axis=1)
    
    # Carreguem dades M1 i filtrem a partir de l'any 2000
    data_M1 = load_data(symbol, 'M1', path_root)
    data_M1 = data_M1[data_M1.time >= pd.to_datetime("2000-01-01")].copy()
    data_M1['time_hour'] = data_M1['time'].dt.floor('h')
    
    # Revisem que no hi hagi duplicates per hora al DataFrame original
    data = data.drop_duplicates(subset=['time_hour'])
    
    # Merge: afegim senyals del timeframe original a les barres M1
    data_M1 = data_M1.merge(
        data[['time_hour', 'signal', 'cond_close_long', 'cond_close_short']],
        on='time_hour',
        how='left'
    )
    
    # Comptem el minut dins de cada hora
    data_M1['n_minute'] = data_M1.groupby('time_hour').cumcount()
    
    # Només la primera barra de cada hora manté el senyal
    data_M1.loc[data_M1['n_minute'] > 0, ['signal', 'cond_close_long', 'cond_close_short']] = 0
    
    # Neteja de columnes auxiliars i reset d'índex
    data_M1 = data_M1.drop(['time_hour', 'n_minute'], axis=1)
    data_M1 = data_M1.reset_index(drop=True)
    
    return data_M1
