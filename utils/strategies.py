import pandas as pd
import json
import os
import matplotlib.pyplot as plt
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

def save_strategy_parameters(path_root):
    """
    Guarda un diccionari amb els paràmetres de les estratègies en format JSON.

    - Cada clau del diccionari principal correspon a un ID d'estratègia.
    - Cada estratègia té:
        * Nom de l'estratègia (`name`)
        * Configuració de la mida del lot (dinàmica o fixa)
        * Percentatge de l'equity o mida fixa de la posició
        * Límits de nombre i durada de les operacions
        * Temps mínim entre operacions
        * Nivells de Take Profit i Stop Loss (per llargues i curtes)

    El fitxer es desa a:
        {path_root}/output/Forex/strategies/dict_strategies.json

    Args:
        path_root (str): Directori arrel on es guardaran els resultats.
    """
    dict_strategies = {
        1006: {
            'Name': "Mitjanes",
            'TimeFrame': "M1",
            "NBars": 200,
            'DynamicLotSize': True,
            'EquityPercent': 2.5,
            'FixedLotSize': None,
            'MaxOpenTrades': 1,
            'MaxMinutesOpenTrades': 60*12,
            'MinBetweenTrades': 60,
            'TP_short': 100,
            'TP_long': 100,
            'SL_long': 250,
            'SL_short': 250                
        },
        2006: {
            'Name': "Mitjanes",
            'TimeFrame': "H1",
            "NBars": 200,
            'DynamicLotSize': True,
            'EquityPercent': 2.5,
            'FixedLotSize': None,
            'MaxOpenTrades': 1,
            'MaxMinutesOpenTrades': 60*12,
            'MinBetweenTrades': 60,
            'TP_short': 250,
            'TP_long': 250,
            'SL_long': 1000,
            'SL_short': 1000                
        },
    }
    
    path = os.path.join(path_root, "output/Forex/strategies")
    filename = os.path.join(path, "dict_strategies.json")
    os.makedirs(path, exist_ok=True)
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(dict_strategies, f, indent=4, ensure_ascii=False)

def load_strategy_parameters(MagicNumber, path_root):
    """
    Carrega el diccionari de paràmetres d'estratègies des d'un fitxer JSON.

    El fitxer ha d'existir a:
        {path_root}/output/Forex/strategies/dict_strategies.json

    Args:
        path_root (str): Directori arrel on s'han desat els resultats.

    Returns:
        dict: Diccionari amb les estratègies i els seus paràmetres.
    """
    path = os.path.join(path_root, "output/Forex/strategies")
    filename = os.path.join(path, "dict_strategies.json")
    with open(filename, "r", encoding="utf-8") as f:
        dict_strategies = json.load(f)
    return dict_strategies[str(MagicNumber)]

def get_strategy(symbol, MagicNumber, path_root, realtime=False):
    """
    Obté dades i genera senyals d'estratègia per a un símbol i número màgic donats.
    
    Parameters:
    symbol (str): Símbol de l'actiu (ex: 'EURUSD')
    MagicNumber (int): Número màgic que identifica l'estratègia
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
    strategy_params = load_strategy_parameters(MagicNumber, path_root)
    timeframe = strategy_params["TimeFrame"]
    timeframe_mt5 = dict_timeframes[timeframe]
    nbars = strategy_params["NBars"]
    
    # Mode temps real
    if realtime:
        try:
            if not mt5.initialize():
                print(f"Error MT5: {mt5.last_error()}")
                return None  # Sortida anticipada en cas d'error
            
            # Obtenir dades històriques des de la posició actual
            rates = mt5.copy_rates_from_pos(symbol, timeframe_mt5, 0, nbars)
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
    # Definim signal, cond_close_log i cond_close_short per a les diferents estratègies
    #########################################################################
    if MagicNumber in [1006, 2006]:
        # Calcular mitjanes mòbils exponencials
        indicator_cols = ['open', 'high', 'low', 'close', 'ema18', 'ema30', 'ema200']
        data = get_EMAs(data)
                
        # Condicions d'entrada Llarg (Long)
        long_entry_cond = ((data.ema30>data.ema200) & (data.ema18>data.ema30) & (data.ema18.shift(1)<data.ema30.shift(1)))
        
        # Condicions d'entrada Curt (Short)
        short_entry_cond = ((data.ema30<data.ema200) & (data.ema18<data.ema30) & (data.ema18.shift(1)>data.ema30.shift(1)))
        
        # Crear senyal combinat: 1 = Compra, -1 = Venda, 0 = Cap senyal
        data["signal"] = long_entry_cond.astype(int) - short_entry_cond.astype(int)
        
        # Condicions de tancament
        data['cond_close_long'] = (data.ema30 < data.ema200) * 1
        data['cond_close_short'] = (data.ema30 > data.ema200) * 1
    
    if MagicNumber == 1974:
        # Calcular mitjanes mòbils exponencials
        indicator_cols = ['open', 'high', 'low', 'close', 'ema18', 'ema30', 'ema200']
        data = get_EMAs(data)
        
        # Extreure hora de cada timestamp
        data['hour'] = data.time.dt.hour
        
        # Condicions d'entrada Llarg (Long)
        long_entry_cond = (((data.hour==9) | (data.hour==16)) 
                           & (data.ema30>data.ema200) & (data.ema18>data.ema30) & (data.close.shift(1) > data.ema18))        
        
        # Condicions d'entrada Curt (Short)
        short_entry_cond = (((data.hour==9) | (data.hour==16)) 
                            & (data.ema30<data.ema200) & (data.ema18<data.ema30) & (data.close.shift(1) < data.ema18))        
        
        # Crear senyal combinat: 1 = Compra, -1 = Venda, 0 = Cap senyal
        data["signal"] = long_entry_cond.astype(int) - short_entry_cond.astype(int)
        
        # Condicions de tancament
        data['cond_close_long'] = 0
        data['cond_close_short'] = 0

    # La barra actual està en procés i no es pot utilitzar per prendre decisions
    # Les decisions es prenen basant-se en informació de barres completes (anteriors)
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
    
def preparing_data_backtest(data, symbol, path_root):
    '''
    Preparem dataset per fer el backtest en cas que el timeframe no sigui M1
    
    Parameters:
    data (DataFrame): Dades del timeframe original amb senyals generades
    
    Returns:
    DataFrame: Dades en timeframe M1 amb senyals propagades correctament
    '''
    # Creem una columna amb l'hora (sense minuts) per fer el merge
    data['time_hour'] = data['time'].dt.floor('h')
    data.drop('time', axis=1, inplace=True)
    # Carreguem dades M1 i filtrem a partir de l'any 2000
    data_M1 = load_data(symbol, 'M1', path_root)
    data_M1 = data_M1[data_M1.time >= pd.to_datetime("2000-01-01")].reset_index(drop=True)
    # Creem la mateixa columna d'hora per fer el merge
    data_M1['time_hour'] = data_M1['time'].dt.floor('h')
    
    # Fusionem les dades M1 amb les senyals del timeframe original
    # Les senyals es propagaran a totes les barres M1 dins de la mateixa hora
    data_M1 = data_M1.merge(data[['time_hour', 'signal', 'cond_close_long', 'cond_close_short']], on='time_hour', how='left')
    
    # Identifiquem el número de minut dins de cada hora
    data_M1['n_minute'] = data_M1.groupby('time_hour').time.cumcount()
    
    # Assegurem que el senyal només es manté per la primera barra M1 de l'hora
    data_M1.loc[data_M1.n_minute > 0, ['signal', 'cond_close_long', 'cond_close_short']] = 0
    
    # Netegem columnes auxiliars
    data_M1.drop(['time_hour', 'n_minute'], axis=1, inplace=True)
    
    return data_M1

def plot_signal(data, signal_index, bars_left=10, bars_right=20):    
    # Obtenir les dates amb senyal
    signal_dates = data[data.signal != 0].time.to_list()
    
    if not signal_dates:
        print("No hi ha senyals...")
        return
    
    if signal_index >= len(signal_dates):
        print(f"Índex {signal_index} fora de rang. Només hi ha {len(signal_dates)} senyals")
        return
    
    # Trobar la data del senyal específic
    target_date = signal_dates[signal_index]
    
    # Trobar l'índex d'aquesta data al DataFrame original
    signal_idx = data[data.time == target_date].index[0]
    
    # Calcular els índexs per al rang de dades a mostrar
    start_idx = max(0, signal_idx - bars_left)
    end_idx = min(len(data), signal_idx + bars_right + 1)
    
    # Seleccionar el subset de dades
    plot_data = data.iloc[start_idx:end_idx].copy()
    
    # Crear la figura
    fig, ax = plt.subplots(figsize=(15, 4))
    
    # Plotar preu de tancament
    ax.plot(plot_data['time'], plot_data['close'], 
            label='Close', color='black', linewidth=1.5, alpha=0.8)
    
    # Plotar EMAs
    ax.plot(plot_data['time'], plot_data['ema18'], 
            label='EMA 18', color='blue', linewidth=1.5, alpha=0.7)
    ax.plot(plot_data['time'], plot_data['ema30'], 
            label='EMA 30', color='green', linewidth=1.5, alpha=0.7)
    ax.plot(plot_data['time'], plot_data['ema200'], 
            label='EMA 200', color='red', linewidth=2, alpha=0.7)
    
    # Afegir barra vertical al punt del senyal
    ax.axvline(x=target_date, color='purple', linestyle='--', 
               linewidth=2, alpha=0.8, label='Senyal d\'Entrada')
    
    # Afegir fons ombrejat per al període del senyal
    ax.axvspan(target_date, plot_data['time'].iloc[-1], 
               alpha=0.1, color='yellow')
    
    # Configurar el gràfic
    ax.set_title(f"Senyal #{signal_index} - {target_date.strftime('%Y-%m-%d %H:%M')} - Tipus: {'COMPRA' if data.loc[signal_idx, 'signal'] == 1 else 'VENDA'} - Preu d'entrada: {data.loc[signal_idx, 'open']:.5f}")
    ax.set_xlabel('Temps')
    ax.set_ylabel('Preu')
    ax.legend(loc='best')
    ax.grid(True, alpha=0.3)
    # Rotar les dates de l'eix x per millor llegibilitat
    plt.xticks(rotation=45)
    # Ajustar layout perquè no es tallin les etiquetes
    plt.tight_layout()
    plt.show()