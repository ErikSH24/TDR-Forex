import pandas as pd
import os
from datetime import datetime, timedelta
from tqdm import tqdm
import gc
import matplotlib.pyplot as plt
import MetaTrader5 as mt5
from utils.mt5 import init_mt5
from utils.indicators import get_EMAs

def get_symbol_details(symbol_name):
    """
    Obté detalls específics d'un símbol de MT5.
    """
    init_mt5()
    info = mt5.symbol_info(symbol_name)
    mt5.shutdown()
    
    if info is None:
        return {
            'digits': None,
            'spread': None,
            'point': None,
            'volume_min': None,
            'volume_max': None,
            'swap_long': None,
            'swap_short': None
        }
    
    return {
        'digits': info.digits,
        'spread': info.spread,
        'point': info.point,
        'volume_min': info.volume_min,
        'volume_max': info.volume_max,
        'swap_long': info.swap_long,
        'swap_short': info.swap_short
    }

def get_symbols_info(path_root, save=True):
    """Obté tots els símbols Forex de MT5 amb els seus detalls específics.
    
    Returns:
        pd.DataFrame: DataFrame amb les columnes:
            - name, description, currency_base, currency_quote
            - digits, spread, point, volume_min, volume_max, swap_long, swap_short
    """    
    init_mt5()
    symbols = mt5.symbols_get()
    mt5.shutdown()
    symbols = pd.DataFrame(list(symbols), columns=symbols[0]._asdict().keys())
    symbols['forex'] = symbols.path.map(lambda x: "Forex" in x)
    symbols = symbols[symbols.forex==True].reset_index(drop=True)
    symbols = (symbols[['name', 'description', 'currency_base', 'currency_profit']]
               .rename(columns={'currency_profit': 'currency_quote'}))
    print(f"We have {len(symbols)} forex symbols")
    
    details = pd.DataFrame(
        symbols['name'].apply(lambda x: get_symbol_details(x)).tolist()
    )
    
    symbols = pd.concat([symbols, details], axis=1)
    if save:
        file = os.path.join(path_root, "output", "Forex", "info_symbols.csv")
        symbols.to_csv(file, index=False)
    return symbols

def save_data(symbols, start, end, path_root):
    """
    Desa dades històriques de Forex de MetaTrader 5 en arxius CSV comprimits.
    
    Args:
        symbols (list): Llista de símbols (ex: ["EURUSD", "GBPUSD"])
        start (datetime): Data d'inici
        end (datetime): Data final
        path_root (str): Ruta arrel per desar els arxius
        
    Returns:
        None. Desa arxius a:
            - {path_root}/output/Forex/{timeframe}/{symbol}.csv.gz
            - {path_root}/output/Forex/info_files.csv (metadades)
            - {path_root}/output/Forex/{symbol}_D1.png (gràfiques)
    
    Processament:
        1. Connecta a MT5
        2. Itera per cada símbol/timeframe
        3. Desa dades comprimides
        4. Genera i desa gràfics per timeframes diaris (D1)
        5. Registra metadades (mida, nombre de barres)
    """
    mt5_timeframes = {
        'M1': mt5.TIMEFRAME_M1,
        'M5': mt5.TIMEFRAME_M5,
        'M15': mt5.TIMEFRAME_M15,
        'H1': mt5.TIMEFRAME_H1,
        'D1': mt5.TIMEFRAME_D1,
    }
    
    info_files = pd.DataFrame(columns=["símbol", "finestra", "inici", "fi", "MB", "n_barres"])
    counter = 0
    
    init_mt5()  # Connecta a MT5
    
    try:
        for symbol in tqdm(symbols, desc="Guardant dades", unit="símbol"):
            for timeframe_name, timeframe_value in mt5_timeframes.items():
                # 1. Prepara estructura de carpetes
                output_path = os.path.join(path_root, "output", "Forex", timeframe_name)
                os.makedirs(output_path, exist_ok=True)
                
                # 2. Obtenció de dades
                rates = mt5.copy_rates_range(symbol, timeframe_value, start, end)
                if rates is None:  # Maneig d'errors
                    print(f"⚠️ No s'han trobat dades per {symbol} {timeframe_name}")
                    continue
                    
                df = pd.DataFrame(rates)
                df['time'] = pd.to_datetime(df['time'], unit='s')
                
                # 3. Desa arxiu comprimit
                file_path = os.path.join(output_path, f"{symbol}.csv.gz")
                df.to_csv(file_path, index=False, compression="gzip")
                
                # 4. Registra metadades
                file_size_mb = round(os.path.getsize(file_path) / (1024 ** 2), 2)
                info_files.loc[counter] = [symbol, timeframe_name, df.time.min(), df.time.max(), file_size_mb, len(df)]
                
                # 5. Gràfic per timeframe diari
                if timeframe_name == "D1":
                    plt.figure(figsize=(18,4))
                    df.set_index("time")["close"].plot(title=f"{symbol} {timeframe_name}")
                    plt.tight_layout()
                    plot_path = os.path.join(path_root, "output", "Forex", "charts")
                    os.makedirs(plot_path, exist_ok=True)  # Crea carpeta si no existeix
                    plt.savefig(os.path.join(plot_path, f"{symbol}_D1.png"), dpi=150, bbox_inches='tight')
                    plt.show()
                
                counter += 1
                del df  # Allibera memòria
                gc.collect()
            
            # Mostra progrés parcial
            display(info_files[info_files.símbol==symbol].reset_index(drop=True))
        
        # Desa metadades completes
        info_files.to_csv(os.path.join(path_root, "output", "Forex", "info_files.csv"), index=False)
        
    except Exception as e:
        print(f"❌ Error crític: {str(e)}")
    finally:
        mt5.shutdown()  # Tanca connexió
        
def load_data(symbol, timeframe, path_root):
    """
    Carrega dades històriques d'un parell de divises des d'un arxiu CSV comprimit.

    Args:
        symbol (str): Símbol del parell de divises (ex: 'EURUSD').
        timeframe (str): Timeframe de les dades (ex: 'D1', 'H1').
        path_root (str): Ruta arrel on es troben els arxius de dades.

    Returns:
        pd.DataFrame: DataFrame amb les dades carregades, amb les columnes:
            - time: Data i hora (datetime).
            - open: Preu d'obertura.
            - high: Preu màxim.
            - low: Preu mínim.
            - close: Preu de tancament.
            - tick_volume: Volum en ticks.
    """
    path_data = os.path.join(path_root, "output", "Forex", timeframe)
    filename = os.path.join(path_data, f"{symbol}.csv.gz")
    data = pd.read_csv(filename, parse_dates=["time"]).drop(['real_volume', 'spread'], axis=1)    
    return data

def get_dataset(symbol, path_root, start="2000-01-01"):
    """
    Args:
        symbol (str): Símbol del parell (ex: 'EURUSD')
        path_root (str): Ruta arrel dels arxius de dades
        start (str): Data d'inici en format 'YYYY-MM-DD' (per filtrar dades)
    
    Returns:
        tuple: (data_M1, data_H1) - DataFrames amb dades minútils i horàries
               amb indicadors de tendència diària incorporats.
    """
    
    # ======================
    # 1. Processament D1 (Diari)
    # ======================
    data_D1 = load_data(symbol, "D1", path_root)
    
    # Crear columna de data sense hora per fer merge posterior
    data_D1['date'] = data_D1.time.dt.date  
    
    # Calcular EMAs i canviar noms per identificar timeframe
    data_D1 = (get_EMAs(data_D1)
              .rename(columns={
                  'ema18': 'ema18_D1',
                  'ema30': 'ema30_D1', 
                  'ema200': 'ema200_D1'
              }))
    
    # Seleccionar només columnes necessàries
    data_D1 = data_D1[['date', 'ema18_D1', 'ema30_D1', 'ema200_D1']]
    

    # ======================
    # 2. Processament H1 (Horari)
    # ====================== 
    data_H1 = load_data(symbol, "H1", path_root)
    
    # Extracció components de data/hora
    data_H1['date'] = data_H1.time.dt.date
    data_H1['year'] = data_H1.time.dt.year
    data_H1['month'] = data_H1.time.dt.month 
    data_H1['day'] = data_H1.time.dt.day
    data_H1['hour'] = data_H1.time.dt.hour
    
    # Calcular EMAs horàries
    data_H1 = (get_EMAs(data_H1)
              .rename(columns={
                  'ema18': 'ema18_H1',
                  'ema30': 'ema30_H1',
                  'ema200': 'ema200_H1'
              }))
    
    # Combinar amb dades diàries
    data_H1 = data_H1.merge(data_D1, on='date', how='left')
    
    # Filtrar per data d'inici
    data_H1 = data_H1[data_H1.time >= pd.to_datetime(start)].reset_index(drop=True)
    
    # Selecció final de columnes
    cols = [
        'year', 'month', 'day', 'hour',
        'ema18_D1', 'ema30_D1', 'ema200_D1',  # Tendència diària
        'ema18_H1', 'ema30_H1', 'ema200_H1'   # Tendència horària
    ]
    data_H1 = data_H1[cols]
    

    # ======================
    # 3. Processament M1 (Minutal)
    # ======================
    data_M1 = load_data(symbol, "M1", path_root)
    
    # Extracció components de data/hora 
    data_M1['date'] = data_M1.time.dt.date
    data_M1['year'] = data_M1.time.dt.year
    data_M1['month'] = data_M1.time.dt.month
    data_M1['day'] = data_M1.time.dt.day
    data_M1['hour'] = data_M1.time.dt.hour
    
    # Filtrar per data d'inici
    data_M1 = data_M1[data_M1.time >= pd.to_datetime(start)].reset_index(drop=True)
    

    return data_M1, data_H1

