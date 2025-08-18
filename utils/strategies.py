import pandas as pd

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
    if (DynamicLotSize == True) & (StopLoss > 0):
        # Calcula l'import màxim a arriscar
        MaxRiskAmount = AccountEquity * EquityPercent / 100
        
        # Fórmula de mida de lot: (Risc / StopLoss) * factor de conversió
        LotSize = (MaxRiskAmount / StopLoss) * 0.10
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

def get_strategy_H1(data_H1, data_M1, strategy, backtest=True):
    """
    Obté les dades necessàries per fer el backtest d'estratègies

    Args:
        data_H1 (pd.DataFrame): Dades H1
        data_M1 (pd.DataFrame): Dades M1
        strategy (str): Nom de l'estratègia
        backtest (bool): Si True, mostra estadístiques de senyals generades

    Returns:
        tuple: (data_M1, data_H1, strategy_params)
            - data_M1
            - data_H1
            - strategy_params: Diccionari de paràmetres de l'estratègia
    """
    
    # Paràmetres generals de l'estratègia
    strategy_params = {
        "DynamicLotSize": True,        # Utilitza mida de lot dinàmic
        "EquityPercent": 15,           # Percentatge d'equity per risc
        "FixedLotSize": 0.10,          # Mida fixa de lot (si DynamicLotSize=False)
        "MaxOpenTrades": 1,            # Màxim nombre de trades oberts simultàniament
        "MaxMinutesOpenTrades": None,  # Temps màxim en minuts dels trades
        "MinBetweenTrades": None       # Temps mínim entre trades
    }

    if strategy == 'mitjanes':
        # Configuració específica per estratègia de creuament de mitjanes
        strategy_params.update({
            "MaxMinutesOpenTrades": 60*3,  # Tancament automàtic després de 3 hores
            "MinBetweenTrades": 60          # Esperar 1 hora entre trades
        })

        # Inicialitzar columnes de tancament
        data_H1["cond_close_long"] = 0
        data_H1["cond_close_short"] = 0

        # Condicions d'entrada
        long_entry_cond = (
            (data_H1.ema30_H1 > data_H1.ema200_H1) & 
            (data_H1.ema30_H1.shift(1) <= data_H1.ema200_H1.shift(1))
        )
        
        short_entry_cond = (
            (data_H1.ema30_H1 < data_H1.ema200_H1) & 
            (data_H1.ema30_H1.shift(1) >= data_H1.ema200_H1.shift(1))
        )

        # Nivells de Take Profit i Stop Loss (fixos en aquesta versió)
        data_H1["TP_short"] = 100  # 10 pips
        data_H1["TP_long"] = 100    # 10 pips
        data_H1["SL_short"] = 100   # 10 pips
        data_H1["SL_long"] = 100    # 10 pips

    # Generar senyal consolidada (1=long, -1=short, 0=neutral)
    data_H1["signal"] = long_entry_cond.astype(int) - short_entry_cond.astype(int)

    # Preparar dades minutals per al merge
    data_M1["n_minute"] = data_M1.groupby(
        ["year", "month", "day", "hour"]
    ).time.cumcount() + 1  # Comptador de minuts dins de cada hora

    # Columnes a propagar a timeframe M1
    cols_strategy = [
        "year", "month", "day", "hour",
        "signal", "SL_long", "SL_short", 
        "TP_long", "TP_short", 
        "cond_close_long", "cond_close_short"
    ]

    # Combinar senyals H1 amb dades M1
    data_M1 = data_M1.merge(
        data_H1[cols_strategy], 
        on=["year", "month", "day", "hour"], 
        how="left"
    )

    # Només mantenir senyal al primer minut de cada hora
    data_M1.loc[data_M1.n_minute > 1, "signal"] = 0

    # Estadístiques per backtesting
    if backtest:
        print(f"Ordres de compra: {sum(data_M1.signal==1)}")
        print(f"Ordres de venda: {sum(data_M1.signal==-1)}")
        print(f"Ratio compra/venda: {sum(data_M1.signal==1)/max(1, sum(data_M1.signal==-1)):.2f}")

    return data_M1, data_H1, strategy_params