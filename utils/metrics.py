import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.graph_objects as go
import os

def get_trading_activity(trades):
    """
    Calcula el % de temps en què el compte ha estat actiu al mercat,
    tenint en compte operacions solapades (fusió d'intervals).
    """

    # Ordenem per entrada
    trades = trades.sort_values(by="entry_time")

    # Construïm llista d'intervals (entrada, sortida)
    intervals = list(zip(trades.entry_time, trades.exit_time))

    # Inicialitzem amb el primer interval
    merged_intervals = []
    current_start, current_end = intervals[0]

    for start, end in intervals[1:]:
        if start <= current_end:  
            # Solapament → allarguem el final si cal
            current_end = max(current_end, end)
        else:
            # No solapa → afegim interval i reiniciem
            merged_intervals.append((current_start, current_end))
            current_start, current_end = start, end

    # Afegim l'últim interval
    merged_intervals.append((current_start, current_end))

    # Temps total actiu (segons) = suma de tots els intervals fusionats
    temps_total_actiu = sum((end - start).total_seconds() for start, end in merged_intervals)

    # Temps total del període analitzat
    durada_total = (trades.exit_time.max() - trades.entry_time.min()).total_seconds()

    # Percentatge de temps actiu
    percentatge_actiu = (temps_total_actiu / durada_total) * 100

    return percentatge_actiu

def max_consecutive_wins(trades):
    """
    Calcula la ratxa màxima de guanys consecutius i el benefici acumulat
    d'aquesta ratxa.
    
    Retorna:
        (max_num_guanys, max_profit_guanys)
    """

    # Inicialització
    max_streak = 0
    current_streak = 0
    current_streak_profit = 0
    max_streak_profit = 0

    # Iterem per cada trade
    for _, trade in trades.iterrows():
        if trade['profit'] > 0:
            # Suma a la ratxa actual
            current_streak += 1
            current_streak_profit += trade['profit']

            # Si superem la ratxa màxima, actualitzem
            if current_streak > max_streak:
                max_streak = current_streak
                max_streak_profit = current_streak_profit
        else:
            # Reset si hi ha pèrdua o 0
            current_streak = 0
            current_streak_profit = 0

    return max_streak, max_streak_profit

def max_consecutive_profit(trades):
    """
    Calcula la ratxa amb el màxim benefici acumulat.
    
    Retorna:
        (max_benefici_acumulat, num_trades_ratxa)
    """

    max_profit = 0            # Benefici acumulat màxim trobat
    max_profit_trades = 0     # Nombre de trades que formen aquesta ratxa

    current_profit = 0        # Benefici acumulat de la ratxa actual
    current_count = 0         # Nombre de trades a la ratxa actual

    for _, trade in trades.iterrows():
        if trade['profit'] > 0:
            # Suma a la ratxa actual
            current_profit += trade['profit']
            current_count += 1

            # Si el benefici acumulat supera el màxim, actualitzem
            if current_profit > max_profit:
                max_profit = current_profit
                max_profit_trades = current_count
        else:
            # Reset de la ratxa si hi ha pèrdua o 0
            current_profit = 0
            current_count = 0

    return max_profit, max_profit_trades

def max_consecutive_losses(trades):
    """
    Calcula la ratxa més llarga de pèrdues consecutives
    i la pèrdua acumulada en aquesta ratxa.

    Retorna:
        (max_consecutive_losses, max_loss_sum)
    """

    max_consecutive = 0    # màxim nombre de trades perdedors seguits
    max_loss_sum = 0       # pèrdua acumulada en la ratxa més llarga

    current_consecutive = 0
    current_loss_sum = 0

    for _, trade in trades.iterrows():
        if trade['profit'] < 0:
            # seguim la ratxa de pèrdues
            current_consecutive += 1
            current_loss_sum += trade['profit']

            # si la ratxa actual és més llarga que la màxima registrada
            if current_consecutive > max_consecutive:
                max_consecutive = current_consecutive
                max_loss_sum = current_loss_sum
        else:
            # reset si el trade no és pèrdua
            current_consecutive = 0
            current_loss_sum = 0

    return max_consecutive, max_loss_sum

def max_consecutive_loss(trades):
    """
    Calcula la ratxa amb la pèrdua acumulada més gran (consecutiva) 
    i el nombre de trades que la composen.

    Retorna:
        (max_loss_sum, trades_in_streak)
    """

    max_loss_sum = 0        # màxima pèrdua acumulada (serà negativa)
    trades_in_max_streak = 0

    current_loss_sum = 0
    current_trades = 0

    for _, trade in trades.iterrows():
        if trade['profit'] < 0:
            # afegim pèrdua a la ratxa actual
            current_loss_sum += trade['profit']
            current_trades += 1

            # si aquesta ratxa té una pèrdua més gran en valor absolut
            if current_loss_sum < max_loss_sum:
                max_loss_sum = current_loss_sum
                trades_in_max_streak = current_trades
        else:
            # reset si hi ha un trade guanyador
            current_loss_sum = 0
            current_trades = 0

    return max_loss_sum, trades_in_max_streak

def calculate_sharpe(order_book):
    returns = order_book['Return %'] / 100  # Pasar a proporción
    if returns.std() == 0:
        return 0.0
    sharpe = returns.mean() / returns.std()
    return sharpe

def calculate_max_drawdown(order_book):
    balance = order_book['Balance'].values
    running_max = np.maximum.accumulate(balance)
    drawdowns = (balance - running_max) / running_max
    max_dd = drawdowns.min()  # valor negativo
    return abs(max_dd)  # en proporción

def calculate_recovery_factor(order_book):
    net_profit = order_book['profit'].sum()
    max_dd = calculate_max_drawdown(order_book)
    if max_dd == 0:
        return float('inf')
    return net_profit / (max_dd * order_book['Balance'].iloc[0])  # escalar al balance inicial

def calculate_all_drawdowns(order_book):
    """
    Calcula les 4 mètriques de drawdown a partir de la columna Balance
    """
    balance = order_book['Balance'].values
    initial_balance = balance[0]
    
    # 1. Absolute Drawdown (Màxima pèrdua respecte capital inicial)
    min_balance = np.min(balance)
    absolute_drawdown = max(0, initial_balance - min_balance)
    
    # 2. Maximal Drawdown (Màxima pèrdua des de qualsevol pic)
    peak = balance[0]
    maximal_drawdown_value = 0
    maximal_drawdown_percent = 0
    
    for i, current_balance in enumerate(balance):
        if current_balance > peak:
            peak = current_balance
        
        drawdown_value = peak - current_balance
        drawdown_percent = (drawdown_value / peak) * 100 if peak > 0 else 0
        
        if drawdown_value > maximal_drawdown_value:
            maximal_drawdown_value = drawdown_value
            maximal_drawdown_percent = drawdown_percent
    
    # 3. Relative Drawdown by Balance (% respecte capital inicial)
    relative_drawdown_balance = (absolute_drawdown / initial_balance) * 100 if initial_balance > 0 else 0
    
    # 4. Relative Drawdown by Equity (assumim que equity = balance si no tens columna Equity)
    # Si tens columna 'Equity' al order_book, canvia balance per equity
    equity = order_book['Balance'].values  # o order_book['Equity'] si tens
    peak_equity = equity[0]
    relative_drawdown_equity = 0
    
    for current_equity in equity:
        if current_equity > peak_equity:
            peak_equity = current_equity
        
        drawdown_equity = (peak_equity - current_equity) / peak_equity * 100 if peak_equity > 0 else 0
        if drawdown_equity > relative_drawdown_equity:
            relative_drawdown_equity = drawdown_equity
    
    return {
        'maximal_drawdown_value': maximal_drawdown_value,
        'maximal_drawdown_percent': maximal_drawdown_percent,
        'relative_drawdown_balance': relative_drawdown_balance,
        'relative_drawdown_equity': relative_drawdown_equity
    }
