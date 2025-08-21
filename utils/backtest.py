import pandas as pd
from tqdm import tqdm
import numpy as np
import os
from datetime import datetime, timedelta
import MetaTrader5 as mt5
from utils.mt5 import init_mt5
from utils.strategies import CalcLotSize

import plotly.graph_objs as go

def get_orders_M1(data_M1, InitialDeposit, symbol, strategy, strategy_params, online, path_root):
    
    '''
    Input: 
    - data_M1: dataframe with the following columns:
        * 'time', 'open', 'high', 'low', 'close', 
        * 'signal': 1 open buy order, -1 open sell order, 0 neutral
        * 'SL_long': stop loss (in pips) for long positions
        * 'TP_long': take profit (in pips) for long positions
        * 'SL_short': stop loss (in pips) for short positions
        * 'TP_short': take profit (in pips) for short positions
        * 'cond_close_long': if =1 close all long positions
        * 'cond_close_short': if =1 close all short positions
        * InitialDeposit: Initial Capital;
        * DynamicLotSize: Allow dynamic lot size or not;
        * EquityPercent: maximum % of account to risk on the trade;
        * MaxOpenTrades: maximum number of open trades at the same time;
        * Symbol: symbol of the dataframe (to obtain info like PipPoint, spread or swap)
    
    Output: retuns the book with detailed info of all trades as well as a dictionary concerning the details of the symbol
    '''
    path_results = os.path.join(path_root, f'Output/Forex/backtesting/{strategy}/{symbol}')
    os.makedirs(path_results, exist_ok=True)

    if  online:
        init_mt5()
        info_symbols = mt5.symbol_info(symbol)
        mt5.shutdown()
    else:
        path_data = path_root + '/Output/Data'
        info_symbols = pd.read_csv(os.path.join(path_root, 'output', 'Forex', 'info_symbols.csv'))
        info_symbols = info_symbols.set_index('name').T.to_dict()
    
    dict_details = {
        'Symbol': symbol,
        'PipPoint': info_symbols[symbol]['point'] * 10,
        'Spread': info_symbols[symbol]['spread'] * 0.1,
        'DailySwap': {
            'Long': info_symbols[symbol]['swap_long'] * 0.1, 
            'Short': info_symbols[symbol]['swap_short'] * 0.1
        }
    }

    DynamicLotSize = strategy_params['DynamicLotSize']
    EquityPercent = strategy_params['EquityPercent']
    FixedLotSize = strategy_params['FixedLotSize']
    MaxOpenTrades = strategy_params['MaxOpenTrades']
    MaxMinutesOpenTrade = strategy_params['MaxMinutesOpenTrade']
    MinBetweenTrades = strategy_params['MinBetweenTrades']
    if MinBetweenTrades == None:
        MinBetweenTrades = 0
    PipPoint = dict_details['PipPoint']
    spread = dict_details['Spread']
    DailySwap = dict_details['DailySwap']
            
    
    print(f'Strategy {strategy}')
    print(f'Symbol {symbol}')
    print(f'Initial Deposit {InitialDeposit}')
    print(f'Max Open Trades {MaxOpenTrades}')
    print(f'DynamicLotSize={DynamicLotSize} - EquityPercent={EquityPercent}% - FixedLotSize={FixedLotSize}')
    print(f'PipPoint {PipPoint}')
    print(f'Spread {spread:.2f}')
    print(f"SwapLong {dict_details['DailySwap']['Long']:.2f}")
    print(f"SwapShort {dict_details['DailySwap']['Short']:.2f}")

    #First we get all (long and short) orders from the strategy
    orders = data_M1.loc[(data_M1.signal.isin([-1,1])), ['time', 'open', 'signal']].reset_index(drop=True)

    #Init previous exit time (useful to count open orders at a given time), counter of orders, and account equity
    n_orders = 1
    exit_time_prev = None
    AccountEquity = InitialDeposit

    #Define book
    cols = ['order', 'type', 'volume', 'symbol', 'entry_time', 'entry_price', 'SL', 'TP', 
            'exit_time', 'exit_price', 'comission', 'swap', 'profit', 'pips', 'n_open_trades']
    book = pd.DataFrame(columns=cols)
    n_order_counter = 1
    #We iterate to get the info of every order
    for n_order in tqdm(range(len(orders))):
    #for n_order in tqdm(range(5)):

        #Get the type of the order: Buy or Sell
        type_bool = orders.signal.loc[n_order]
        if type_bool == -1.:
            type_ = 'Short'
        if type_bool == 1.:
            type_ = 'Long'

        #Get the daily swap in pips
        daily_swap = DailySwap[type_]

        #We count the number of trades at the same time
        entry_time = orders.time.loc[n_order]
        if exit_time_prev == None:
            minutes_elapsed = 1e09
        else:
            minutes_elapsed = (entry_time-exit_time_prev).days*24*60 + (entry_time-exit_time_prev).seconds/60/60
        if (exit_time_prev != None):
            if entry_time < exit_time_prev:
                n_orders += 1
            else:
                #Do not have any open position, a part from the current one
                n_orders = 1

        #print('n_orders: {} - MaxOpenTrades: {} - minutes_elapsed: {} - MinBetweenTrades: {}'.format(n_orders, MaxOpenTrades, minutes_elapsed, MinBetweenTrades))
        if (n_orders <= MaxOpenTrades) & (minutes_elapsed > MinBetweenTrades):
            #We get the history from the entry time to the end
            feats_strategy =  ['time', 'open', 'high', 'low', 'close', 
                               'signal', 'SL_long', 'TP_long', 'SL_short', 'TP_short', 'cond_close_long', 'cond_close_short']\
                               + [feat for feat in ['entry_price_long', 'entry_price_short'] if feat in data_M1.columns]
            df_temp = data_M1.loc[(data_M1.time>=entry_time), feats_strategy].head(60*24*30).reset_index(drop=True)

            if type_ == 'Long':
                StopLoss = df_temp.SL_long.loc[0]         #Get stop loss for long positions
                TakeProfit = df_temp.TP_long.loc[0]       #Get take profit for long positions
                if 'entry_price_long' in data_M1.columns:
                    entry_price = df_temp.entry_price_long.loc[0] + spread*PipPoint
                else:
                    entry_price = orders.open.loc[n_order] + spread*PipPoint                      #Open long at ask price at open
                df_temp['entry_price'] = entry_price                                          #Column as entry price
                df_temp['profit_pips'] = (df_temp.close-df_temp.entry_price)/PipPoint         #Profit at bid close
                df_temp['profit_pips_max'] = (df_temp.high-df_temp.entry_price)/PipPoint           #Max profit of the position at a given bar
                df_temp['drawdown_pips_max'] = (df_temp.entry_price-df_temp.low)/PipPoint          #Max drawdown (positive) of the position at a given bar

                #We close the long position if one of the following conditions hold (the first one that is achieved)
                df_temp['cond_close'] = df_temp.cond_close_long

            if type_ == 'Short':
                StopLoss = df_temp.SL_short.loc[0]        #Get stop loss for long positions
                TakeProfit = df_temp.TP_short.loc[0]      #Get take profit for long positions
                if 'entry_price_short' in data_M1.columns:
                    entry_price = df_temp.entry_price_short.loc[0]
                else:
                    entry_price = orders.open.loc[n_order]                                                 #Open Short at bid price at open 
                df_temp['entry_price'] = entry_price                                                   #Column as entry price
                df_temp['profit_pips'] = (df_temp.entry_price-df_temp.close)/PipPoint - spread*PipPoint     #Profit at ask close
                df_temp['profit_pips_max'] = (df_temp.entry_price - df_temp.low)/PipPoint - spread*PipPoint #Max profit at a given bar
                df_temp['drawdown_pips_max'] = (df_temp.high-df_temp.entry_price)/PipPoint - spread*PipPoint#Max drawdown (positive) at a given bar

                #We close the short position if one of the following conditions hold (the first one that is achieved)
                df_temp['cond_close'] = df_temp.cond_close_short
                
            if MaxMinutesOpenTrade != None:
                df_temp['cond_close'] = ((df_temp.cond_close) | (df_temp.index>MaxMinutesOpenTrade))*1
            
            #We compute the lot size with the specified risk
            LotSize = CalcLotSize(DynamicLotSize, AccountEquity, EquityPercent, StopLoss, FixedLotSize)
            #print('AccountEquity: {} - LotSize {}'.format(AccountEquity, LotSize))
            #We define flags for SL and TP
            df_temp['cond_stop_loss'] = (df_temp.drawdown_pips_max>StopLoss)*1
            df_temp['cond_take_profit'] = (df_temp.profit_pips_max>TakeProfit)*1

            #We close the position if one of the conditions hold
            df_temp['cond_close_order'] = ((df_temp.cond_close) | (df_temp.cond_stop_loss) | (df_temp.cond_take_profit))*1

            #We choose the first ocurrence of the three conditions
            temp = df_temp[df_temp.cond_close_order==1].head(2).reset_index(drop=True)

            #If we close the position (it can happen that none of the conditions hold)
            #The first row is to obtain the condition, the second one to obtain the price to close the position
            if len(temp) == 2:

                #If SL and TP are achived in the same bar, we consider the worst situation
                #We get the values of the flags for the three conditions 
                cond_SL = temp.cond_stop_loss.loc[1]
                cond_TP = temp.cond_take_profit.loc[1]
                cond_close = temp.cond_close.loc[1]

                if cond_SL == 1:
                    #We exit at the current bar
                    exit_time = temp.time.loc[1]
                    exit_price = entry_price - StopLoss*PipPoint*type_bool
                    pips = -StopLoss
                elif cond_close == 1:
                    #We exit at the next bar
                    exit_time = temp.time.loc[1]
                    exit_price = temp.open.loc[1] + spread*PipPoint*(type_ == 'Short')
                    pips = (exit_price-entry_price)/PipPoint*type_bool
                elif cond_TP == 1:
                    #We exit at the current bar
                    exit_time = temp.time.loc[1]
                    exit_price = entry_price + TakeProfit*PipPoint*type_bool
                    pips = TakeProfit

                days = (exit_time.date()  - entry_time.date()).days

                #Adding swap
                swap = round(days*daily_swap*LotSize/0.10,2)
                profit = round(pips*LotSize/0.10+swap,2)
            #If the position is not closed yet
            else:
                profit = np.nan

            #We fill the order details
            book.loc[n_order] = [n_order_counter,                     #order number
                                 type_,                               #type order: buy or sell
                                 LotSize,                             #Lot size
                                 symbol,                              #Symbol
                                 entry_time,                          #Order entry time
                                 entry_price,                         #Order entry price
                                 entry_price-StopLoss*PipPoint*type_bool,       #Stop Loss
                                 entry_price+TakeProfit*PipPoint*type_bool,   #Take Profit
                                 exit_time,                           #Order exit time
                                 exit_price,                          #Order exit price
                                 0,                                   #Comission
                                 swap,                                #Swap
                                 profit,                              #Profit
                                 pips,                                #pips
                                 n_orders                             #N simultaneous orders by type
                                ] 
            #Update
            n_order_counter +=1
            exit_time_prev = exit_time
            AccountEquity += profit

    #We filter orders by max_open_trades
    #book = book[book.n_open_trades<=max_open_trades].reset_index(drop=True)
    #book['order'] = book.index + 1
    book['Balance'] = book.profit.cumsum() + InitialDeposit
    book['Reward %'] = (book['Balance']-InitialDeposit)/InitialDeposit*100

    file_book = os.path.join(path_results, f'book_{symbol}_{strategy}.csv.gz')
    file_dict_details = os.path.join(path_results, f'details_{symbol}_{strategy}.npy')
    book.to_csv(file_book, index=False, compression='gzip')
    np.save(file_dict_details, dict_details)        

    return book, dict_details

def get_monthly_returns(signal, path_results):
    # Inicialitzar el balance de partida
    previous_balance = signal.iloc[0].Balance

    # Generar mesos entre primera i última trade
    date_range = pd.date_range(signal.entry_time.min().replace(day=1),
                               signal.exit_time.max().replace(day=1),
                               freq="MS").strftime("%Y-%m").tolist()

    monthly_returns = []
    
    for i in range(len(date_range)-1):
        year, month = map(int, date_range[i].split("-"))
        next_year, next_month = map(int, date_range[i+1].split("-"))
        
        mask = (
            (signal.exit_time >= datetime(year, month, 1)) &
            (signal.exit_time < datetime(next_year, next_month, 1)) &
            (signal['type'] != 'Balance')
        )
        if mask.sum() > 0:
            temp = signal.loc[mask].sort_values("exit_time")
            last_balance = temp.Balance.values[-1]
            return_month = (last_balance - previous_balance) / previous_balance * 100
            previous_balance = last_balance
        else:
            return_month = 0
        monthly_returns.append([date_range[i], return_month])

    # Crear DataFrame
    monthly_returns = pd.DataFrame(monthly_returns, columns=["date", "return"])
    monthly_returns["year"] = monthly_returns.date.str[:4].astype(int)
    monthly_returns["month"] = monthly_returns.date.str[5:].astype(int)

    # Pivot amb mesos
    monthly_pivot = monthly_returns.pivot_table(
        index="year", columns="month", values="return", aggfunc="sum"
    ).fillna(0)

    # Canviar noms dels mesos
    monthly_pivot.columns = ["Jan","Feb","Mar","Apr","May","Jun",
                             "Jul","Aug","Sep","Oct","Nov","Dec"]

    # Retorn anual i total
    monthly_pivot["Year"] = ((1 + monthly_pivot/100).prod(axis=1) - 1) * 100
    return_overall = ((1 + monthly_pivot["Year"]/100).prod() - 1) * 100

    # Format amb 2 decimals
    formatted = monthly_pivot.map(lambda x: f"{x:.2f}%")
    formatted = formatted.reset_index()

    # Afegir fila total
    last_row = [""] * (len(formatted.columns)-1) + [f"Total: {return_overall:.2f}%"]
    formatted.loc["Return Overall"] = last_row

    # Taula Plotly
    fig = go.Figure(data=[go.Table(
        header=dict(values=["Year"] + formatted.columns[1:].tolist(),
                    fill_color="lightgrey",
                    align="center"),
        cells=dict(values=[formatted[col] for col in formatted.columns],
                   fill_color=[["#FFFFFF","#F9F9F9"] * (len(formatted)//2+1)],
                   align="center"))
    ])

    fig.update_layout(
        title="Monthly Returns", title_x=0.5,
        margin=dict(l=20, r=20, t=40, b=20),
        height=700
    )

    filename = os.path.join(path_results,"resultats_mensuals")
    fig.write_html(f'{filename}.html')
    fig.write_image(f'{filename}.png', scale=2)
    fig.show()
    
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


def get_statistics(signal, path_results):

    # Eliminar les files que són només "Balance" (no són operacions reals)
    trades = signal[signal['type'] != 'Balance'].reset_index(drop=True)
    
    # ---- Estadístiques bàsiques ----
    total_trades = len(trades)  # total d'operacions
    profit_trades = sum(trades['profit'] >= 0)  # operacions guanyadores
    loss_trades = total_trades - profit_trades  # operacions perdedores
    best_trade = trades['profit'].max()  # millor operació
    worst_trade = trades['profit'].min()  # pitjor operació
    
    # Beneficis i pèrdues acumulats
    gross_profit = trades.loc[trades['profit'] >= 0, 'profit'].sum()
    gross_profit_pips = trades.loc[trades['profit'] >= 0, 'pips'].sum()
    gross_loss = trades.loc[trades['profit'] < 0, 'profit'].sum()
    gross_loss_pips = trades.loc[trades['profit'] < 0, 'pips'].sum()
    
    # Estadístiques de ratxes
    max_wins, max_wins_profit = max_consecutive_wins(trades)
    max_profit, max_profit_trades = max_consecutive_profit(trades)

    # TODO: Sharpe ratio → ara està posat fix, cal calcular-lo amb retorns reals
    sharpe_ratio = 0.14  
    
    # % de temps al mercat
    trading_activity = get_trading_activity(trades)

    # TODO: max_deposit_load → està a 0, s’hauria d’extreure de dades de compte
    max_deposit_load = 0 
    
    # Temps de l'última operació
    date_last_trade = max(trades['entry_time'].max(), trades['exit_time'].max())
    latest_trade = datetime.now() - date_last_trade
    days = latest_trade.days
    hours = latest_trade.seconds // 3600
    minutes = latest_trade.seconds // 60
    if days >= 1:
        latest_trade = f'{days} days ago'
    elif hours >= 1:
        latest_trade = f'{hours} hours ago'
    else:
        latest_trade = f'{minutes} minutes ago'
    
    # Promig d'operacions per setmana
    trades_per_week = len(trades) / ((date_last_trade - signal.entry_time.min()).days / 7)

    # Temps de mitjana en mercat per operació
    avg_holding_time = (signal.exit_time - signal.entry_time).mean()
    avg_holding_time = f'{avg_holding_time.days} days and {int(avg_holding_time.seconds/3600)} hours'
    
    # ---- Altres mètriques ----
    recovery_factor = 26.38  # TODO: posar càlcul correcte
    long_trades = sum(trades['type'] == 'Long')
    short_trades = sum(trades['type'] == 'Short')
    profit_factor = 2.69  # TODO: posar càlcul correcte
    expected_payoff = trades.profit.mean()
    avg_profit = trades.loc[trades.profit >= 0, 'profit'].mean()
    avg_loss = trades.loc[trades.profit < 0, 'profit'].mean()

    # Ratxes de pèrdues
    max_losses, max_loss = max_consecutive_losses(trades)
    max_loss, max_loss_trades = max_consecutive_loss(trades)
    
    # Creixement (ara placeholders)
    monthly_growth = '1.90%'
    annual_forecast = '22.17%'

    # ---- Taula amb Plotly ----
    fig = go.Figure(data=[go.Table(
        cells=dict(
            values=[
                ['Trades:', 'Profit Trades:', 'Loss Trades:', 'Best trade:', 'Worst trade:', 'Gross Profit:', 'Gross Loss:',
                 'Maximum consecutive wins:', 'Maximal consecutive profit:', 'Sharpe Ratio:', 'Trading activity:',
                 'Max deposit load:', 'Latest trade:', 'Trades per week:', 'Avg holding time:'],
                [f'{total_trades}', 
                 f'{profit_trades} ({profit_trades / total_trades * 100:.2f}%)', 
                 f'{loss_trades} ({loss_trades / total_trades * 100:.2f}%)', 
                 f'{best_trade:.2f} USD', 
                 f'{worst_trade:.2f} USD', 
                 f'{gross_profit:.2f} USD ({gross_profit_pips:.1f} pips)',
                 f'{gross_loss:.2f} USD ({gross_loss_pips:.1f} pips)',
                 f'{max_wins} ({max_wins_profit:.2f} USD)', 
                 f'{max_profit:.2f} USD ({max_profit_trades})', 
                 'XX',  # TODO: Sharpe real
                 f'{trading_activity:.2f}%', 
                 f'{max_deposit_load*100:.2f}%',
                 latest_trade, 
                 f'{trades_per_week:.1f}', 
                 avg_holding_time],
                ['Recovery Factor:', 'Long Trades:', 'Short Trades:', 'Profit Factor:', 'Expected Payoff:', 'Average Profit:',
                 'Average Loss:', 'Maximum consecutive losses:', 'Maximal consecutive loss:', 'Monthly growth:',
                 'Annual Forecast:', 'Algo trading:'],
                ['XX',  # TODO: Recovery factor real
                 f'{long_trades} ({long_trades / total_trades * 100:.2f}%)', 
                 f'{short_trades} ({short_trades / total_trades * 100:.2f}%)', 
                 'XX',  # TODO: Profit factor real
                 f'{expected_payoff:.2f} USD', 
                 f'{avg_profit:.2f} USD', 
                 f'{avg_loss:.2f} USD', 
                 f'{max_losses} ({max_loss:.2f} USD)',
                 f'{max_loss:.2f} USD ({max_loss_trades})', 
                 monthly_growth, 
                 annual_forecast, 
                 '100%'  # TODO: Si tens camp "algo_trading"
                ]
            ], 
            align=['left', 'right', 'left', 'right']
        )
    )])
    
    # Estètica de la taula
    fig.update_layout(
        title='Statistics', title_x=0.5,
        margin=dict(l=20, r=20, t=40, b=20),
        height=450
    )
        
    filename = os.path.join(path_results,"estadístiques")
    fig.write_html(f'{filename}.html')
    fig.write_image(f'{filename}.png', scale=2)
    
    # Mostrar la taula
    fig.show()

"""
def get_statistics_from_book(book_temp, details, year=None):

    '''
    Returns a dictionary the main statistics from the book
    '''
    
    if (year != None) & (year != 'All'):
        year = int(year)
        book_temp = book_temp[book_temp.entry_time.dt.year==year].reset_index(drop=True)
    else:
        book_temp = book_temp.reset_index(drop=True)
    
    if len(book_temp)>0:
        InitialCapital = book_temp.Balance.loc[0] - book_temp.profit.loc[0]
        TotalNetProfit = book_temp.profit.sum(); TotalNetProfitPerc = TotalNetProfit/InitialCapital*100
        TotalTrades = len(book_temp)
        LongTrades = sum(book_temp['type'] == 'Long'); ShortTrades = sum(book_temp['type'] == 'Short')
        GrossProfit = book_temp[book_temp.profit>0].profit.sum(); GrossLoss = book_temp[book_temp.profit<0].profit.sum()
        ExpectedPayOff = book_temp.profit.mean()
        ShortPositionsWon = sum((book_temp.type=='Short')&(book_temp.profit>0))
        if ShortTrades>0:
            ShortPositionsWonPerc = round(ShortPositionsWon/ShortTrades*100,2)
        else:
            ShortPositionsWonPerc = round(0.,2)
        LongPositionsWon = sum((book_temp.type=='Long')&(book_temp.profit>0))
        if LongTrades>0:
            LongPositionsWonPerc = round(LongPositionsWon/LongTrades*100,2)
        else:
            LongPositionsWonPerc = round(0.,2)
        ProfitTrades = sum(book_temp.profit>0); ProfitTradesPerc = ProfitTrades/TotalTrades*100
        LossTrades = sum(book_temp.profit<=0); LossTradesPerc = LossTrades/TotalTrades*100
        LargestProfitTrade = book_temp.profit.max()
        LargestLosstrade = book_temp.profit.min()
        AverageProfitTrade = book_temp[book_temp.profit>0].profit.mean()
        AverageLosstrade = book_temp[book_temp.profit<=0].profit.mean()
        #Absolute drawdown is the difference between the initial deposit and the smallest value of the equity    
        AbsoluteDrawdown = round(max((1-book_temp.Balance.min()/InitialCapital)*100, 0),2)
        
        #Relative drawdown is a ratio between the maximal drawdown and the value of the corresponding local maximum of the equity. 
        #This coefficient shows losses, in percents of equity, experienced by an Expert Advisor;
        #This only has sense for a position!!!
        RelativeDrawDown = 0.00#(1-book_temp.Balance.min()/book_temp.Balance.max())*100
        
        book_temp['profit %'] = book_temp.profit/book_temp.Balance.shift(1).fillna(InitialCapital)
        
        #Maximal drawdown is the maximal difference between one of the local maximums and the subsequent minimum of the equity; 
        MaximalDrawDown_temp = 0
        MaximalDrawDown = 0
        for i in range(len(book_temp)):
            book_temp['dd_temp'] = book_temp['profit'].rolling(window=i+1).sum()
            book_temp['dd_temp %'] = book_temp['profit %'].rolling(window=i+1).sum()
            dd_temp_porc = book_temp['dd_temp %'].min()
            #print(i,MaximalDrawDown_temp)
            if dd_temp_porc < MaximalDrawDown_temp:
                MaximalDrawDown_temp = dd_temp_porc
                n_trades = i+1
                top_index = book_temp[book_temp['dd_temp %'] == dd_temp_porc].index[0]
                BalanceMaxDrawDown = book_temp.Balance.shift(1).fillna(InitialCapital).loc[top_index-n_trades+1]
                BalanceMinDrawDown = book_temp.Balance.loc[top_index]
                EntryTimeDrawDown = book_temp[top_index-n_trades:(top_index+1)].entry_time.min()
                ExitTimeDrawDown = book_temp[top_index-n_trades:(top_index+1)].entry_time.max()
                
                MaximalDrawDown = (BalanceMaxDrawDown - BalanceMinDrawDown)/BalanceMaxDrawDown*100
                
        
        SharpeRatio = book_temp.profit.mean()/book_temp.profit.std()
        ProfitFactor = -book_temp[book_temp.profit>0].profit.mean()/book_temp[book_temp.profit<0].profit.mean()

        
        statistics = {'Symbol': details['Symbol'], 
                      'PipPoint': details['PipPoint'],
                      'Spread': details['Spread'], 
                      'SwapLong': details['DailySwap']['Long'],
                      'SwapShort': details['DailySwap']['Short'],
                      'InitialCapital': InitialCapital,
                      'TotalNetProfit': TotalNetProfit, 'TotalNetProfitPerc': TotalNetProfitPerc,
                      'TotalTrades': TotalTrades, 'LongTrades': LongTrades, 'ShortTrades': ShortTrades,
                      'GrossProfit': GrossProfit, 'GrossLoss': GrossLoss, 
                      'ExpectedPayOff': ExpectedPayOff, 
                      'LongPositionsWon': LongPositionsWon, 'LongPositionsWonPerc': LongPositionsWonPerc,
                      'ShortPositionsWon': ShortPositionsWon, 'ShortPositionsWonPerc': ShortPositionsWonPerc, 
                      'ProfitTrades': ProfitTrades, 'ProfitTradesPerc': ProfitTradesPerc,
                      'LossTrades': LossTrades, 'LossTradesPerc': LossTradesPerc,
                      'LargestProfitTrade': LargestProfitTrade, 'LargestLosstrade': LargestLosstrade,
                      'AverageProfitTrade': AverageProfitTrade, 'AverageLosstrade': AverageLosstrade,
                      'AbsoluteDrawdown': AbsoluteDrawdown, 'MaximalDrawDown': MaximalDrawDown,
                      'SharpeRatio': SharpeRatio, 'ProfitFactor': ProfitFactor,
                      'AvgHoldingTime': (book_temp.exit_time - book_temp.entry_time).mean()}
                        
        return statistics



def print_statistics(statistics):

    '''
    Prints on the screen the statistics on a easy readable form
    '''

    Symbol = statistics['Symbol'];
    PipPoint = statistics['PipPoint'];
    Spread = statistics['Spread'];
    SwapLong = statistics['SwapLong'];
    SwapShort = statistics['SwapShort'];
    
    InitialCapital = statistics['InitialCapital'];
    TotalNetProfit = statistics['TotalNetProfit']; 
    TotalNetProfitPerc = statistics['TotalNetProfitPerc'];
    TotalTrades = statistics['TotalTrades'];  
    LongTrades = statistics['LongTrades']; 
    ShortTrades = statistics['ShortTrades'];
    GrossProfit = statistics['GrossProfit']; 
    GrossLoss = statistics['GrossLoss'];
    ExpectedPayOff = statistics['ExpectedPayOff'];
    LongPositionsWon = statistics['LongPositionsWon']; 
    LongPositionsWonPerc = statistics['LongPositionsWonPerc'];
    ShortPositionsWon = statistics['ShortPositionsWon']; 
    ShortPositionsWonPerc = statistics['ShortPositionsWonPerc'];
    ProfitTrades = statistics['ProfitTrades']; 
    ProfitTradesPerc = statistics['ProfitTradesPerc'];
    LossTrades = statistics['LossTrades']; 
    LossTradesPerc = statistics['LossTradesPerc'];
    LargestProfitTrade = statistics['LargestProfitTrade']; 
    LargestLosstrade = statistics['LargestLosstrade'];
    AverageProfitTrade = statistics['AverageProfitTrade']; 
    AverageLosstrade = statistics['AverageLosstrade'];
    AbsoluteDrawdown = statistics['AbsoluteDrawdown']; 
    MaximalDrawDown = statistics['MaximalDrawDown'];
    SharpeRatio = statistics['SharpeRatio'];
    ProfitFactor = statistics['ProfitFactor'];
    AvgHoldingTime = statistics['AvgHoldingTime'];
    
    row_format = '{0:2}{1:45}{2:45}{3:45}'
    print(row_format.format('','Currency: {}'.format(Symbol),
                            'Spread: {:0.2f}'.format(Spread),
                            'Swap: {:0.2f} long {:0.2f} short').format(SwapLong, SwapShort))
    print(row_format.format('','Initial Deposit: {:0.2f}€'.format(InitialCapital),
                            '',
                            ''))
    print(row_format.format('','Total Net Profit: {:0.2f}€ ({:0.2f}%)'.format(TotalNetProfit, TotalNetProfitPerc),
                            '',
                            ''))
    print(row_format.format('','Total trades: {}'.format(TotalTrades),
                            'Long: {}'.format(LongTrades),
                            'Short: {}'.format(ShortTrades)))
    print(row_format.format('','Expected Pay Off: {:0.2f}€'.format(ExpectedPayOff), 
                            'Gross Profit: {:0.2f}€'.format(GrossProfit),
                            'Gross Loss: {:0.2f}€'.format(GrossLoss)))
    print(row_format.format('','Sharpe Ratio: {:0.2f}'.format(SharpeRatio),
                            'Long Positions Won: {} ({:0.2f}%)'.format(LongPositionsWon,LongPositionsWonPerc),
                            'Short Positions Won: {} ({:0.2f}%)'.format(ShortPositionsWon,ShortPositionsWonPerc)))
    print(row_format.format('','Profit Factor: {:0.2f}'.format(ProfitFactor),
                            'Profit Trades: {} ({:0.2f}%)'.format(ProfitTrades,ProfitTradesPerc),
                            'Loss Trades: {} ({:0.2f}%)'.format(LossTrades, LossTradesPerc)))
    print(row_format.format('','Largest',
                            'Profit Trade: {:0.2f}€'.format(LargestProfitTrade),
                            'Loss Trade: {:0.2f}€'.format(LargestLosstrade)))
    print(row_format.format('','Average',
                            'Profit Trade: {:0.2f}€'.format(AverageProfitTrade),
                            'Loss Trade: {:0.2f}€'.format(AverageLosstrade)))
    print(row_format.format('','Absolute DD: {:0.2f} %'.format(AbsoluteDrawdown),
                            'Maximal DD: {:0.2f} %'.format(MaximalDrawDown),
                            ''))
    print(row_format.format('','Avg Holding Time: {}'.format(AvgHoldingTime),
                            '',
                            ''))

"""