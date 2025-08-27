import pandas as pd
from tqdm import tqdm
import numpy as np
import os
from datetime import datetime, timedelta
import MetaTrader5 as mt5
from utils.mt5 import init_mt5
from utils.strategies import CalcLotSize
from utils.metrics import *

import plotly.graph_objs as go

def calculate_swap(entry_time, exit_time, swap_rate, lot_size, triple_wednesday=True):
    """
    Calcula el swap (overnight financing) per una operació, considerant swap triple els dimecres.
    
    Parameters:
    entry_time (datetime): Data i hora d'entrada de l'operació
    exit_time (datetime): Data i hora de sortida de l'operació
    swap_rate (float): Taxa de swap per lot per nit (en pips/diners)
    lot_size (float): Mida del lot de l'operació
    triple_wednesday (bool): Si és True, aplica swap triple els dimecres
    
    Returns:
    float: Valor del swap en la moneda de base del compte
    """
    
    try:
        # Validar inputs
        if entry_time >= exit_time:
            return 0.0  # Operació intraday, no swap
        
        if lot_size <= 0:
            raise ValueError("La mida del lot ha de ser positiva")
        
        # Convertir a datetime si són strings
        if isinstance(entry_time, str):
            entry_time = pd.to_datetime(entry_time)
        if isinstance(exit_time, str):
            exit_time = pd.to_datetime(exit_time)
        
        # Calcular nits completes (swap s'aplica cada nit a les 22:00 GMT)
        swap_days = 0
        triple_swap_days = 0
        
        # Començar des de la nit següent a l'entrada
        current_date = entry_time.date() + timedelta(days=1)
        
        while current_date <= exit_time.date():
            # Verificar si la nit anterior era dimecres (swap triple)
            previous_day = current_date - timedelta(days=1)
            
            if triple_wednesday and previous_day.weekday() == 2:  # 2 = dimecres
                triple_swap_days += 1
            else:
                swap_days += 1
            
            current_date += timedelta(days=1)
                
        # Calcular el swap
        swap_value = (swap_rate * swap_days * lot_size) + (swap_rate * 3 * triple_swap_days * lot_size)
        return round(swap_value, 2)
        
    except Exception as e:
        print(f"Error calculant swap: {e}")
        return 0.0

def backtest_strategy(data, initial_capital, symbol, magic_number, strategy_parameters, is_online_mode, base_path):
    """
    Backtest general per a estratègies que tanquen només per SL/TP/condicions.
    """
    # Configuració
    results_path = os.path.join(base_path, f'output/Forex/backtesting/{magic_number}/{symbol}')
    os.makedirs(results_path, exist_ok=True)
    
    # Taula on posarem totes les operacions
    order_book_columns = ['order_id', 'position_type', 'lot_size', 'symbol', 'entry_time', 
                         'entry_price', 'stop_loss', 'take_profit', 'exit_time', 'exit_price', 
                         'commission', 'swap', 'profit', 'pips', 'concurrent_positions']
    order_book = pd.DataFrame(columns=order_book_columns)
    
    # Variable on actualitzarem el capital després de tancar una posició
    current_equity = initial_capital
    # Llista on posarem totes les operacions obertes
    open_positions = []
    
    # Paràmetres de l'estratègia
    use_dynamic_lot_size = strategy_parameters['DynamicLotSize']
    risk_percentage = strategy_parameters['EquityPercent']
    fixed_lot_size = strategy_parameters['FixedLotSize']
    max_concurrent_trades = strategy_parameters['MaxOpenTrades']
    max_minutes_open = strategy_parameters['MaxMinutesOpenTrades']
    min_minutes_between_trades = strategy_parameters['MinBetweenTrades'] or 0
    take_profit_short = strategy_parameters['TP_short']
    take_profit_long = strategy_parameters['TP_long']
    stop_loss_long = strategy_parameters['SL_long']
    stop_loss_short = strategy_parameters['SL_short']
    
    # Informació del símbol
    symbols_info_path = os.path.join(base_path, 'output', 'Forex', 'info_symbols.csv')
    symbols_info_df = pd.read_csv(symbols_info_path)
    symbols_info_dict = symbols_info_df.set_index('name').T.to_dict()
    symbol_info = symbols_info_dict.get(symbol, {})
    
    pip_value = symbol_info['point']
    spread_cost = symbol_info['spread']
    swap_rates = {'Buy': symbol_info['swap_long'], 'Sell': symbol_info['swap_short']}
    
    print(f'Backtest per {symbol}')
    print(f'Capital inicial: ${initial_capital:,.2f}')
    
    # Filtrar senyals vàlides
    valid_entries = data[data['signal'].isin([1, -1])].copy()
    print(f'   Senyals d\'entrada vàlides: {len(valid_entries)}')
    
    order_counter = 1
    
    # Processar cada possible entrada
    for entry_index, entry_row in tqdm(valid_entries.iterrows(), total=len(valid_entries), desc="Processant entrades"):
        entry_time = entry_row['time']
        signal_value = entry_row['signal']
        position_type = 'Buy' if signal_value == 1 else 'Sell'
        
        # Verificar límit de posicions obertes
        current_open_positions = len([p for p in open_positions if p['exit_time'] is None])
        if current_open_positions >= max_concurrent_trades:
            continue
            
        # Verificar temps mínim entre operacions
        if open_positions:
            last_exit_times = [p['exit_time'] for p in open_positions if p['exit_time'] is not None]
            if last_exit_times:
                last_exit_time = max(last_exit_times)
                minutes_since_last = (entry_time - last_exit_time).total_seconds() / 60
                if minutes_since_last < min_minutes_between_trades:
                    continue
        
        # Configurar preus d'entrada i stops
        if position_type == 'Buy':
            stop_loss_pips = stop_loss_long
            take_profit_pips = take_profit_long
            entry_price = entry_row['open'] + spread_cost * pip_value
            sl_price = entry_price - stop_loss_pips * pip_value
            tp_price = entry_price + take_profit_pips * pip_value
        else:
            stop_loss_pips = stop_loss_short
            take_profit_pips = take_profit_short
            entry_price = entry_row['open']
            sl_price = entry_price + stop_loss_pips * pip_value
            tp_price = entry_price - take_profit_pips * pip_value
        
        # Calcular mida del lot
        lot_size = CalcLotSize(use_dynamic_lot_size, current_equity, risk_percentage, stop_loss_pips, fixed_lot_size)
        
        # Buscar punt de sortida
        future_data = data[data['time'] >= entry_time].copy()
        
        # Condicions de sortida
        if position_type == 'Buy':
            sl_condition = future_data['low'] <= sl_price
            tp_condition = future_data['high'] >= tp_price
            if 'cond_close_long' in future_data.columns:
                cond_condition = future_data['cond_close_long'] == 1
            else:
                cond_condition = pd.Series(False, index=future_data.index)
        else:
            sl_condition = future_data['high'] >= sl_price
            tp_condition = future_data['low'] <= tp_price
            if 'cond_close_short' in future_data.columns:
                cond_condition = future_data['cond_close_short'] == 1
            else:
                cond_condition = pd.Series(False, index=future_data.index)
        
        # Sortida per temps límit
        if max_minutes_open is not None:
            time_condition = (future_data['time'] - entry_time).dt.total_seconds() / 60 > max_minutes_open
        else:
            time_condition = pd.Series(False, index=future_data.index)
        
        # Combinar condicions de sortida
        exit_condition = sl_condition | tp_condition | cond_condition | time_condition
        exit_points = future_data[exit_condition]
        
        if not exit_points.empty:
            exit_point = exit_points.iloc[0]
            exit_time_point = exit_point['time']
            
            if sl_condition.loc[exit_point.name]:
                exit_type = 'SL'
                exit_price = sl_price
                pips = -stop_loss_pips
            elif tp_condition.loc[exit_point.name]:
                exit_type = 'TP' 
                exit_price = tp_price
                pips = take_profit_pips
            elif time_condition.loc[exit_point.name]:
                exit_type = 'TIME'
                if position_type == 'Buy':
                    exit_price = exit_point['open']
                else:
                    exit_price = exit_point['open'] + spread_cost * pip_value
                pips = (exit_price - entry_price) / pip_value * signal_value
            else:
                exit_type = 'COND'
                if position_type == 'Buy':
                    exit_price = exit_point['open']  # Long surt al bid
                else:
                    exit_price = exit_point['open'] + spread_cost * pip_value  # Short surt al ask
                pips = (exit_price - entry_price) / pip_value * signal_value
            
            swap_amount = calculate_swap(entry_time, exit_time_point, swap_rates[position_type], lot_size, triple_wednesday=True)
            
            #Càlcul de profit
            if position_type == 'Buy':
                profit = (exit_price - entry_price) / pip_value * lot_size# + swap_amount
            else:
                profit = (entry_price - exit_price) / pip_value * lot_size# + swap_amount
            
            # Afegir al llibre d'ordres
            order_book.loc[len(order_book)] = [
                order_counter, position_type, lot_size, symbol, entry_time,
                entry_price, sl_price, tp_price, exit_time_point, exit_price,
                0, swap_amount, profit, pips, current_open_positions + 1
            ]
            
            # Actualitzar estat
            current_equity += profit
            order_counter += 1
            
            # Actualitzar llista de posicions obertes
            open_positions.append({
                'entry_time': entry_time,
                'exit_time': exit_time_point,
                'type': position_type,
                'size': lot_size,
                'profit': profit
            })
    
    # Resultats finals
    if not order_book.empty:
        order_book['Balance'] = order_book['profit'].cumsum() + initial_capital
        order_book['Return %'] = (order_book['Balance'] - initial_capital) / initial_capital * 100
    
    # Guardar resultats
    order_book_file = os.path.join(results_path, f'order_book_{symbol}_{magic_number}.csv.gz')
    order_book.to_csv(order_book_file, index=False, compression='gzip')
    
    print(f"Backtest completat")
    print(f"   Operacions executades: {len(order_book)}")
    print(f"   Capital final: ${current_equity:,.2f}")
    
    return symbol_info, order_book


def get_monthly_returns(order_book, path_results):
    """
    Calcula i mostra els retorns mensuals a partir del llibre d'ordres.
    
    Args:
        order_book: DataFrame amb les operacions (resultat del backtest)
        path_results: Path on guardar els resultats
    """
    # Verificar si hi ha dades suficients
    if len(order_book) == 0:
        print("No hi ha dades al llibre d'ordres")
        return None
    
    # Inicialitzar el balance de partida
    previous_balance = order_book.iloc[0].Balance

    # Generar mesos entre primera i última trade
    date_range = pd.date_range(order_book.entry_time.min().replace(day=1),
                               order_book.exit_time.max().replace(day=1),
                               freq="MS").strftime("%Y-%m").tolist()

    monthly_returns = []
    
    for i in range(len(date_range)-1):
        year, month = map(int, date_range[i].split("-"))
        next_year, next_month = map(int, date_range[i+1].split("-"))
        
        mask = (
            (order_book.exit_time >= datetime(year, month, 1)) &
            (order_book.exit_time < datetime(next_year, next_month, 1))
        )
        
        if mask.sum() > 0:
            temp = order_book.loc[mask].sort_values("exit_time")
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

    monthly_pivot = monthly_returns.pivot_table(index="year", columns="month", values="return", aggfunc="sum").fillna(0)

    for month in range(1, 13):
        if month not in monthly_pivot.columns:
            monthly_pivot[month] = 0
        
        monthly_pivot = monthly_pivot.reindex(columns=range(1, 13)).fillna(0)

    month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    monthly_pivot.columns = month_names[:len(monthly_pivot.columns)]

    # Retorn anual i total
    monthly_pivot["Year"] = ((1 + monthly_pivot[month_names]/100).prod(axis=1) - 1) * 100
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
        #height=700
    )

    filename = os.path.join(path_results, "monthly_returns")
    fig.write_html(f'{filename}.html')
    fig.write_image(f'{filename}.png', scale=2)
    fig.show()

def get_statistics_full(order_book, path_results, symbol_details=None, year=None):
    """
    Calcula estadístiques completes d'un backtest i mostra una taula interactiva amb Plotly.
    Organitzada com la imatge de referència.
    """
    trades = order_book.copy()
    
    # Filtre per any si toca
    if year is not None and year != 'All':
        trades = trades[trades.entry_time.dt.year == int(year)].reset_index(drop=True)
    
    total_trades = len(trades)
    if total_trades == 0:
        print("⚠️ No hi ha operacions per generar estadístiques")
        return
    
    # --- Càlcul d'estadístiques ---
    profit_trades = sum(trades['profit'] > 0)
    loss_trades = sum(trades['profit'] <= 0)
    best_trade = trades['profit'].max()
    worst_trade = trades['profit'].min()

    gross_profit = trades.loc[trades['profit'] > 0, 'profit'].sum()
    gross_profit_pips = trades.loc[trades['profit'] > 0, 'pips'].sum()
    gross_loss = trades.loc[trades['profit'] <= 0, 'profit'].sum()
    gross_loss_pips = trades.loc[trades['profit'] <= 0, 'pips'].sum()

    # Funcions auxiliars (assumint que les tens implementades)
    max_wins, max_wins_profit = max_consecutive_wins(trades)
    max_profit, max_profit_trades = max_consecutive_profit(trades)
    max_losses, max_loss = max_consecutive_losses(trades)
    max_loss_sum, max_loss_trades = max_consecutive_loss(trades)

    sharpe_ratio = calculate_sharpe(trades)
    trading_activity = get_trading_activity(trades)
    max_drawdown_balance = calculate_max_drawdown(trades) * 100
    
    # Drawdown addicional (assumint funcions existents)
    recovery_factor = calculate_recovery_factor(trades)
    dict_DDs = calculate_all_drawdowns(trades)
    maximal_drawdown_value = dict_DDs["maximal_drawdown_value"]
    maximal_drawdown_percent = dict_DDs["maximal_drawdown_percent"]
    relative_drawdown_balance = dict_DDs["relative_drawdown_balance"]
    relative_drawdown_equity = dict_DDs["relative_drawdown_equity"]

    long_trades = sum(trades['position_type'] == 'Long')
    short_trades = sum(trades['position_type'] == 'Short')

    profit_factor = abs(gross_profit / gross_loss) if gross_loss != 0 else float('inf')
    expected_payoff = trades['profit'].mean()
    avg_profit = trades.loc[trades['profit'] > 0, 'profit'].mean()
    avg_loss = trades.loc[trades['profit'] <= 0, 'profit'].mean()

    # --- Temps ---
    avg_holding_time = (trades['exit_time'] - trades['entry_time']).mean()
    avg_hours = avg_holding_time.total_seconds() / 3600
    avg_holding_time_str = f'{avg_hours:.1f} hours'

    date_last_trade = trades['exit_time'].max()
    latest_trade_delta = datetime.now() - date_last_trade
    days = latest_trade_delta.days
    if days >= 1:
        latest_trade_str = f'{days} days ago'
    else:
        hours = latest_trade_delta.seconds // 3600
        latest_trade_str = f'{hours} hours ago'

    weeks_trading = max((date_last_trade - trades['entry_time'].min()).days / 7, 1)
    trades_per_week = total_trades / weeks_trading

    # --- Capital i rendiment ---
    InitialCapital = trades['Balance'].iloc[0] - trades['profit'].iloc[0]
    TotalNetProfit = trades['profit'].sum()

    # Rendiment per mes i forecast anual
    months = max(((date_last_trade - trades['entry_time'].min()).days / 30), 1)
    MonthlyGrowth = (TotalNetProfit / InitialCapital / months) * 100
    AnnualForecast = MonthlyGrowth * 12

    # --- Taula organitzada com la imatge ---
    fig = go.Figure(data=[go.Table(
        columnwidth=[1, 1.5, 1, 1.5],
        header=dict(values=["", "Value", "", "Value"],
                    fill_color='lightgrey', 
                    align='center',
                    font=dict(size=12, color='black', weight='bold')),
        cells=dict(values=[
            # Columna 1 (Mètriques)
            ["Trades:", "Profit Trades:", "Loss Trades:", "Best trade:", "Worst trade:", 
             "Gross Profit:", "Gross Loss:", "Max consecutive wins:", 
             "Max consecutive profit:", "Sharpe Ratio:", "Trading activity:",
             "Max deposit load:", "Latest trade:", "Avg holding time:",
             "Drawdown by balance:", "Absolute:", "Maximal:"],
            
            # Columna 2 (Valors)
            [f"{total_trades}", f"{profit_trades} ({profit_trades/total_trades*100:.2f}%)", 
             f"{loss_trades} ({loss_trades/total_trades*100:.2f}%)", f"{best_trade:.2f} USD", 
             f"{worst_trade:.2f} USD", f"{gross_profit:.2f} USD", 
             f"{gross_loss:.2f} USD", f"{max_wins} ({max_wins_profit:.2f} USD)",
             f"{max_profit:.2f} USD ({max_profit_trades})", f"{sharpe_ratio:.2f}", 
             f"{trading_activity:.2f}%", f"{max_drawdown_balance:.2f}%", latest_trade_str,
             avg_holding_time_str, "", f"{maximal_drawdown_value:.2f} USD", f"{maximal_drawdown_percent:.2f}% USD"],
            
            # Columna 3 (Mètriques)
            ["Recovery Factor:", "Long Trades:", "Short Trades:", "Profit Factor:", 
             "Expected Payoff:", "Average Profit:", "Average Loss:", 
             "Max consecutive losses:", "Max consecutive loss:", "Monthly growth:", 
             "Annual Forecast:", "Algo trading:", "Trades per week:", "", "Relative drawdown:", "By balance:",
             "By equity:"],
            
            # Columna 4 (Valors)
            [f"{recovery_factor:.2f}", f"{long_trades} ({long_trades/total_trades*100:.2f}%)", 
             f"{short_trades} ({short_trades/total_trades*100:.2f}%)", f"{profit_factor:.2f}", 
             f"{expected_payoff:.2f} USD", f"{avg_profit:.2f} USD", f"{avg_loss:.2f} USD",
             f"{max_losses} ({max_loss:.2f} USD)", f"{max_loss_sum:.2f} USD ({max_loss_trades})",
             f"{MonthlyGrowth:.2f}%", f"{AnnualForecast:.2f}%", "100%", f"{trades_per_week:.1f}", "", "", 
             f"{relative_drawdown_balance:.2f}%", f"{relative_drawdown_equity:.2f}%"]
        ],
        fill_color=[['white', '#f0f8ff'] * 10],  # Colors alternats
        align=['left', 'right', 'left', 'right'],
        font=dict(size=11),
        height=30)
    )])

    fig.update_layout(
        title=dict(text='Estadístiques', x=0.5, font=dict(size=18, weight='bold')),
        margin=dict(l=20, r=20, t=80, b=20),
        height=750,
        #width=1300
    )

    filename = os.path.join(path_results, "statistics_summary")
    fig.write_html(f'{filename}.html')
    fig.write_image(f'{filename}.png', scale=2, width=1300, height=650)
    fig.show()
