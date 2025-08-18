import os
import pandas as pd
from tqdm import tqdm
from utils.strategies import CalcLotSize

def get_orders_M1(data_M1, InitialDeposit, symbol, strategy, strategy_params, online, path_root):

    # Paràmetres de l'estratègia
    DynamicLotSize = strategy_params["DynamicLotSize"]
    EquityPercent = strategy_params["EquityPercent"]
    FixedLotSize = strategy_params["FixedLotSize"]
    MaxOpenTrades = strategy_params["MaxOpenTrades"]
    MaxMinutesOpenTrades = strategy_params["MaxMinutesOpenTrades"]
    MinBetweenTrades = strategy_params["MinBetweenTrades"]

    # Si no hi ha temps mínim entre trades, s'estableix a zero
    if MinBetweenTrades == None:
        MinBetweenTrades = 0

    # Configuració dels paràmetres del símbol (online/offline)
    if online:
        # Mode online: obté dades directament de MT5
        init_mt5()
        PipPoint = mt5.symbol_info(symbol).point * 10
        spread = mt5.symbol_info(symbol).spread * 0.1
        DailySwap = {
            'Long': mt5.symbol_info(symbol).swap_long * 0.1,
            'Short': mt5.symbol_info(symbol).swap_short * 0.1,
        }
        mt5.shutdown()
    else:
        # Mode offline: llegeix dades des d'arxiu CSV
        file = os.path.join(path_root, "output", "Forex", "info_symbols.csv")
        info_symbols = pd.read_csv(file)
        
        s = info_symbols.loc[info_symbols['name'] == symbol].iloc[0]
        
        PipPoint = float(s["point"]) * 10
        spread = float(s["spread"]) * 0.1
        DailySwap = {
            'Long': float(s["swap_long"]) * 0.1,
            'Short': float(s["swap_short"]) * 0.1,
        }

    # Diccionari amb els detalls del símbol per a referència futura
    dict_details = {
        "Symbol": symbol,
        "PipPoint": PipPoint,
        "Spread": spread,
        "DailySwap": DailySwap
    }

    # Imprimeix la configuració inicial
    print(f"Strategy: {strategy}")
    print(f"Symbol: {symbol}")
    print(f"Initial deposit: {InitialDeposit}")
    print(f"DynamicLotSize={DynamicLotSize} - EquityPercent={EquityPercent} - FixedFixedLotSize={FixedLotSize}")
    print(f"PipPoint: {PipPoint}")
    print(f"Spread: {spread:.2f}")
    print(f"SwapLong: {dict_details['DailySwap']['Long']:.2f}")
    print(f"SwapShort: {dict_details['DailySwap']['Short']:.2f}")

    # Prepara les ordres a executar (filtra senyals diferents de zero)
    orders = data_M1.loc[data_M1.signal != 0,['time', 'open', 'signal']].reset_index(drop=True)

    # Inicialitzem paràmetres
    n_orders = 1
    exit_time_prev = None
    AccountEquity = InitialDeposit

    # Columnes del llibre d'ordres (trade book)
    cols = ['order', 'type', 'volume', 'symbol', 'entry_time', 'entry_price', 'SL', 'TP',
           'exit_time', 'exit_price', 'comissions', 'swap', 'profit', 'n_open_trades']
    book = pd.DataFrame(columns=cols)  # Inicialitza el llibre d'ordres buit
    n_order_counter = 1

    # Itera sobre les ordres
    for n_order in tqdm(range(len(orders))):
        type_bool = orders.signal.loc[n_order]
        
        # Determina el tipus d'ordre
        if type_bool == -1:
            type_ = "Short"
        if type_bool == 1:
            type_ = "Long"
        
        daily_swap = DailySwap[type_]
        entry_time = orders.time.loc[n_order]
        if exit_time_prev == None:
            minutes_elapsed = 1e09
        else:
            minutes_elapsed = (entry_time-exit_time_prev).days*24*60 + (entry_time-exit_time_prev).seconds/60
        if exit_time_prev != None:
            if entry_time < exit_time_prev:
                n_orders += 1
            else:
                n_orders = 1
        #print(f"n_orders: {n_orders} - MaxOpMaxOpenTrades: {MaxOpenTrades} - minutes_elapsed: {minutes_elapsed} - MinBetweenTrades: {MinBetweenTrades}")
        if (n_orders <= MaxOpenTrades) & (minutes_elapsed > MinBetweenTrades):
            cols_strategy = [
                'time', 'open', 'high', 'low', 'close', 'signal', 'SL_long', 'TP_long',
                'SL_short', 'TP_short', 'cond_close_long', 'cond_close_short'
            ] + [col for col in ['entry_price_long', 'entry_price_short'] if col in data_M1.columns]
            df_temp = data_M1.loc[data_M1.time>=entry_time, cols_strategy].head(60*24*30).reset_index(drop=True)
            
            if type_ == 'Long':
                StopLoss = df_temp.SL_long.loc[0]
                TakeProfit = df_temp.TP_long.loc[0]
                if 'entry_price_long' in data_M1.columns:
                    entry_price = df_temp.entry_price_long.loc[0] + spread*PipPoint
                else:
                    entry_price = orders.open.loc[n_order] + spread*PipPoint
                df_temp['entry_price'] = entry_price
                df_temp['profit_pips'] = (df_temp.close-df_temp.entry_price)/PipPoint
                df_temp['profit_pips_max'] = (df_temp.high-df_temp.entry_price)/PipPoint
                df_temp['drawdown_pips_max'] = (df_temp.entry_price-df_temp.low)/PipPoint
                df_temp['cond_close'] = df_temp.cond_close_long

            if type_ == 'Short':
                StopLoss = df_temp.SL_short.loc[0]
                TakeProfit = df_temp.TP_short.loc[0]
                if 'entry_price_short' in data_M1.columns:
                    entry_price = df_temp.entry_price_short.loc[0]
                else:
                    entry_price = orders.open.loc[n_order]
                df_temp['entry_price'] = entry_price
                df_temp['profit_pips'] = (df_temp.entry_price-df_temp.close)/PipPoint - spread * PipPoint
                df_temp['profit_pips_max'] = (df_temp.entry_price-df_temp.low)/PipPoint - spread * PipPoint
                df_temp['drawdown_pips_max'] = (df_temp.high-df_temp.entry_price)/PipPoint - spread * PipPoint
                df_temp['cond_close'] = df_temp.cond_close_short

            if MaxMinutesOpenTrades != None:
                df_temp['cond_close'] = ((df_temp.cond_close) | (df_temp.index>MaxMinutesOpenTrades))*1

            LotSize = CalcLotSize(DynamicLotSize, AccountEquity, EquityPercent, StopLoss, FixedLotSize)
            #print(f"Account equity: {AccountEquity} - Lot Size: {LotSize}")

            df_temp['cond_stop_loss'] = (df_temp.drawdown_pips_max > StopLoss)*1
            df_temp['cond_take_profit'] = (df_temp.profit_pips_max > TakeProfit)*1
            df_temp['cond_close_order'] = ((df_temp.cond_close)|(df_temp.cond_stop_loss)|(df_temp.cond_take_profit))*1
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
                    profit = -StopLoss
                elif cond_close == 1:
                    #We exit at the next bar
                    exit_time = temp.time.loc[1]
                    exit_price = temp.open.loc[1] + spread*PipPoint*(type_ == 'Short')
                    profit = (exit_price-entry_price)/PipPoint*type_bool
                elif cond_TP == 1:
                    #We exit at the current bar
                    exit_time = temp.time.loc[1]
                    exit_price = entry_price + TakeProfit*PipPoint*type_bool
                    profit = TakeProfit

                days = (exit_time.date()  - entry_time.date()).days

                #Adding swap
                swap = round(days*daily_swap*LotSize/0.10,2)
                profit = round(profit*LotSize/0.10+swap,2)
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

    book.to_csv(path_results + '/book_{}_{}.csv.gz'.format(symbol, strategy), index=False, compression='gzip')
    np.save(path_results + '/details_{}_{}.npy'.format(symbol, strategy), dict_details)
    return book, dict_details
    
    
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



def print_statistics(statistics, filename=None):

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