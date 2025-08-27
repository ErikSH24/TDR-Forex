import pandas as pd
import MetaTrader5 as mt5
from datetime import datetime, timezone, timedelta
import pytz

def init_mt5():
    """
    Aquesta funció intenta establir connexió amb el terminal MetaTrader 5 (MT5).
    Si falla la inicialització, mostra el codi d'error, tanca la connexió
    i atura l'execució del programa per evitar errors posteriors.
    """    
    if not mt5.initialize():
        print(f"initialize() failed, error code: {mt5.last_error()}")
        mt5.shutdown()

def get_server_timezone():
    """Detecta automáticamente la timezone del servidor MT5"""
    if not mt5.initialize():
        print("Error inicializando MT5")
        return pytz.utc
    
    try:
        symbol = "EURUSD"
        tick = mt5.symbol_info_tick(symbol)
        
        if not tick:
            print("No se pudo obtener tick")
            return pytz.utc
        
        # Hora del servidor desde timestamp
        server_time_utc = datetime.utcfromtimestamp(tick.time)
        
        # Hora actual UTC
        current_utc = datetime.utcnow()
        
        # Calcular diferencia en horas
        time_diff = (server_time_utc - current_utc).total_seconds() / 3600
        offset_hours = round(time_diff)
        
        # Validar que el offset sea razonable
        if not -12 <= offset_hours <= 14:
            print(f"Offset fuera de rango ({offset_hours}), usando UTC")
            return pytz.utc
        
        #print(f"Timezone del servidor detectado: UTC{offset_hours:+d}")
        
        # Crear timezone correspondiente
        if offset_hours >= 0:
            return pytz.timezone(f'Etc/GMT-{offset_hours}')
        else:
            return pytz.timezone(f'Etc/GMT+{abs(offset_hours)}')
            
    except Exception as e:
        print(f"Error detectando timezone: {e}")
        return pytz.utc
    finally:
        mt5.shutdown()
        
def get_positions_df(MagicNumber=None, symbol=None):
    """
    Obté les posicions obertes actuals de MetaTrader 5 i les retorna com a DataFrame de pandas.
    
    Args:
        MagicNumber (int, optional): Filtra per número màgic específic.
        symbol (str, optional): Filtra per símbol específic.
    
    Returns:
        pandas.DataFrame: DataFrame amb les posicions obertes. Retorna DataFrame buit si no hi ha posicions o si hi ha problemes de connexió.
    """
    cols = ['ticket', 'time', 'time_msc', 'time_update', 'time_update_msc', 'type',
            'magic', 'identifier', 'reason', 'volume', 'price_open', 'sl', 'tp',
            'price_current', 'swap', 'profit', 'symbol', 'comment', 'external_id']
    
    if not mt5.initialize():
        print(f"initialize() failed, error code: {mt5.last_error()}")
        return pd.DataFrame(columns=cols)
    
    try:
        positions = mt5.positions_get()
        
        if not positions:
            return pd.DataFrame(columns=cols)
        
        # Crear DataFrame
        df = pd.DataFrame(list(positions), columns=positions[0]._asdict().keys())
        
        # Convertir timestamps
        df['time'] = pd.to_datetime(df['time'], unit='s')
        df['time_update'] = pd.to_datetime(df['time_update'], unit='s')
        df['time_msc'] = pd.to_datetime(df['time_msc'], unit='ms')
        df['time_update_msc'] = pd.to_datetime(df['time_update_msc'], unit='ms')
        
        # Filtrar per magic number
        if MagicNumber is not None:
            df = df[df['magic'] == MagicNumber].copy()
        # Filtrar per magic number
        if symbol is not None:
            df = df[df['symbol'] == symbol].copy()
        
        return df[cols].reset_index(drop=True)
        
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame(columns=cols)
    finally:
        mt5.shutdown()

def get_position_info(argTicket):
    """
    Retorna la informació d'una posició oberta específica (per ticket) en forma de DataFrame de pandas.
    """

    init_mt5()
    positions = mt5.positions_get()
    mt5.shutdown()

    if positions == None:
        # Error en la connexió o en l’obtenció de dades
        df = "No positions, error code={}".format(mt5.last_error())
    elif len(positions) == 0:
        # No hi ha cap posició oberta → retornem DataFrame buit amb les columnes predefinides
        df = pd.DataFrame(columns=['ticket', 'time', 'time_msc', 'time_update', 'time_update_msc', 'type',
                                   'magic', 'identifier', 'reason', 'volume', 'price_open', 'sl', 'tp',
                                   'price_current', 'swap', 'profit', 'symbol', 'comment', 'external_id'])
    elif len(positions) > 0:
        df = pd.DataFrame(list(positions), columns=positions[0]._asdict().keys())
        df = df[df.ticket == argTicket].reset_index(drop=True)
        df['time'] = pd.to_datetime(df['time'], unit='s')
        df['time_msc'] = pd.to_datetime(df['time_msc'], unit='ms')

    return df

def get_deals_df(date_from=None, date_to=None, MagicNumber=None, symbol=None):
    """
    Obté els deals (operacions executades) de MetaTrader 5 i les retorna com a DataFrame de pandas.
    
    Args:
        date_from (datetime, optional): Data inicial per filtrar. Si és None, cerca dels últims 30 dies.
        date_to (datetime, optional): Data final per filtrar. Si és None, usa data actual.
        MagicNumber (int, optional): Filtra per número màgic específic.
        symbol (str, optional): Filtra per símbol específic (ex: 'EURUSD').
    
    Returns:
        pandas.DataFrame: DataFrame amb els deals. Retorna DataFrame buit si no n'hi ha.
    """
    # Columnes esperades per deals
    cols = ['ticket', 'order', 'time', 'time_msc', 'type', 'entry', 'magic', 
            'position_id', 'reason', 'volume', 'price', 'commission', 'swap', 
            'profit', 'fee', 'symbol', 'comment', 'external_id']
    
    #server_timezone = get_server_timezone()
    #now_server_time = datetime.now(server_timezone)
    now_server_time = datetime.now()
    # Dates per defecte
    if date_from is None:
        date_from = now_server_time - timedelta(days=30)
    if date_to is None:
        date_to = now_server_time + timedelta(days=1)
    
    if not mt5.initialize():
        print(f"initialize() failed, error code: {mt5.last_error()}")
        return pd.DataFrame(columns=cols)
    
    try:
        # Obtenir deals dins el rang de dates
        deals = mt5.history_deals_get(date_from, date_to)
        
        if deals is None:
            error_code = mt5.last_error()
            if error_code == 1:  # ERROR_SUCCESS: no hi ha deals
                return pd.DataFrame(columns=cols)
            else:
                print(f"Error obtenint deals: {error_code}")
                return pd.DataFrame(columns=cols)
        
        elif len(deals) == 0:
            return pd.DataFrame(columns=cols)
        
        else:
            # Crear DataFrame amb els deals
            df = pd.DataFrame(list(deals), columns=deals[0]._asdict().keys())
            df['time'] = pd.to_datetime(df['time'], unit='s')
            df['time_msc'] = pd.to_datetime(df['time_msc'], unit='ms')
            
            # Aplicar filtres
            if MagicNumber is not None:
                df = df[df['magic'] == MagicNumber].copy()
            if symbol is not None:
                df = df[df['symbol'] == symbol].copy()
            
            return df.reset_index(drop=True)
            
    except Exception as e:
        print(f"Excepció inesperada: {str(e)}")
        return pd.DataFrame(columns=cols)
    
    finally:
        try:
            mt5.shutdown()
        except:
            pass

def OpenBuyOrder(argSymbol, argLotSize, argSlippage, argMagicNumber, argComment, argSLPips=0, argTPPips=0):
    """
    Obre una ordre de compra (BUY) a MetaTrader 5 amb SL i TP en pips.
    
    Args:
        argSymbol:      símbol (ex: "EURUSD")
        argLotSize:     mida de la posició en lots
        argSlippage:    desviació màxima permesa en punts
        argMagicNumber: identificador únic de l'estratègia/EA
        argComment:     comentari associat a l'ordre
        argSLPips:      stop loss en pips (0 = sense SL)
        argTPPips:      take profit en pips (0 = sense TP)
    
    Returns:
        int: Número d'ordre si èxit, None si error
    """
    if not mt5.initialize():
        print(f"initialize() failed, error code: {mt5.last_error()}")
        mt5.shutdown()
        return None
        
    try:
        # Obtenir informació del símbol
        symbol_info = mt5.symbol_info_tick(argSymbol)
        symbol_info_detail = mt5.symbol_info(argSymbol)
        
        if symbol_info is None or symbol_info_detail is None:
            print(f"No es pot obtenir informació per {argSymbol}")
            return None
        
        current_price = symbol_info.ask
        point = symbol_info_detail.point
        digits = symbol_info_detail.digits
        
        # Calcular SL i TP en preu - manejar SL=0 i TP=0 correctament
        sl_price = 0.0
        tp_price = 0.0
        
        if argSLPips > 0:
            sl_price = round(current_price - argSLPips * point * 10, digits)
        
        if argTPPips > 0:
            tp_price = round(current_price + argTPPips * point * 10, digits)
        
        # Preparar la sol·licitud d'ordre - NO incloure SL/TP si són 0
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": argSymbol,
            "volume": argLotSize,
            "type": mt5.ORDER_TYPE_BUY,
            "price": current_price,
            "deviation": argSlippage,
            "magic": argMagicNumber,
            "comment": argComment,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_FOK
        }
        
        # Afegir SL i TP només si són majors que 0
        if argSLPips > 0:
            request["sl"] = sl_price
        if argTPPips > 0:
            request["tp"] = tp_price
        
        print(f"Obrint ordre BUY: {argSymbol} {argLotSize} lots")
        print(f"Preu: {current_price}")
        if argSLPips > 0:
            print(f"SL: {sl_price} ({argSLPips} pips)")
        if argTPPips > 0:
            print(f"TP: {tp_price} ({argTPPips} pips)")
        
        # Enviar l'ordre
        result = mt5.order_send(request)
        
        # Verificar que result no sigui None
        if result is None:
            print("Error: order_send() ha retornat None")
            return None
            
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            print(f"Error {result.retcode}: {get_retcode_description(result.retcode)}")
            return None
        else:
            print(f"Ordre executada correctament!")
            print(f"   Order: {result.order}, Deal: {result.deal}")
            if argSLPips > 0:
                print(f"   SL: {sl_price}")
            if argTPPips > 0:
                print(f"   TP: {tp_price}")
            return result.order
            
    except Exception as e:
        print(f"Excepció en OpenBuyOrder: {e}")
        import traceback
        traceback.print_exc()  # Això mostrarà el traceback complet
        return None
    finally:
        mt5.shutdown()

def OpenSellOrder(argSymbol, argLotSize, argSlippage, argMagicNumber, argComment, argSLPips=0, argTPPips=0):
    """
    Obre una ordre de venda (SELL) a MetaTrader 5 amb SL i TP en pips.
    
    Args:
        argSymbol:      símbol (ex: "EURUSD")
        argLotSize:     mida de la posició en lots
        argSlippage:    desviació màxima permesa en punts
        argMagicNumber: identificador únic de l'estratègia/EA
        argComment:     comentari associat a l'ordre
        argSLPips:      stop loss en pips (0 = sense SL)
        argTPPips:      take profit en pips (0 = sense TP)
    
    Returns:
        int: Número d'ordre si èxit, None si error
    """
    if not mt5.initialize():
        print(f"initialize() failed, error code: {mt5.last_error()}")
        mt5.shutdown()
        return None
        
    try:
        # Obtenir informació del símbol
        symbol_info = mt5.symbol_info_tick(argSymbol)
        symbol_info_detail = mt5.symbol_info(argSymbol)
        
        if symbol_info is None or symbol_info_detail is None:
            print(f"No es pot obtenir informació per {argSymbol}")
            return None
        
        # Per VENDES s'utilitza el preu BID
        current_price = symbol_info.bid
        point = symbol_info_detail.point
        digits = symbol_info_detail.digits
        
        # Calcular SL i TP en preu - PER VENDES és al revés que compres
        sl_price = 0.0
        tp_price = 0.0
        
        if argSLPips > 0:
            # Per VENDES: SL va PER DAMUNT del preu actual
            sl_price = round(current_price + argSLPips * point * 10, digits)
            # Validar SL - per vendes SL ha de ser MAJOR que preu actual
            if sl_price <= current_price:
                print(f"Error: SL ({sl_price}) ha de ser major que preu actual ({current_price}) per venda")
                return None
        
        if argTPPips > 0:
            tp_price = round(current_price - argTPPips * point * 10, digits)
            # Validar TP
            if tp_price >= current_price:
                print(f"Error: TP ({tp_price}) ha de ser menor que preu actual ({current_price}) per venda")
                return None
        
        # Preparar la sol·licitud d'ordre
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": argSymbol,
            "volume": argLotSize,
            "type": mt5.ORDER_TYPE_SELL,
            "price": current_price,
            "deviation": argSlippage,
            "magic": argMagicNumber,
            "comment": argComment,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_FOK
        }
        
        # Afegir SL i TP només si són majors que 0
        if argSLPips > 0:
            request["sl"] = sl_price
        if argTPPips > 0:
            request["tp"] = tp_price
        
        print(f"Obrint ordre SELL: {argSymbol} {argLotSize} lots")
        print(f"Preu: {current_price}")
        if argSLPips > 0:
            print(f"SL: {sl_price} ({argSLPips} pips)")
        if argTPPips > 0:
            print(f"TP: {tp_price} ({argTPPips} pips)")
        
        # Enviar l'ordre
        result = mt5.order_send(request)
        
        # Verificar que result no sigui None
        if result is None:
            print("Error: order_send() retornó None")
            return None
            
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            print(f"Error {result.retcode}: {get_retcode_description(result.retcode)}")
            return None
        else:
            print(f"Ordre de venda executada correctament!")
            print(f"   Order: {result.order}, Deal: {result.deal}")
            if argSLPips > 0:
                print(f"   SL: {sl_price}")
            if argTPPips > 0:
                print(f"   TP: {tp_price}")
            return result.order
            
    except Exception as e:
        print(f"Excepció en OpenSellOrder: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        mt5.shutdown()

def Modify_SL_and_TP(argTicket, argSL, argTP):
    """
    Modifica el Stop Loss (SL) i Take Profit (TP) d'una posició existent a MetaTrader5.

    Paràmetres:
    - argTicket: número de ticket de la posició a modificar
    - argSL: nou valor de Stop Loss
    - argTP: nou valor de Take Profit

    Retorna:
    - Cap valor. Mostra per pantalla si la modificació ha tingut èxit o ha fallat.
    """
    
    # Inicialitzar connexió amb MT5
    init_mt5()
    
    # Obtenir totes les posicions obertes
    positions = mt5.positions_get()
    selected_position = None
    
    # Buscar la posició amb el ticket indicat
    for position in positions:
        if str(argTicket) == str(position.ticket):
            selected_position = position
    
    if selected_position is not None:
        # Extreure informació de la posició seleccionada
        position_ticket = selected_position.ticket
        position_symbol = selected_position.symbol
        position_volume = selected_position.volume
        position_type = selected_position.type
        position_magic = selected_position.magic

        # Crear la sol·licitud de modificació de SL i TP
        request = {
            "action": mt5.TRADE_ACTION_SLTP,
            "position": position_ticket,
            "symbol": position_symbol,
            "volume": position_volume,
            "sl": argSL,
            "tp": argTP
        }
        
        # Enviar la sol·licitud
        result = mt5.order_send(request)
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            print("Change SL-TP for position {} failed, retcode={}".format(position_ticket, result.retcode))
        else:
            print('Change SL-TP {} successfully'.format(position_ticket))
    else:
        # El ticket indicat no existeix com a posició oberta
        print('Ticket {} is not a position'.format(argTicket))
    
    # Tancar connexió amb MT5
    mt5.shutdown()

def CloseOrderByTicket(argTicket, argSlippage):
    """
    Tanca una posició oberta a MetaTrader5 utilitzant el seu ticket.

    Paràmetres:
    - argTicket: número de ticket de la posició a tancar
    - argSlippage: desviació màxima acceptada per al preu de tancament

    Retorna:
    - Cap valor. Mostra per pantalla si el tancament ha tingut èxit o ha fallat.
    """
    
    # Inicialitzar connexió amb MT5
    init_mt5()
    
    # Obtenir totes les posicions obertes
    positions = mt5.positions_get()
    selected_position = None
    
    # Buscar la posició amb el ticket indicat
    for position in positions:
        if str(argTicket) == str(position.ticket):
            selected_position = position
    
    if selected_position is not None:
        # Extreure informació de la posició seleccionada
        position_ticket = selected_position.ticket
        position_symbol = selected_position.symbol
        position_volume = selected_position.volume
        position_type = selected_position.type
        position_magic = selected_position.magic
        position_comment = selected_position.comment

        # Crear la sol·licitud per tancar la posició
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "position": position_ticket,
            "symbol": position_symbol,
            "volume": position_volume,
            "deviation": argSlippage,
            "magic": position_magic,
            "comment": position_comment,
        }

        # Determinar tipus d'ordre per tancar correctament la posició
        if position_type == 0:  # Si és BUY
            request['price'] = mt5.symbol_info_tick(position_symbol).bid
            request['type'] = mt5.ORDER_TYPE_SELL
        else:  # Si és SELL
            request['price'] = mt5.symbol_info_tick(position_symbol).ask
            request['type'] = mt5.ORDER_TYPE_BUY

        # Enviar la sol·licitud de tancament
        result = mt5.order_send(request)
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            print("Position {} close failed, retcode={}".format(position_ticket, result.retcode))
        else:
            print('Ticket {} closed successfully'.format(position_ticket))
    else:
        # Si no es troba la posició amb aquest ticket
        print('Ticket {} is not a position'.format(argTicket))
    
    mt5.shutdown()

def CloseAllOrders(argSymbol, argMagicNumber, argSlippage):
    """
    Tanca totes les posicions obertes a MetaTrader5 que coincideixin amb un símbol i MagicNumber determinats.

    Paràmetres:
    - argSymbol: símbol del mercat (ex. "EURUSD")
    - argMagicNumber: MagicNumber associat a les posicions a tancar
    - argSlippage: desviació màxima acceptada per al preu de tancament

    Retorna:
    - Cap valor. Mostra per pantalla l'estat de cada tancament.
    """

    # Inicialitzar connexió amb MT5
    init_mt5()
    
    # Obtenir totes les posicions obertes
    positions = mt5.positions_get()
    
    # Iterar per totes les posicions obertes
    for i in range(len(positions)):
        position_ticket = positions[i].ticket
        position_symbol = positions[i].symbol
        position_magic = positions[i].magic
        position_volume = positions[i].volume
        position_type = positions[i].type

        # Només tancar si coincideix el símbol i el MagicNumber
        if (position_symbol == argSymbol) and (position_magic == argMagicNumber):
            
            # Preparar la sol·licitud de tancament
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "position": position_ticket,
                "symbol": position_symbol,
                "volume": position_volume,
                "deviation": argSlippage,
                "magic": position_magic,
            }

            # Determinar tipus d'ordre correcte per tancar
            if position_type == 0:  # BUY
                request['price'] = mt5.symbol_info_tick(argSymbol).bid
                request['type'] = mt5.ORDER_TYPE_SELL
            else:  # SELL
                request['price'] = mt5.symbol_info_tick(argSymbol).ask
                request['type'] = mt5.ORDER_TYPE_BUY

            # Enviar la sol·licitud
            result = mt5.order_send(request)
            if result.retcode != mt5.TRADE_RETCODE_DONE:
                print("Position {} close failed, retcode={}".format(position_ticket, result.retcode))
            else:
                print('Closed successfully: retcode={} - deal={} - order={}'.format(result.retcode, result.deal, result.order))
    
    # Tancar connexió amb MT5
    mt5.shutdown()