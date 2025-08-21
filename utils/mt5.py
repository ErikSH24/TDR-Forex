import pandas as pd
import MetaTrader5 as mt5

def init_mt5():
    """
    Aquesta funció intenta establir connexió amb el terminal MetaTrader 5 (MT5).
    Si falla la inicialització, mostra el codi d'error, tanca la connexió
    i atura l'execució del programa per evitar errors posteriors.
    """    
    if not mt5.initialize():
        print(f"initialize() failed, error code: {mt5.last_error()}")
        mt5.shutdown()

def get_positions_df():
    """
    Obté les posicions obertes actuals de MetaTrader 5 i les retorna en forma de DataFrame de pandas.
    """
    init_mt5()
    positions = mt5.positions_get()
    mt5.shutdown()

    if positions == None:
        df = "No positions, error code={}".format(mt5.last_error())
    elif len(positions) == 0:
        df = pd.DataFrame(columns=['ticket', 'time', 'time_msc', 'time_update', 'time_update_msc', 'type',
                                   'magic', 'identifier', 'reason', 'volume', 'price_open', 'sl', 'tp',
                                   'price_current', 'swap', 'profit', 'symbol', 'comment', 'external_id'])
    elif len(positions) > 0:
        df = pd.DataFrame(list(positions), columns=positions[0]._asdict().keys())
        df['time'] = pd.to_datetime(df['time'], unit='s')
        df['time_msc'] = pd.to_datetime(df['time_msc'], unit='ms')

    return df

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

def OpenBuyOrder(argSymbol, argLotSize, argSlippage, argMagicNumber, argComment):
    """
    Obre una ordre de compra (BUY) a MetaTrader 5.

    - Inicialitza la connexió amb MT5, envia una ordre de compra de mercat i després la tanca.
    - Els paràmetres són:
        argSymbol:      símbol (ex: "EURUSD")
        argLotSize:     mida de la posició en lots
        argSlippage:    desviació màxima permesa en punts
        argMagicNumber: identificador únic de l’estratègia/EA
        argComment:     comentari associat a l’ordre
    - Si l’ordre s’executa correctament → retorna el número d’ordre i mostra per pantalla
      el retcode, el deal i l’order.
    - Si l’ordre falla: retorna None i mostra el codi d’error (retcode).
    """
    init_mt5()
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": argSymbol,
        "volume": argLotSize,
        "type": mt5.ORDER_TYPE_BUY,
        "price": mt5.symbol_info_tick(argSymbol).ask,
        "deviation": argSlippage,
        "magic": argMagicNumber,
        "comment": argComment,
    }
    result = mt5.order_send(request)
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        order = None
        print("OpenBuyOrder failed, retcode={}".format(result.retcode))
    else:
        order = result.order
        print('retcode={} - deal={} - order={}'.format(result.retcode, result.deal, order))
    mt5.shutdown()
    
    return order

def OpenSellOrder(argSymbol, argLotSize, argSlippage, argMagicNumber, argComment):
    """
    Obre una ordre de venda (SELL) al mercat amb MetaTrader5.

    Paràmetres:
    - argSymbol: símbol del parell de divises (ex: "EURUSD")
    - argLotSize: mida del lot a negociar
    - argSlippage: desviació màxima per acceptar el preu d’execució
    - argMagicNumber: identificador únic de l’estratègia
    - argComment: comentari opcional per a la transacció

    Retorna:
    - El número de l’ordre si s’ha creat correctament
    - None si hi ha error en l’operació
    """
    
    # Inicialitzar connexió amb MT5
    init_mt5()
    
    # Crear la sol·licitud de l’ordre de venda
    request = {
        "action": mt5.TRADE_ACTION_DEAL,  # ordre de mercat immediata
        "symbol": argSymbol,               # símbol a negociar
        "volume": argLotSize,              # mida del lot
        "type": mt5.ORDER_TYPE_SELL,       # tipus SELL
        "price": mt5.symbol_info_tick(argSymbol).bid,  # preu de venda actual
        "deviation": argSlippage,          # slippage permès
        "magic": argMagicNumber,           # identificador únic
        "comment": argComment,             # comentari opcional
    }

    # Enviar l’ordre
    result = mt5.order_send(request)
    
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        # Error en executar l’ordre
        order = None
        print("OpenSellOrder failed, retcode={}".format(result.retcode))
    else:
        order = result.order
        print('retcode={} - deal={} - order={}'.format(result.retcode, result.deal, order))
    
    mt5.shutdown()
    
    return order

def Modify_SL_and_TP(argTicket, argSL, argTP, argSlippage):
    """
    Modifica el Stop Loss (SL) i Take Profit (TP) d'una posició existent a MetaTrader5.

    Paràmetres:
    - argTicket: número de ticket de la posició a modificar
    - argSL: nou valor de Stop Loss
    - argTP: nou valor de Take Profit
    - argSlippage: desviació màxima per acceptar el preu (actualment no s'utilitza en la sol·licitud)

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

        # Crear la sol·licitud per tancar la posició
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "position": position_ticket,
            "symbol": position_symbol,
            "volume": position_volume,
            "deviation": argSlippage,
            "magic": position_magic,
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

##########################################################################
# PENDING ORDERS
##########################################################################
def get_orders_df():
    """
    Obté les ordres pendents actuals de MetaTrader 5 i les retorna en forma de DataFrame de pandas.
    """
    init_mt5()
    orders = mt5.orders_get()
    mt5.shutdown()

    if orders == None:
        df = "No orders, error code={}".format(mt5.last_error())
    elif len(orders) == 0:
        df = pd.DataFrame(columns=['ticket', 'time_setup', 'time_setup_msc', 'time_done', 'time_done_msc',
                                   'time_expiration', 'type', 'type_time', 'type_filling', 'state',
                                   'magic', 'position_id', 'position_by_id', 'reason', 'volume_initial',
                                   'volume_current', 'price_open', 'sl', 'tp', 'price_current',
                                   'price_stoplimit', 'symbol', 'comment', 'external_id'])
    elif len(orders) > 0:
        df = pd.DataFrame(list(orders), columns=orders[0]._asdict().keys())
        df['time_setup'] = pd.to_datetime(df['time_setup'], unit='s')
        df['time_setup_msc'] = pd.to_datetime(df['time_setup_msc'], unit='ms')
        # df['time_done'] = pd.to_datetime(df['time_done'], unit='s')
        # df['time_done_msc'] = pd.to_datetime(df['time_done_msc'], unit='ms')
        # df['time_expiration'] = pd.to_datetime(df['time_expiration'], unit='s')

    return df

def get_order_info(argTicket):
    """
    Retorna la informació d'una ordre pendent específica (per ticket) en forma de DataFrame de pandas.

    """

    init_mt5()
    orders = mt5.orders_get()
    mt5.shutdown()

    if orders == None:
        # Error en la connexió o en l’obtenció de dades
        df = "No orders, error code={}".format(mt5.last_error())
    elif len(orders) == 0:
        # No hi ha ordres pendents → retornem DataFrame buit amb les columnes predefinides
        df = pd.DataFrame(columns=['ticket', 'time_setup', 'time_setup_msc', 'time_done', 'time_done_msc',
                                   'time_expiration', 'type', 'type_time', 'type_filling', 'state',
                                   'magic', 'position_id', 'position_by_id', 'reason', 'volume_initial',
                                   'volume_current', 'price_open', 'sl', 'tp', 'price_current',
                                   'price_stoplimit', 'symbol', 'comment', 'external_id'])
    elif len(orders) > 0:
        # Convertim la llista d’ordres a DataFrame
        df = pd.DataFrame(list(orders), columns=orders[0]._asdict().keys())

        # Filtrar només l’ordre amb el ticket indicat
        df = df[df.ticket == argTicket].reset_index(drop=True)

        # Convertir camps de temps a datetime
        df['time_setup'] = pd.to_datetime(df['time_setup'], unit='s')
        df['time_setup_msc'] = pd.to_datetime(df['time_setup_msc'], unit='ms')
        # df['time_done'] = pd.to_datetime(df['time_done'], unit='s')
        # df['time_done_msc'] = pd.to_datetime(df['time_done_msc'], unit='ms')
        # df['time_expiration'] = pd.to_datetime(df['time_expiration'], unit='s')

    return df


def OpenPendingOrder(argSymbol, argLotSize, argPrice, argSL, argTP, argSlippage, argMagicNumber, argComment, argOrderType):
    """
    Obre una ordre pendent (Pending Order) a MetaTrader5 segons els paràmetres especificats.

    Paràmetres:
    - argSymbol: símbol del mercat (ex. "EURUSD")
    - argLotSize: mida del lot de l'ordre
    - argPrice: preu de l'ordre pendent
    - argSL: stop loss
    - argTP: take profit
    - argSlippage: desviació màxima acceptada per al preu d'execució
    - argMagicNumber: MagicNumber associat a l'ordre
    - argComment: comentari per identificar l'ordre
    - argOrderType: tipus d'ordre pendent (string)
        Possibles valors:
        - 'ORDER_TYPE_BUY_LIMIT'
        - 'ORDER_TYPE_SELL_LIMIT'
        - 'ORDER_TYPE_BUY_STOP'
        - 'ORDER_TYPE_SELL_STOP'

    Retorna:
    - Resultat de mt5.order_send amb informació de l'ordre enviada
    """

    # Inicialitzar connexió amb MT5
    init_mt5()

    # Obtenir preus actuals i informació del símbol
    current_bid_price = mt5.symbol_info_tick(argSymbol).bid
    current_ask_price = mt5.symbol_info_tick(argSymbol).ask
    point = mt5.symbol_info(argSymbol).point
    digits = mt5.symbol_info(argSymbol).digits
    offset = 0  # Opcional, es pot usar per ajustar la condició de preu

    # Preparar sol·licitud base
    request = {
        "action": mt5.TRADE_ACTION_PENDING,
        "symbol": argSymbol,
        "volume": argLotSize,
        "type": mt5.ORDER_TYPE_BUY,  # Serà modificat segons argOrderType
        "price": mt5.symbol_info_tick(argSymbol).ask,
        "sl": argSL,
        "tp": argTP,
        "deviation": argSlippage,
        "magic": argMagicNumber,
        "comment": argComment,
    }

    # Validar preu segons tipus d'ordre pendent
    valid_price = False
    if (argOrderType == 'ORDER_TYPE_BUY_LIMIT') and (argPrice < current_ask_price - offset*point):
        valid_price = True
        request['type'] = mt5.ORDER_TYPE_BUY_LIMIT
        request['price'] = round(argPrice, digits)
    elif (argOrderType == 'ORDER_TYPE_SELL_LIMIT') and (argPrice > current_bid_price + offset*point):
        valid_price = True
        request['type'] = mt5.ORDER_TYPE_SELL_LIMIT
        request['price'] = round(argPrice, digits)
    elif (argOrderType == 'ORDER_TYPE_BUY_STOP') and (argPrice > current_ask_price + offset*point):
        valid_price = True
        request['type'] = mt5.ORDER_TYPE_BUY_STOP
        request['price'] = round(argPrice, digits)
    elif (argOrderType == 'ORDER_TYPE_SELL_STOP') and (argPrice < current_bid_price - offset*point):
        valid_price = True
        request['type'] = mt5.ORDER_TYPE_SELL_STOP
        request['price'] = round(argPrice, digits)
    else:
        print("{} not valid or Price doesn't match the offset condition".format(argOrderType))

    # Enviar ordre si el preu és vàlid
    if valid_price:
        result = mt5.order_send(request)
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            print("{} failed, retcode={}".format(argOrderType, result.retcode))
        else:
            print('Pending order placed successfully: retcode={} - deal={} - order={}'.format(result.retcode, result.deal, result.order))

    # Tancar connexió amb MT5
    mt5.shutdown()

    return result

def ModifyPendingOrder(argticket, argPrice, argSL, argTP):
    """
    Modifica una ordre pendent en MT5.
    
    Paràmetres:
    - argticket: ticket de l'ordre pendent a modificar
    - argPrice: nou preu
    - argSL: nou Stop Loss
    - argTP: nou Take Profit
    """
    # Inicialitza MT5
    init_mt5()
    
    request = {
        "action": mt5.TRADE_ACTION_MODIFY,
        "order": argticket,
        "price": argPrice,
        "sl": argSL,
        "tp": argTP
    }
    
    result = mt5.order_send(request)
    
    if result is None:
        print(f"ModifyPendingOrder failed: order_send returned None, error code={mt5.last_error()}")
    elif result.retcode != mt5.TRADE_RETCODE_DONE:
        print(f"Order {argticket} modified unsuccessfully, retcode={result.retcode}")
    else:
        print(f"Order {argticket} modified successfully: retcode={result.retcode}")
    
    mt5.shutdown()

def ClosePendingOrderByTicket(argTicket):
    """
    Tanca (elimina) una ordre pendent a MT5 per ticket.
    
    Paràmetres:
    - argTicket: ticket de l'ordre pendent a tancar
    """
    # Inicialitza MT5
    init_mt5()
    
    request = {
        "action": mt5.TRADE_ACTION_REMOVE,
        "order": argTicket,
    }
    
    result = mt5.order_send(request)
    
    # Comprovació per evitar AttributeError si order_send retorna None
    if result is None:
        print(f"Delete order {argTicket} failed: order_send returned None, error code={mt5.last_error()}")
    elif result.retcode != mt5.TRADE_RETCODE_DONE:
        print(f"Delete order {argTicket} failed, retcode={result.retcode}")
    else:
        print(f"Order {argTicket} deleted successfully: retcode={result.retcode}")
    
    mt5.shutdown()

def CloseAllPendingOrder(argSymbol, argMagicNumber):
    """
    Tanca (elimina) totes les ordres pendents d'un símbol i MagicNumber específics.

    Paràmetres:
    - argSymbol: símbol de les ordres pendents a tancar
    - argMagicNumber: MagicNumber de les ordres pendents a tancar
    """
    init_mt5()
    orders = mt5.orders_get()

    if orders is None:
        print(f"No es poden obtenir ordres: error code={mt5.last_error()}")
        mt5.shutdown()
        return

    if len(orders) == 0:
        print("No hi ha ordres pendents a tancar.")
    else:
        for order in orders:
            ticket = order.ticket
            symbol = order.symbol
            magic = order.magic

            if (symbol == argSymbol) & (magic == argMagicNumber):
                request = {
                    "action": mt5.TRADE_ACTION_REMOVE,
                    "order": ticket,
                }

                result = mt5.order_send(request)

                # Comprovació robusta per evitar AttributeError
                if result is None:
                    print(f"Delete order {ticket} failed: order_send returned None, error code={mt5.last_error()}")
                elif result.retcode != mt5.TRADE_RETCODE_DONE:
                    print(f"Delete order {ticket} failed, retcode={result.retcode}")
                else:
                    print(f"Order {ticket} deleted successfully: retcode={result.retcode}")

    mt5.shutdown()