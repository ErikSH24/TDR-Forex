import os
import plotly.graph_objs as go
from plotly.subplots import make_subplots

def plot_balance(order_book, InitialDeposit, path_results):
    """
    Genera gràfics de balance i volum a partir del llibre d'ordres.
    
    Args:
        order_book: DataFrame amb les operacions (resultat del backtest)
        InitialDeposit: Capital inicial
        path_results: Path on guardar els resultats
    """
    if len(order_book) > 0:
        total_profit = order_book.profit.sum()
        title = f'Initial Deposit {InitialDeposit:0.2f}€ - Total Profit: {total_profit:0.2f}€'

        # Crear subplots (2 files, 1 columna)
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            row_heights=[0.6, 0.4],
            vertical_spacing=0.05
        )

        # Gràfic de Balance (fila 1)
        fig.add_trace(go.Scatter(
            x=order_book.exit_time,
            y=order_book.Balance,
            mode='lines',
            name='Balance',
            line=dict(color='blue', width=2)
        ), row=1, col=1)

        # Gràfic de Volume (fila 2)
        fig.add_trace(go.Bar(
            x=order_book.exit_time,
            y=order_book['lot_size'],
            name='Volume',
            marker_color='orange'
        ), row=2, col=1)

        # Layout
        fig.update_layout(
            height=600,
            title_text=title,
            title_x=0.5,
            font=dict(size=12),
            plot_bgcolor='white',
            showlegend=True
        )

        # Eixos
        fig.update_xaxes(showline=True, linewidth=1, linecolor='black', row=1, col=1, showticklabels=True)
        fig.update_xaxes(showline=True, linewidth=1, linecolor='black', row=2, col=1, title_text="Exit Time")
        fig.update_yaxes(showline=True, linewidth=1, linecolor='black', row=1, col=1, title_text="Balance (€)")
        fig.update_yaxes(showline=True, linewidth=1, linecolor='black', row=2, col=1, title_text="Lot Size")
        
        filename = os.path.join(path_results, "balance_chart")
        fig.write_html(f'{filename}.html')
        fig.write_image(f'{filename}.png', scale=2, width=1000, height=600)
        fig.show()
        
        print(f"✅ Gràfic guardat a: {filename}.html/png")
    else:
        print("⚠️  No hi ha dades per mostrar gràfic")
