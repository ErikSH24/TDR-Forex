import os
import plotly.graph_objs as go
from plotly.subplots import make_subplots

def plot_balance(book, InitialDeposit, path_results):
    if len(book) > 0:
        title = 'Initial Deposit {:0.2f}€ - Total Profit: {:0.2f}€'.format(InitialDeposit, book.profit.sum())

        # Crear subplots (2 files, 1 columna)
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            row_heights=[0.6, 0.4],
            vertical_spacing=0.05
        )

        # Gràfic de Balance (fila 1)
        fig.add_trace(go.Scatter(
            x=book.exit_time,
            y=book.Balance,
            mode='lines',
            name='Balance'
        ), row=1, col=1)

        # Gràfic de Volume (fila 2)
        fig.add_trace(go.Bar(
            x=book.exit_time,
            y=book.volume,
            name='Volume'
        ), row=2, col=1)

        # Layout
        fig.update_layout(
            height=500,
            title_text=title,
            title_x=0.5,
            font=dict(size=10),
            plot_bgcolor='white'
        )

        # Eixos
        fig.update_xaxes(showline=True, linewidth=1, linecolor='black', row=1, col=1, showticklabels=True)
        fig.update_xaxes(showline=True, linewidth=1, linecolor='black', row=2, col=1, title_text="Exit Time")
        fig.update_yaxes(showline=True, linewidth=1, linecolor='black', row=1, col=1, title_text="Balance")
        fig.update_yaxes(showline=True, linewidth=1, linecolor='black', row=2, col=1, title_text="Volume")
        filename = os.path.join(path_results,"balance")
        fig.write_html(f'{filename}.html')
        fig.write_image(f'{filename}.png', scale=2)
        fig.show()