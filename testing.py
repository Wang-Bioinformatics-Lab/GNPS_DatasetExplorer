import dash
from dash import dcc, html
from dash.dependencies import Input, Output

app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    dcc.Link(html.Button('Open Website'), id='open-link', href='', target='_blank'),
    dcc.Dropdown(
        id='server-dropdown',
        options=[
            {'label': 'USA', 'value': 'usa'},
            {'label': 'Germany (DE)', 'value': 'de'}
        ],
        value='usa'
    )
])

@app.callback(
    Output('url', 'search'),
    [Input('server-dropdown', 'value')]
)
def update_url(selected_server):
    if selected_server == 'usa':
        return 'https://www.google.com'
    elif selected_server == 'de':
        return 'https://www.google.de'
    return ''

@app.callback(
    Output('open-link', 'href'),
    [Input('url', 'search')]
)
def update_link(href):
    return href

if __name__ == '__main__':
    app.run_server(debug=True)
