import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import dash
from dash import dcc, html, Input, Output, State
import plotly.graph_objects as go
from urllib.parse import parse_qs, unquote
from common import constants

# 1. 配置数据库连接 (请替换为你的实际账号密码)
engine = create_engine(constants.dbStr)


# --- 1. 配色方案 (固定 10 组高对比度颜色) ---
CODE_COLORS = [
    {'up': '#0984e3', 'down': '#fdcb6e'},  # 蓝/黄
    {'up': '#27ae60', 'down': '#e056fd'},  # 绿/紫
    {'up': '#e67e22', 'down': '#34495e'},  # 橙/灰
    {'up': '#6c5ce7', 'down': '#badc58'},  # 紫/嫩绿
    {'up': '#d63031', 'down': '#7ed6df'},  # 红/青
    {'up': '#1dd1a1', 'down': '#ee5253'},  # 碧/红
    {'up': '#576574', 'down': '#feca57'},  # 炭/金
    {'up': '#5f27cd', 'down': '#ff9f43'},  # 靛/橘
    {'up': '#00d2d3', 'down': '#ff6b6b'},  # 蓝绿/粉红
    {'up': '#222f3e', 'down': '#10ac84'}  # 深蓝/薄荷
]


# --- 2. 数据获取 (支持动态传参) ---
def get_data(engine, codes, start_yr, end_yr):
    all_results = {}
    valid_codes = []
    for code in codes:
        sql = f"SELECT trade_date, close FROM daily_hfq_data WHERE code='{code}' AND YEAR(trade_date) BETWEEN {start_yr} AND {end_yr} ORDER BY trade_date"
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)
        if df.empty: continue

        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df['mmdd'] = df['trade_date'].dt.strftime('%m-%d')

        matrix = []
        for y in range(start_yr, end_yr + 1):
            y_df = df[df['trade_date'].dt.year == y]
            y_seg = y_df[(y_df['mmdd'] >= '01-01') & (y_df['mmdd'] <= '12-31')]
            if y_seg.empty: continue
            matrix.append((y_seg.set_index('mmdd')['close'] / y_seg['close'].iloc[0] * 100).rename(y))

        if matrix:
            all_results[code] = pd.concat(matrix, axis=1).sort_index().ffill().bfill().mean(axis=1)
            valid_codes.append(code)
    return pd.DataFrame(all_results), valid_codes


# --- 3. Dash 实例 (支持多页面路由) ---
app = dash.Dash(__name__, suppress_callback_exceptions=True)

# 基础布局：包含一个 URL 监听器
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


# --- 4. 路由回调：根据 URL 参数生成内容 ---
@app.callback(Output('page-content', 'children'), [Input('url', 'search')])
def display_page(search):
    # 解析 URL 参数，例如: ?codes=000001.ZS,399006.SZ&start=2021&end=2026
    params = parse_qs(search.lstrip('?'))
    codes_str = params.get('codes', ['000001.ZS'])[0]
    codes = [c.strip() for c in codes_str.split(',')]
    start_yr = int(params.get('start', [2021])[0])
    end_yr = int(params.get('end', [2026])[0])

    # 建立数据库连接 (建议在回调内或使用连接池)
    df_avg, actual_codes = get_data(engine, codes, start_yr, end_yr)

    if df_avg.empty:
        return html.H1("未找到数据，请检查 URL 参数")

    # 构建 Trace
    traces = []
    for i, code in enumerate(actual_codes):
        series = df_avg[code]
        colors = CODE_COLORS[i % len(CODE_COLORS)]
        for j in range(1, len(series)):
            is_up = series.iloc[j] >= series.iloc[j - 1]
            traces.append(go.Scatter(
                x=series.index[j - 1:j + 1], y=series.iloc[j - 1:j + 1],
                mode='lines', line=dict(color=colors['up'] if is_up else colors['down'], width=2),
                legendgroup=code, name=code, showlegend=True if j == 1 else False, hoverinfo='none'
            ))

    # 返回完整页面布局
    return html.Div(style={'display': 'flex', 'height': '98vh'}, children=[
        # 左侧固定顺序面板
        html.Div(
            style={'width': '320px', 'padding': '20px', 'backgroundColor': 'white', 'borderRight': '1px solid #eee',
                   'overflowY': 'auto'}, children=[
                html.H3("📌 季节性看板", style={'margin': '0 0 10px 0'}),
                html.P(f"正在查看: {len(actual_codes)} 个品种", style={'fontSize': '12px', 'color': '#999'}),
                html.Hr(),
                # 存储数据矩阵，供回调使用 (隐藏容器)
                dcc.Store(id='matrix-storage', data={'df': df_avg.to_json(), 'codes': actual_codes}),
                html.Div(id='hover-content-fixed')
            ]),
        # 右侧图表
        html.Div(style={'flex': '1', 'padding': '15px'}, children=[
            dcc.Graph(id='graph-fixed', style={'height': '100%'}, figure={
                'data': traces,
                'layout': go.Layout(
                    xaxis={'type': 'category', 'nticks': 15, 'showspikes': True, 'spikemode': 'across'},
                    yaxis={'title': '归一化均值 (1-1=100)'},
                    hovermode='x', template='plotly_white', legend={'orientation': 'h', 'y': 1.05}
                )
            })
        ])
    ])


# --- 5. 交互回调：保持固定顺序显示 ---
@app.callback(
    Output('hover-content-fixed', 'children'),
    [Input('graph-fixed', 'hoverData')],
    [State('matrix-storage', 'data')]
)
def update_side_panel(hoverData, stored_data):
    if not hoverData or not stored_data: return "鼠标悬浮查看详情..."

    mmdd = hoverData['points'][0]['x']
    df = pd.read_json(stored_data['df'])
    codes = stored_data['codes']  # 这里拿到的是原始顺序列表

    rows = [html.Div(f"📅 {mmdd}", style={'fontSize': '24px', 'fontWeight': 'bold', 'marginBottom': '20px'})]

    # 严格按照 codes 原始列表顺序渲染，不使用 sort_values
    for i, code in enumerate(codes):
        val = df.loc[mmdd, code]
        ret = (val / 100) - 1
        colors = CODE_COLORS[i % len(CODE_COLORS)]

        rows.append(html.Div(style={
            'marginBottom': '12px', 'padding': '10px', 'borderRadius': '6px',
            'borderLeft': f'6px solid {colors["up"]}', 'backgroundColor': '#fcfcfc',
            'boxShadow': '0 1px 3px rgba(0,0,0,0.05)'
        }, children=[
            html.Div(code, style={'fontWeight': 'bold', 'fontSize': '14px'}),
            html.Div([
                html.Span(f"净值: {val:.2f}", style={'marginRight': '15px', 'color': '#666'}),
                html.Span(f"{ret:+.2%}", style={'color': '#e74c3c' if ret > 0 else '#27ae60', 'fontWeight': 'bold'})
            ])
        ]))
    return html.Div(rows)


if __name__ == '__main__':
    # 启动时，只需运行脚本
    app.run(debug=True, port=8050)