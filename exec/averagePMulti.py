import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import dash
from dash import dcc, html, Input, Output
import plotly.graph_objects as go

from common import constants

# 1. 配置数据库连接 (请替换为你的实际账号密码)
engine = create_engine(constants.dbStr)


# --- 1. 数据逻辑 (保持归一化) ---
def get_multi_seasonal_data(engine, codes, start_year, end_year, start_mmdd, end_mmdd):
    all_results = {}
    for code in codes:
        sql = f"SELECT trade_date, close FROM daily_hfq_data WHERE code = '{code}' AND YEAR(trade_date) BETWEEN {start_year} AND {end_year} ORDER BY trade_date ASC"
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)
        if df.empty: continue
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df['mmdd'] = df['trade_date'].dt.strftime('%m-%d')

        matrix_list = []
        for y in range(start_year, end_year + 1):
            y_df = df[df['trade_date'].dt.year == y].copy()
            mask = (y_df['mmdd'] >= start_mmdd) & (y_df['mmdd'] <= end_mmdd)
            y_seg = y_df[mask]
            if y_seg.empty: continue
            y_norm = (y_seg.set_index('mmdd')['close'] / y_seg['close'].iloc[0]) * 100
            matrix_list.append(y_norm.rename(y))

        if matrix_list:
            all_results[code] = pd.concat(matrix_list, axis=1).sort_index().ffill().bfill().mean(axis=1)
    return pd.DataFrame(all_results)


# --- 2. 颜色方案定义 ---
# 为不同品种分配主色调 (Dark 为上涨色, Light 为下跌色)
COLOR_PALETTE = {
    0: {'up': '#1f77b4', 'down': '#aec7e8'},  # 蓝色
    1: {'up': '#ff7f0e', 'down': '#ffbb78'},  # 橙色
    2: {'up': '#2ca02c', 'down': '#98df8a'},  # 绿色
    3: {'up': '#d62728', 'down': '#ff9896'},  # 红色
    4: {'up': '#9467bd', 'down': '#c5b0d5'},  # 紫色
}

app = dash.Dash(__name__)


# --- 3. 布局与分段绘图 ---
def create_layout(codes, start_yr, end_yr):
    traces = []
    for idx, code in enumerate(avg_matrix.columns):
        series = avg_matrix[code]
        dates = avg_matrix.index
        # 获取该品种对应的颜色组
        colors = COLOR_PALETTE.get(idx % len(COLOR_PALETTE))

        for i in range(1, len(series)):
            is_up = series.iloc[i] >= series.iloc[i - 1]
            current_color = colors['up'] if is_up else colors['down']

            traces.append(go.Scatter(
                x=dates[i - 1:i + 1], y=series.iloc[i - 1:i + 1],
                mode='lines',
                line=dict(color=current_color, width=3.5 if is_up else 2),
                legendgroup=code,
                name=code,
                showlegend=True if i == 1 else False,
                hoverinfo='none'
            ))

    return html.Div(style={'display': 'flex', 'height': '98vh', 'backgroundColor': '#fcfcfc'}, children=[
        # 左侧固定面板
        html.Div(id='side-panel', style={
            'width': '320px', 'padding': '20px', 'backgroundColor': '#ffffff',
            'borderRight': '1px solid #eee', 'boxShadow': '2px 0 10px rgba(0,0,0,0.05)'
        }, children=[
            html.H3("📊 多品种对比仪表盘", style={'fontSize': '20px', 'marginBottom': '5px'}),
            html.P(f"{start_yr}-{end_yr} 历史平均走势", style={'color': '#888', 'fontSize': '12px'}),
            html.Hr(style={'margin': '20px 0', 'opacity': '0.3'}),
            html.Div(id='hover-content')
        ]),
        # 右侧图表
        html.Div(style={'flex': '1', 'padding': '20px'}, children=[
            dcc.Graph(
                id='main-graph',
                figure={
                    'data': traces,
                    'layout': go.Layout(
                        xaxis={'type': 'category', 'nticks': 15, 'showspikes': True, 'spikemode': 'across'},
                        yaxis={'title': '平均归一化净值 (起点=100)', 'gridcolor': '#f0f0f0'},
                        hovermode='x',
                        template='plotly_white',
                        legend={'orientation': 'h', 'y': 1.05},
                        margin={'t': 30, 'l': 50, 'r': 30, 'b': 50}
                    )
                },
                style={'height': '100%'}
            )
        ])
    ])


# --- 4. 交互回调 ---
@app.callback(Output('hover-content', 'children'), Input('main-graph', 'hoverData'))
def update_hover(hoverData):
    if not hoverData: return "鼠标悬浮查看数据..."
    mmdd = hoverData['points'][0]['x']

    # 提取数据并按表现排序
    data_at_date = avg_matrix.loc[mmdd].sort_values(ascending=False)

    rows = [html.Div(f"📅 {mmdd}", style={'fontSize': '22px', 'fontWeight': 'bold', 'marginBottom': '15px'})]
    for i, (code, val) in enumerate(data_at_date.items()):
        ret = (val / 100) - 1
        # 获取当前 code 对应的 UI 颜色
        orig_idx = list(avg_matrix.columns).index(code)
        ui_color = COLOR_PALETTE[orig_idx % len(COLOR_PALETTE)]['up']

        rows.append(html.Div(style={'marginBottom': '12px', 'padding': '10px', 'borderRadius': '5px',
                                    'borderLeft': f'5px solid {ui_color}', 'backgroundColor': '#f9f9f9'}, children=[
            html.Div(code, style={'fontWeight': 'bold', 'fontSize': '14px'}),
            html.Div([
                html.Span(f"净值: {val:.2f}", style={'marginRight': '10px'}),
                html.Span(f"{ret:+.2%}", style={'color': '#e74c3c' if ret > 0 else '#27ae60', 'fontWeight': 'bold'})
            ])
        ]))
    return html.Div(rows)


# --- 5. Main ---
if __name__ == '__main__':
    CODES = ['004898.OF','009803.OF','000001.ZS', '000852.ZS']
    START_YR, END_YR = 2023, 2025
    S_MD, E_MD = '01-01', '12-31'

    avg_matrix = get_multi_seasonal_data(engine, CODES, START_YR, END_YR, S_MD, E_MD)

    if not avg_matrix.empty:
        app.layout = create_layout(CODES, START_YR, END_YR)
        app.run(debug=True, port=8050)