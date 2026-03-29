import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import dash
from dash import dcc, html, Input, Output, State
import plotly.graph_objects as go
from urllib.parse import parse_qs

from common import constants

# 1. 配置数据库连接 (请替换为你的实际账号密码)
engine = create_engine(constants.dbStr)

# --- 1. 高对比度配色方案 ---
CODE_COLORS = [
    {'up': '#0984e3', 'down': '#fdcb6e'},  # 蓝/黄
    {'up': '#27ae60', 'down': '#e056fd'},  # 绿/紫
    {'up': '#e67e22', 'down': '#34495e'},  # 橙/灰
    {'up': '#6c5ce7', 'down': '#badc58'},  # 紫/嫩绿
    {'up': '#d63031', 'down': '#7ed6df'},  # 红/青
]


# --- 2. 增强型数据获取 (已支持离散年份列表) ---
def get_data_v14(engine, codes, year_list, s_md, e_md):
    all_results = {}
    valid_codes = []

    # 获取年份边界以优化 SQL 查询范围
    min_yr = min(year_list)
    max_yr = max(year_list)

    for code in codes:
        # 为了计算每一年初的收益，我们需要取到最小年份前一年的12月数据作为基准
        sql = f"""
        SELECT trade_date, close FROM daily_hfq_data 
        WHERE code = '{code}' AND trade_date >= '{min_yr - 1}-12-01' AND trade_date <= '{max_yr}-12-31'
        ORDER BY trade_date ASC
        """
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)
        if df.empty: continue

        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df['mmdd'] = df['trade_date'].dt.strftime('%m-%d')

        yearly_matrices = []
        # --- 核心修改：只循环指定的离散年份 ---
        for y in year_list:
            y_mask = (df['trade_date'].dt.year == y) & (df['mmdd'] >= s_md) & (df['mmdd'] <= e_md)
            y_seg = df[y_mask].copy()
            if y_seg.empty: continue

            # 找到该年份片段首个交易日之前的最后价格（基准价）
            first_date = y_seg['trade_date'].min()
            base_df = df[df['trade_date'] < first_date]

            if not base_df.empty:
                base_price = base_df['close'].iloc[-1]
            else:
                base_price = y_seg['close'].iloc[0]

            # 计算归一化序列
            y_norm = (y_seg.set_index('mmdd')['close'] / base_price) * 100
            yearly_matrices.append(y_norm.rename(y))

        if yearly_matrices:
            # 合并多年数据并求均值
            combined = pd.concat(yearly_matrices, axis=1).sort_index().ffill().bfill()
            all_results[code] = combined.mean(axis=1)
            valid_codes.append(code)

    return pd.DataFrame(all_results), valid_codes


# --- 3. Dash 实例与路由 ---
app = dash.Dash(__name__, suppress_callback_exceptions=True)

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content-v14')
])


@app.callback(Output('page-content-v14', 'children'), [Input('url', 'search')])
def display_page(search):
    # 解析参数示例：?codes=004898.OF&years=2021,2023,2024&s_md=01-01&e_md=12-31
    params = parse_qs(search.lstrip('?'))

    codes_str = params.get('codes', ['000001.ZS'])[0]
    codes = [c.strip() for c in codes_str.split(',')]

    # --- 核心修改：从 years=2021,2023 获取年份列表 ---
    years_raw = params.get('years', ['2021,2023,2024'])[0]
    try:
        year_list = [int(y.strip()) for y in years_raw.split(',')]
    except ValueError:
        return html.H2("⚠️ 年份格式错误，请检查 URL (例如: years=2021,2024)", style={'textAlign': 'center'})

    s_md = params.get('s_md', ['01-01'])[0]
    e_md = params.get('e_md', ['12-31'])[0]

    # 获取计算结果
    df_avg, actual_codes = get_data_v14(engine, codes, year_list, s_md, e_md)

    if df_avg.empty:
        return html.H2("⚠️ 未找到匹配数据，请检查代码或年份", style={'textAlign': 'center', 'marginTop': '50px'})

    # 绘制折线
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

    years_label = ",".join(map(str, sorted(year_list)))

    return html.Div(style={'display': 'flex', 'height': '98vh', 'font-family': 'Heiti TC, Arial'}, children=[
        # 左侧固定顺序面板
        html.Div(
            style={'width': '350px', 'padding': '25px', 'backgroundColor': '#ffffff', 'borderRight': '1px solid #eee',
                   'overflowY': 'auto'}, children=[
                html.H3("🎯 季节性看板", style={'margin': '0'}),
                html.P(f"选定年份: {years_label}", style={'fontSize': '12px', 'color': '#666'}),
                html.P(f"区间: {s_md} 至 {e_md}", style={'fontSize': '12px', 'color': '#666'}),
                html.Hr(style={'opacity': '0.2'}),
                dcc.Store(id='storage-v14', data={'df': df_avg.to_json(), 'codes': actual_codes}),
                html.Div(id='hover-content-v14')
            ]),
        # 右侧图表
        html.Div(style={'flex': '1', 'padding': '15px'}, children=[
            dcc.Graph(id='graph-v14', style={'height': '100%'}, figure={
                'data': traces,
                'layout': go.Layout(
                    title=f"多品种季节性平均走势 ({years_label})",
                    xaxis={'type': 'category', 'nticks': 12, 'showspikes': True, 'spikemode': 'across'},
                    yaxis={'title': '归一化均值 (100基准)', 'gridcolor': '#f8f9fa'},
                    hovermode='x', template='plotly_white', legend={'orientation': 'h', 'y': 1.05}
                )
            })
        ])
    ])


# --- 4. 悬浮回调 ---
@app.callback(
    Output('hover-content-v14', 'children'),
    [Input('graph-v14', 'hoverData')],
    [State('storage-v14', 'data')]
)
def update_hover_v14(hoverData, stored):
    if not hoverData or not stored: return html.P("💡 提示：在图中移动鼠标查看每日收益...")

    mmdd = hoverData['points'][0]['x']
    df = pd.read_json(stored['df'])
    codes = stored['codes']

    rows = [html.Div(f"📅 {mmdd}",
                     style={'fontSize': '24px', 'fontWeight': 'bold', 'marginBottom': '20px', 'color': '#2d3436'})]

    for i, code in enumerate(codes):
        val = df.loc[mmdd, code]
        ret = (val / 100) - 1
        color = CODE_COLORS[i % len(CODE_COLORS)]['up']

        rows.append(html.Div(style={
            'marginBottom': '12px', 'padding': '12px', 'borderRadius': '8px',
            'borderLeft': f'6px solid {color}', 'backgroundColor': '#fcfcfc',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.04)'
        }, children=[
            html.Div(code, style={'fontWeight': 'bold', 'fontSize': '14px'}),
            html.Div(style={'display': 'flex', 'justifyContent': 'space-between'}, children=[
                html.Span(f"平均相对值: {val:.2f}"),
                html.B(f"{ret:+.2%}", style={'color': '#e17055' if ret > 0 else '#00b894'})
            ])
        ]))
    return html.Div(rows)


if __name__ == '__main__':
    # 示例访问 URL:
    # http://127.0.0.1:8050/?codes=004898.OF,007172.OF,009803.OF,018846.OF,000001.ZS,000300.ZS,000852.ZS&years=2023,2024,2025&s_md=01-01&e_md=12-31
    app.run(debug=True, port=8050)