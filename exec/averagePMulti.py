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


# --- 2. 增强型数据获取 ---
def get_data_v14(engine, codes, start_yr, end_yr, s_md, e_md):
    all_results = {}
    valid_codes = []

    for code in codes:
        # 为了计算第一天的收益，我们需要多取一点点数据（取到起始日期前的一条记录）
        sql = f"""
        SELECT trade_date, close FROM daily_hfq_data 
        WHERE code = '{code}' AND trade_date >= '{start_yr - 1}-12-01' AND trade_date <= '{end_yr}-12-31'
        ORDER BY trade_date ASC
        """
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)
        if df.empty: continue

        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df['mmdd'] = df['trade_date'].dt.strftime('%m-%d')

        yearly_matrices = []
        for y in range(start_yr, end_yr + 1):
            # 获取当年的数据片段
            y_mask = (df['trade_date'].dt.year == y) & (df['mmdd'] >= s_md) & (df['mmdd'] <= e_md)
            y_seg = df[y_mask].copy()
            if y_seg.empty: continue

            # 找到该片段第一个交易日之前的最后价格（基准价）
            first_date = y_seg['trade_date'].min()
            base_df = df[df['trade_date'] < first_date]
            if not base_df.empty:
                base_price = base_df['close'].iloc[-1]  # 取前一天的收盘价
            else:
                base_price = y_seg['close'].iloc[0]  # 若无前序数据，则以首日为基准

            # 计算包含首日变动的归一化序列
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
    # 解析参数：?codes=000001.ZS&start=2021&end=2026&s_md=01-01&e_md=12-31
    params = parse_qs(search.lstrip('?'))

    codes_str = params.get('codes', ['000001.ZS'])[0]
    codes = [c.strip() for c in codes_str.split(',')]

    start_yr = int(params.get('start', [2021])[0])
    end_yr = int(params.get('end', [2026])[0])
    s_md = params.get('s_md', ['01-01'])[0]
    e_md = params.get('e_md', ['12-31'])[0]

    # 建立数据库连接
    df_avg, actual_codes = get_data_v14(engine, codes, start_yr, end_yr, s_md, e_md)

    if df_avg.empty:
        return html.H2("⚠️ 未找到匹配数据，请检查 URL 参数", style={'textAlign': 'center', 'marginTop': '50px'})

    # 绘图 Trace：保持代码配色与涨跌逻辑
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

    return html.Div(style={'display': 'flex', 'height': '98vh', 'font-family': 'Heiti TC, Arial'}, children=[
        # 左侧固定顺序面板
        html.Div(
            style={'width': '350px', 'padding': '25px', 'backgroundColor': '#ffffff', 'borderRight': '1px solid #eee',
                   'overflowY': 'auto'}, children=[
                html.H3("🎯 季节性多窗口看板", style={'margin': '0'}),
                html.P(f"范围: {s_md} 至 {e_md}", style={'fontSize': '12px', 'color': '#666'}),
                html.Hr(style={'opacity': '0.2'}),
                # 存储数据矩阵
                dcc.Store(id='storage-v14', data={'df': df_avg.to_json(), 'codes': actual_codes}),
                html.Div(id='hover-content-v14')
            ]),
        # 右侧图表
        html.Div(style={'flex': '1', 'padding': '15px'}, children=[
            dcc.Graph(id='graph-v14', style={'height': '100%'}, figure={
                'data': traces,
                'layout': go.Layout(
                    title=f"多品种季节性平均走势 ({start_yr}-{end_yr})",
                    xaxis={'type': 'category', 'nticks': 15, 'showspikes': True, 'spikemode': 'across'},
                    yaxis={'title': '包含首日变动的归一化均值', 'gridcolor': '#f8f9fa'},
                    hovermode='x', template='plotly_white', legend={'orientation': 'h', 'y': 1.05}
                )
            })
        ])
    ])


# --- 4. 悬浮回调 (严格固定顺序) ---
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
        ret = (val / 100) - 1  # 相对于基准价的涨跌
        color = CODE_COLORS[i % len(CODE_COLORS)]['up']

        rows.append(html.Div(style={
            'marginBottom': '12px', 'padding': '12px', 'borderRadius': '8px',
            'borderLeft': f'6px solid {color}', 'backgroundColor': '#fcfcfc',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.04)'
        }, children=[
            html.Div(code, style={'fontWeight': 'bold', 'fontSize': '14px'}),
            html.Div(style={'display': 'flex', 'justifyContent': 'space-between'}, children=[
                html.Span(f"相对值: {val:.2f}", style={'color': '#636e72'}),
                html.B(f"{ret:+.2%}", style={'color': '#e17055' if ret > 0 else '#00b894'})
            ])
        ]))
    return html.Div(rows)


if __name__ == '__main__':
    # 运行后访问示例 URL:
    # http://127.0.0.1:8050/?codes=000001.ZS,399006.SZ&start=2021&end=2026&s_md=01-01&e_md=12-31
    app.run(debug=True, port=8050)