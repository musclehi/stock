import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import dash
from dash import dcc, html, Input, Output
import plotly.graph_objects as go
import platform

from common import constants

# 1. 配置数据库连接 (请替换为你的实际账号密码)
engine = create_engine(constants.dbStr)


# --- 1. 数据获取函数 (保持逻辑独立) ---
def get_seasonal_data(engine, code, start_year, end_year, start_mmdd, end_mmdd):
    sql = f"""
    SELECT trade_date, close FROM daily_hfq_data 
    WHERE code = '{code}' AND YEAR(trade_date) BETWEEN {start_year} AND {end_year}
    ORDER BY trade_date ASC
    """
    with engine.connect() as conn:
        all_df = pd.read_sql(text(sql), conn)

    if all_df.empty: return None, None

    all_df['trade_date'] = pd.to_datetime(all_df['trade_date'])
    all_df['mmdd'] = all_df['trade_date'].dt.strftime('%m-%d')

    matrix_list = []
    for y in range(start_year, end_year + 1):
        y_df = all_df[all_df['trade_date'].dt.year == y].copy()
        mask = (y_df['mmdd'] >= start_mmdd) & (y_df['mmdd'] <= end_mmdd)
        y_segment = y_df[mask]
        if y_segment.empty: continue
        matrix_list.append(y_segment.set_index('mmdd')['close'].rename(f"{y}年"))

    df_price = pd.concat(matrix_list, axis=1).sort_index().ffill().bfill()
    df_price['平均中枢'] = df_price.mean(axis=1)
    df_return = df_price.div(df_price.iloc[0]) - 1
    return df_price, df_return


# --- 2. 创建 App 实例 (全局变量用于 Callback) ---
app = dash.Dash(__name__)

# 定义全局变量，由 main 初始化
price_mtx = None
return_mtx = None
CURRENT_CODE = ""


# --- 3. 布局函数 (动态生成) ---
def create_layout(code, start_yr, end_yr):
    # 1. 准备基础曲线 (各年份细线)
    traces = [
        go.Scatter(
            x=price_mtx.index, y=price_mtx[col],
            name=col, mode='lines',
            line={'width': 1.2},
            opacity=0.25,
            hoverinfo='none'
        ) for col in price_mtx.columns if col != '平均中枢'
    ]

    # 2. 核心修改：分段绘制“平均中枢”以实现红绿变色
    avg_series = price_mtx['平均中枢']
    avg_index = price_mtx.index

    for i in range(1, len(avg_series)):
        # 判断涨跌方向
        is_up = avg_series.iloc[i] >= avg_series.iloc[i - 1]
        line_color = '#e74c3c' if is_up else '#27ae60'  # 红涨绿跌

        traces.append(go.Scatter(
            x=avg_index[i - 1:i + 1],  # 取当前点和前一个点构成线段
            y=avg_series.iloc[i - 1:i + 1],
            mode='lines',
            line=dict(color=line_color, width=1.5),
            legendgroup='平均中枢',  # 分组，点击其中一段即可控制全部
            showlegend=True if i == 1 else False,  # 只显示一个图例入口
            name='平均中枢 (趋势)',
            hoverinfo='none'
        ))
    return html.Div(style={'display': 'flex', 'height': '95vh', 'font-family': 'Heiti TC, Arial'}, children=[
        # 左侧固定面板 (代码保持不变)
        html.Div(id='info-panel', style={
            'width': '320px', 'backgroundColor': '#f8f9fa', 'padding': '20px',
            'borderRight': '1px solid #dee2e6', 'boxShadow': '2px 0 5px rgba(0,0,0,0.05)', 'overflowY': 'auto'
        }, children=[
            html.H3(f"📊 {code}", style={'margin-top': '0', 'color': '#333'}),
            html.P(f"历史区间: {start_yr} - {end_yr}", style={'color': '#666'}),
            html.Hr(),
            html.Div(id='hover-data-content', style={'lineHeight': '1.6'}, children="请将鼠标移动至右侧曲线...")
        ]),

        # 右侧图表区
        html.Div(style={'flex': '1', 'padding': '10px'}, children=[
            dcc.Graph(
                id='seasonal-graph',
                figure={
                    'data': traces,  # 使用我们组装好的 traces
                    'layout': go.Layout(
                        title=f'{code} 季节性走势看板 (红绿趋势线)',
                        xaxis={'type': 'category', 'nticks': 20, 'showspikes': True, 'spikemode': 'across',
                               'spikedash': 'dot'},
                        yaxis={'title': '后复权收盘价', 'tickformat': '.2f'},
                        hovermode='x',
                        margin={'t': 60, 'b': 50, 'l': 50, 'r': 50},
                        template='plotly_white'
                    )
                },
                style={'height': '100%'},
                config={'displayModeBar': False},
                clear_on_unhover=True
            )
        ])
    ])

# --- 4. 回调交互 ---
@app.callback(
    Output('hover-data-content', 'children'),
    Input('seasonal-graph', 'hoverData')
)
def update_panel(hoverData):
    if hoverData is None or price_mtx is None:
        return html.Div([
            html.P("💡 操作提示:", style={'fontWeight': 'bold'}),
            html.Ul([
                html.Li("移动鼠标：查看每日历史详情"),
                html.Li("点击图例：隐藏/显示特定年份"),
                html.Li("双击图例：孤立查看某一年")
            ], style={'fontSize': '13px', 'color': '#666'})
        ])

    target_mmdd = hoverData['points'][0]['x']

    # 顶部日期和均值
    header = [
        html.Div(style={'backgroundColor': '#333', 'color': 'white', 'padding': '10px', 'borderRadius': '5px',
                        'marginBottom': '15px'}, children=[
            html.Div(f"📅 日期: {target_mmdd}", style={'fontSize': '18px', 'fontWeight': 'bold'}),
            html.Div(f"平均中枢: {price_mtx.loc[target_mmdd, '平均中枢']:.2f}"),
            html.Div(f"平均涨跌: {return_mtx.loc[target_mmdd, '平均中枢']:+.2%}")
        ])
    ]

    # 年份明细
    year_cols = [c for c in price_mtx.columns if c != '平均中枢']
    details = []
    for yr in year_cols:
        p = price_mtx.loc[target_mmdd, yr]
        r = return_mtx.loc[target_mmdd, yr]
        color = '#e74c3c' if r > 0 else '#27ae60'  # 红涨绿跌

        details.append(html.Div(style={'padding': '5px 0', 'borderBottom': '1px solid #eee', 'display': 'flex',
                                       'justifyContent': 'space-between'}, children=[
            html.Span(f"{yr}", style={'fontWeight': 'bold'}),
            html.Span(f"{p:.2f}"),
            html.Span(f"{r:+.2%}", style={'color': color, 'fontWeight': 'bold'})
        ]))

    return html.Div(header + details)


# --- 单个股票涨跌曲线以及每年的曲线
if __name__ == '__main__':
    # ---------------------------------------------------------
    # 配置区：修改以下参数即可切换分析目标
    # ---------------------------------------------------------
    CODE = '004898.OF'  # 上证指数或其他代码
    START_YR, END_YR = 2021, 2025
    S_MD, E_MD = '01-01', '12-31'

    price_mtx, return_mtx = get_seasonal_data(engine, CODE, START_YR, END_YR, S_MD, E_MD)

    if price_mtx is not None:
        # 注入布局并启动
        app.layout = create_layout(CODE, START_YR, END_YR)
        # 使用最新的 .run 接口
        app.run(debug=True, port=8050)
    else:
        print("❌ 错误: 未能在指定区间内查找到数据，请检查代码或年份设置。")