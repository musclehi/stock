import re
import os
import webbrowser


def generate_exact_colored_html(input_text):
    # 1. 数据解析 (增强型)
    lines = [line.strip() for line in input_text.strip().split('\n') if line.strip()]
    rows_html = ""

    # 日期转百分比 (基于 365 天)
    def to_pct(mmdd):
        days = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        m, d = int(mmdd[:2]), int(mmdd[2:])
        # 容错处理：确保月份在 1-12 之间
        m = max(1, min(12, m))
        return ((sum(days[:m]) + d) / 365) * 100

    for line in lines:
        # --- 核心修复逻辑 ---
        # A. 优先从行末提取十六进制颜色 (支持 3, 6, 8 位 HEX)
        color_match = re.search(r'#(?:[0-9a-fA-F]{3,8})\b$', line)
        if color_match:
            row_color = color_match.group(0)
            # 从原始行中移除颜色部分，防止干扰日期解析
            line_without_color = line[:color_match.start()].strip()
        else:
            row_color = "#007AFF"  # 默认 Apple 蓝
            line_without_color = line

        # B. 解析 Code (开头数字)
        code_match = re.match(r'^(\d+)', line_without_color)
        code = code_match.group(1) if code_match else "Unknown"

        # C. 解析剩余字符串中的所有日期区间 (0101-0808)
        periods = re.findall(r'(\d{4})-(\d{4})', line_without_color)

        # --- 构造 HTML ---
        bars = ""
        for start, end in periods:
            left = to_pct(start)
            width = max(to_pct(end) - left, 1.2)  # 最小宽度
            # 注入精准捕获的自定义颜色
            bars += f'<div class="bar" style="left:{left}%; width:{width}%; background:{row_color};" data-tip="{start} ~ {end}"></div>'

        rows_html += f'<div class="row"><div class="label">{code}</div><div class="track">{bars}</div></div>'

    # 2. HTML 模版 (保持 SF Mono 风格)
    full_html = f"""
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: -apple-system, system-ui, sans-serif; background: #f6f8fa; padding: 20px; }}
            .card {{ background: white; padding: 30px; border-radius: 16px; box-shadow: 0 8px 30px rgba(0,0,0,0.06); max-width: 1100px; margin: auto; }}
            .axis {{ display: flex; justify-content: space-between; margin-left: 120px; color: #8e8e93; font-size: 12px; font-weight: 500; border-bottom: 1px solid #e1e4e8; padding-bottom: 8px; }}
            .row {{ display: flex; align-items: center; height: 50px; border-bottom: 1px solid #f6f8fa; }}
            .label {{ width: 120px; font-weight: 600; color: #1f2328; font-family: monospace; font-size: 14px; }}
            .track {{ position: relative; flex-grow: 1; height: 100%; display: flex; align-items: center; }}
            .bar {{ position: absolute; height: 26px; border-radius: 13px; opacity: 0.88; transition: 0.2s cubic-bezier(0.33, 1, 0.68, 1); cursor: pointer; }}
            .bar:hover {{ opacity: 1; transform: scaleY(1.12); box-shadow: 0 2px 8px rgba(0,0,0,0.1); filter: brightness(1.05); }}
            /* 悬浮气泡 */
            .bar::after {{ 
                content: attr(data-tip); position: absolute; bottom: 38px; left: 50%; transform: translateX(-50%);
                background: rgba(0,0,0,0.85); color: #fff; padding: 5px 10px; border-radius: 6px; font-size: 11px;
                white-space: nowrap; visibility: hidden; opacity: 0; pointer-events: none; transition: 0.15s;
            }}
            .bar:hover::after {{ visibility: visible; opacity: 1; }}
        </style>
    </head>
    <body>
        <div class="card">
            <h3 style="margin-top:0; color:#1f2328;">2026 财务标的持仓/观测对比 (精准颜色版)</h3>
            <div class="axis">
                <span>1月</span><span>2月</span><span>3月</span><span>4月</span><span>5月</span><span>6月</span>
                <span>7月</span><span>8月</span><span>9月</span><span>10月</span><span>11月</span><span>12月</span>
            </div>
            {rows_html}
        </div>
    </body>
    </html>
    """
    return full_html

if __name__ == '__main__':

    # 请确保日期区间后有逗号或空格分隔，行末是 #HEX
    # --- 多颜色输入测试 ---
    test_input = """
        004898 0101-0808, 1116-1231 #34C759
        007172 0311-0808, 1204-1231 #34C759
        009803 0317-0807, 1008-1105, 1114-1231 #34C759
        018846 0102-0808, 1009-1231 #34C759
        000001 0205-0221, 0426-0520, 0524-0610, 0622-0701, 0921-1008, 1031-1107, 1122-1213, 1220-1230 #FF3B30
        000300 0111-0210, 0921-1008, 1031-1107, 1128-1210, 1220-1228 #FF3B30
        000905 0205-0221, 0425-0520, 0524-0618, 0622-0830, 0904-0912, 0921-1008, 1017-1113, 1126-1212, 1223-1230 #FF3B30
    """

    # 执行生成
    html_content = generate_exact_colored_html(test_input)
    file_name = "result.html"
    # 2. 保存文件 (使用绝对路径更稳健)
    file_path = os.path.abspath(file_name)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    # 3. 核心：自动打开
    # 'new=2' 表示如果可能的话在浏览器新标签页打开
    webbrowser.open(f"file://{file_path}", new=2)

    print(f"✅ 页面已生成并尝试自动打开: {file_path}")