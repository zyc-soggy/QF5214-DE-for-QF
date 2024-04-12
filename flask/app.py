import matplotlib
matplotlib.use('Agg')  # 使用非交互式后端
from flask import Flask, render_template, request
from openpyxl import load_workbook
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import threading


# 数据预处理
def process_data(source, time):
    # 直接读取CSV里的日期会出现浮点数，因此多定义一个函数来解决此问题
    def date(para):
        delta = pd.to_timedelta(para - 2, unit='D')
        time = pd.to_datetime('1900-01-01') + delta
        return time

    if source == "雪球":
        file_path = "D:/pythonProject/5214_flask/雪球并表.csv"
    elif source == "股吧":
        file_path = "D:/pythonProject/5214_flask/股吧并表.csv"
    else:
        raise ValueError("Invalid source. Please choose either '雪球' or '股吧'.")

    df = pd.read_csv(file_path, encoding="GBK")
    df['Update time'] = df['Update time'].apply(date)
    if time == "天":
        df['Update time'] = pd.to_datetime(df['Update time']).dt.date
    elif time == "十分钟":
        df['Update time'] = df['Update time'].dt.floor('10min')

    df_grouped = df.groupby(['Update time', 'Company', 'Stock Code']).agg({'情绪得分': 'mean'}).reset_index()
    df_comments_sum = df.groupby(['Update time', 'Company', 'Stock Code']).agg({'Comments': 'sum'}).reset_index()
    df_grouped = pd.merge(df_grouped, df_comments_sum, on=['Update time', 'Company', 'Stock Code'], how='left')
    df_grouped = df_grouped.rename(columns={'Stock Code': 'Code', 'Company': 'Stock', '情绪得分': 'Score', 'Comments': 'Number'})
    df_grouped['Rank'] = df_grouped.groupby('Update time')['Score'].rank(ascending=False)
    df_grouped = df_grouped.sort_values(by=['Update time', 'Rank'])
    # 创建链接字典
    link_dict = {
        '特斯拉': 'https://xueqiu.com/S/TSLA',
        '比亚迪': 'https://xueqiu.com/k?q=比亚迪',
        '宁德时代': 'https://xueqiu.com/k?q=宁德时代',
        '蔚来': 'https://xueqiu.com/S/NIO'
    }

    # 新增 Link 列
    df_grouped['Link'] = df_grouped['Stock'].map(link_dict)
    # 将 Rank 和 Number 列转换为字符串，并去掉小数点后的部分
    df_grouped['Rank'] = df_grouped['Rank'].astype(int).astype(str)
    df_grouped['Number'] = df_grouped['Number'].astype(int).astype(str)
    return df_grouped



app = Flask(__name__)


@app.route('/')
def index():
    return render_template("index.html")

@app.route('/index')
def home():
    return render_template("index.html")

@app.route('/stock', methods=['GET', 'POST'])
def stock():
    df_xueqiu = process_data("雪球", "天")
    df_guba = process_data("股吧","天")
    selected_source = "雪球"

    # Check if the request method is POST
    if request.method == 'POST':
        selected_source = request.form.get('selected_source')  # Get the selected data source
        if selected_source == '雪球':
            df_grouped = df_xueqiu.copy()  # Use data from 雪球
        elif selected_source == '股吧':
            df_grouped = df_guba.copy()  # Use data from 股吧
        else:
            # If no source is selected, use data from 雪球 by default
            df_grouped = df_xueqiu.copy()
    else:
        # If the request method is GET, default to 雪球 data
        df_grouped = df_xueqiu.copy()

    # Convert 'Update time' column to date
    # df_grouped['Update time'] = pd.to_datetime(df_grouped['Update time']).dt.date

    selected_date = request.form.get('selected_date', str(datetime.now().date()))

    # Filter data based on selected date
    if selected_date:
        df_filtered = df_grouped[df_grouped['Update time'] == pd.to_datetime(selected_date).date()]
        datalist = df_filtered.to_dict('records')
    else:
        datalist = df_grouped.to_dict('records')

    return render_template("stock.html", stocks=datalist, selected_date=selected_date, selected_source=selected_source)

@app.route('/score', methods=['GET', 'POST'])
def score():
    df_xueqiu = process_data("雪球", "十分钟")
    df_guba = process_data("股吧", "十分钟")

    selected_company = "特斯拉"  # 默认值
    selected_source = "雪球"
    start_time = "2024-04-02"
    end_time = "2024-04-09"

    if request.method == 'POST':
        selected_source = request.form.get('selected_source')  # Get the selected data source
        if selected_source == '雪球':
            df_grouped = df_xueqiu.copy()  # Use data from 雪球
        elif selected_source == '股吧':
            df_grouped = df_guba.copy()  # Use data from 股吧
        else:
            # If no source is selected, use data from 雪球 by default
            df_grouped = df_xueqiu.copy()

        start_time = request.form.get('start_time')
        end_time = request.form.get('end_time')

        start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M")
        end_time = datetime.strptime(end_time, "%Y-%m-%dT%H:%M")

        df_grouped = df_grouped[(df_grouped['Update time'] >= start_time) & (df_grouped['Update time'] <= end_time)]

        # 添加对 selected_company 的定义
        selected_company = request.form.get('selected_company', None)

    else:
        df_grouped = df_xueqiu.copy()

    # 进一步处理df_grouped，根据选择的时间段和公司进行筛选
    if selected_company:
        # 根据选择的公司进行筛选
        # 例如：selected_company = "宁德时代"
        df_grouped = df_grouped[df_grouped['Stock'] == selected_company]

    # 使用 df_grouped['Score'] 数据绘制折线图
    plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']
    plt.plot(df_grouped['Update time'], df_grouped['Score'])
    plt.xlabel('Update time')
    plt.ylabel('Score')
    plt.title(f"{start_time} 至 {end_time} {selected_company} 情绪得分，来源: {selected_source}")
    plt.xticks(rotation=45)  # 旋转 x 轴标签，以避免重叠
    plt.tight_layout()  # 调整布局，以确保标签不重叠

    # 保存折线图为图像文件
    plot_file = 'static/plot.png'  # 图像文件路径，保存在 static 目录下
    plt.savefig(plot_file)
    plt.close()  # 关闭绘图，释放资源

    # 在 HTML 模板中引用生成的图像文件
    plot_url = plot_file  # 图像文件 URL，用于在 HTML 页面中引用
    dates = df_grouped['Update time']
    scores = df_grouped['Score']
    return render_template("score.html", plot_url=plot_url, dates=dates, scores=scores)


@app.route('/word')
def word():
    return render_template("word.html")

@app.route('/team')
def team():
    return render_template("team.html")

if __name__ == '__main__':
    app.run(debug=True)

