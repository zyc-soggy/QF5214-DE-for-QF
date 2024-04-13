import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime


def fetch_comments(stock_code, start_page=1, end_page=1):
    all_comments = []  # 用于存储所有评论的信息

    for page_num in range(start_page, end_page + 1):
        url = f"http://guba.eastmoney.com/list,{stock_code},f_{page_num}.html"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            comments = soup.find_all('tr', class_='listitem')  # 获取所有评论项
            for comment in comments:
                # 提取所需字段
                read = comment.find('div', class_='read').text if comment.find('div', class_='read') else 'N/A'
                reply = comment.find('div', class_='reply').text if comment.find('div', class_='reply') else 'N/A'
                title_div = comment.find('div', class_='title')
                title = title_div.get_text(strip=True) if title_div else 'N/A'
                author = comment.find('div', class_='author').text if comment.find('div', class_='author') else 'N/A'
                update_time = comment.find('div', class_='update').text if comment.find('div',
                                                                                        class_='update') else 'N/A'

                # 收集数据
                all_comments.append({
                    "页码": page_num,
                    "阅读": read,
                    "回复": reply,
                    "标题": title,
                    "作者": author,
                    "更新时间": update_time
                })
        else:
            print(f"请求失败，状态码：{response.status_code}")

    # 将数据转换为DataFrame
    df = pd.DataFrame(all_comments)

    # 获取当前时间并格式化为字符串
    now = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 生成包含时间戳的文件名
    csv_file = f"comments_{stock_code}_p{start_page}_to_p{end_page}_{now}.csv"
    df.to_csv(csv_file, index=False, encoding='utf_8_sig')
    print(f"数据已保存到CSV文件：{csv_file}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'guba300750',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(hours=5),
)

run_this = PythonOperator(
    task_id='run_python_script',
    python_callable=fetch_comments,
    op_kwargs={'stock_code': '300750', 'start_page': 1, 'end_page': 2},  # 使用关键字参数传递
    dag=dag,
)