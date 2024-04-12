import jieba  # 导入jieba分词库
from PIL import Image  # 图片处理
from matplotlib import pyplot as plt  # 绘图数据可视化
from wordcloud import WordCloud  # 词云
import numpy as np  # 矩阵运算
import pandas as pd

# 准备停用词列表
with open('D:/pythonProject/5214_flask/stopwords.txt', 'r', encoding='utf-8') as f:
    stopwords = set([line.strip() for line in f.readlines()])  # 使用集合提高检索效率

# 准备词云所需要的文字
df1 = pd.read_csv("D:/pythonProject/5214_flask/雪球并表.csv", encoding='GBK')
df2 = pd.read_csv("D:/pythonProject/5214_flask/股吧并表.csv", encoding='GBK')
data1 = df1['Texts']
data2 = df2['Texts']
data = pd.concat([data1, data2])
print(data.size)
text = ''.join(data.astype(str))  # 确保文本是字符串，将所有文本拼接到一起

# 使用jieba进行分词，并过滤停用词
cut_text = jieba.cut(text)
words = [word for word in cut_text if word not in stopwords and len(word) > 1]  # 过滤停用词，且只保留长度大于1的词
string = ' '.join(words)
print(len(string))

# 准备遮罩图片
img = Image.open(r'.\static\assets\img\tree.jpg')  # 打开遮罩图片
img_array = np.array(img)  # 将图片转变为图片数组

# 创建词云对象
wc = WordCloud(
    background_color='white',  # 形成词云图片背景
    mask=img_array,  # 遮罩文件为数组
    font_path='C:/Windows/Fonts/msyhbd.ttc'  # 字体：微软雅黑
)

# 生成词云
wc.generate_from_text(string)  # 从文本中选择生成的词云对象

# 绘制图片
fig = plt.figure(1)  # 从第一个位置开始绘制
plt.imshow(wc)  # 按照词云wc的规则进行显示词云图片
plt.axis('off')  # 关闭坐标轴
# 输出词云图片到文件
plt.savefig(r'.\static\assets\img\wordcloud.jpg', dpi=800)  # dpi：分辨率
plt.show()  # 查看效果
