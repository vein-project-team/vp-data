# 按下面的规范书写一个分析程序
# 导入约束类，照抄
from analysis.ReportGener import ReportGener
# 导入其他依赖
from data_source import local_source
from data_source import date_getter

# 自定义类名，格式照抄
class TestReportGener(ReportGener):

  # 一定要声明一个文件夹名，文件夹将生成在 ~/reports/ 下
  folder_name = 'test'

  # 照抄
  def __init__(self) -> None:
      super().__init__()

  # 可以在这里写其他用得到的方法
  def xxx(self):
    pass

  # 生成分析结果的入口
  def gen(self):

    # 起一个文件名
    filename = '沪深主板股票'

    # 可选：在文件名中嵌入日期
    filename = f'{ date_getter.get_today() }-沪深主板股票'

    # 如果结果已存在，跳过分析过程
    # 直接从文件中取出数据并返回
    if data := self.fetch(filename) is not None:
      return data

    # 具体的分析逻辑
    data = local_source.get_stock_list(
      cols='''
      TS_CODE, NAME, CNSPELL, AREA, INDUSTRY
      ''',
      condition='''
      TS_CODE LIKE '60%' OR TS_CODE LIKE '00%' ORDER BY TS_CODE
      '''
    )

    # 分析结果写入硬盘，data 一定要是 DataFrame 类型， 后填文件名
    # 生成 ~/reports/沪深主板股票.csv
    self.store(data, filename)

    # 或者：覆盖存在的文件：
    # self.store(data, '沪深主板股票', override=True)


    # 也返回一下 data
    return data

