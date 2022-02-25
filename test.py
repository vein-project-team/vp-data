# 导入你要运行的分析程序
from analysis.TestReportGener import TestReportGener

if __name__ == '__main__':

    # 像这样运行，第一次会运行分析并写入硬盘
    data = TestReportGener().gen()
    # print(data)

    # 第二次则不会分析，直接从硬盘读取
    data_read_from_disk = TestReportGener().gen()
    # print(data_read_from_disk)