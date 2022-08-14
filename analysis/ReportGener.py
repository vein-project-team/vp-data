from abc import abstractmethod, ABC

import pandas
from utils import log
from pandas import DataFrame
from pathlib import Path

class ReportGener(ABC):

  folder_name = 'abstract'
  folder = './reports/abstract/'

  def __init__(self) -> None:
    
    self.folder = f'./reports/{self.folder_name}/'
    self.folder = Path(self.folder)

    if not (base := Path('./reports/')).exists():
      base.mkdir()

    if not self.folder.exists():
      self.folder.mkdir()


  def store(self, data: DataFrame, filename: str, override=False):

    store_path = self.folder / f'{filename}.csv'

    if store_path.is_file():

      if override:
        store_path.unlink()

      else: 
        log(f'文件 { store_path.as_posix() } 已存在，程序将不会覆盖它。')
        return

    data.to_csv(store_path, encoding='utf-8-sig', index=False)
  

  def fetch(self, filename: str) -> DataFrame:

    store_path = self.folder / f'{filename}.csv'

    if store_path.is_file():
      data = pandas.read_csv(store_path)
      return data

    else:
      log(f'文件 { store_path.as_posix() } 不存在')
      return
