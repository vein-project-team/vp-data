from analysis.DailyReportGener import daily_report_gener
import json


def get_daily_report(date='latest'):
  up_down_rank = daily_report_gener.gen_up_down_rank(date)
  up_down_aggregation = daily_report_gener.gen_up_down_aggregation(date)
  up_limits_rank = daily_report_gener.gen_up_limits(date)
  down_limits_rank = daily_report_gener.gen_down_limits(date)

  result_json = f'''{{
    "up_down_rank": {up_down_rank.to_json(orient="records")},
    "up_down_aggregation": {up_down_aggregation.to_json(orient="records")},
    "up_limits_rank": {up_limits_rank.to_json(orient="records")},
    "down_limits_rank": {down_limits_rank.to_json(orient="records")}
  }}'''

  result_json = json.loads(result_json)
  return result_json

def get_up_down_rank(date='latest'):
  up_down_rank = daily_report_gener.gen_up_down_rank(date)
  result_json = up_down_rank.to_json(orient="records")
  result_json = json.loads(result_json)
  return result_json

def get_up_down_aggregation(date='latest'):
  up_down_aggregation = daily_report_gener.gen_up_down_aggregation(date)
  result_json = up_down_aggregation.to_json(orient="records")
  result_json = json.loads(result_json)
  return result_json