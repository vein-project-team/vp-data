import json
from analysis.DailyReportGener import daily_report_gener
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = [
  "http://localhost:3000",
  "http://localhost:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/stock-list/{date}")
def get_stock_list(date: str):
  stock_list = daily_report_gener.stock_list
  result_json = stock_list.to_json(orient="records")
  result_json = json.loads(result_json)
  return result_json

@app.get("/api/up-down-rank/{date}")
def get_up_down_rank(date: str):
  up_down_rank = daily_report_gener.gen_up_down_rank(date)
  result_json = up_down_rank.to_json(orient="records")
  result_json = json.loads(result_json)
  return result_json

@app.get("/api/up-down-aggregation/{date}")
def get_up_down_aggregation(date: str):
  up_down_aggregation = daily_report_gener.gen_up_down_aggregation(date)
  result_json = up_down_aggregation.to_json(orient="records")
  result_json = json.loads(result_json)
  return result_json

@app.get("/api/up-limits/{date}")
def get_up_limits(date: str):
  up_limits = daily_report_gener.gen_up_limits(date)
  result_json = up_limits.to_json(orient="records")
  result_json = json.loads(result_json)
  return result_json

@app.get("/api/down-limits/{date}")
def get_down_limits(date: str):
  down_limits = daily_report_gener.gen_down_limits(date)
  result_json = down_limits.to_json(orient="records")
  result_json = json.loads(result_json)
  return result_json