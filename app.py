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

@app.get("/api/daily-report/{date}")
def get_daily_report(date: str):
  up_down_rank = daily_report_gener.gen_up_down_rank(date)
  up_down_aggregation = daily_report_gener.gen_up_down_aggregation(date)
  up_limits_rank = daily_report_gener.gen_up_limits_rank(date)
  down_limits_rank = daily_report_gener.gen_down_limits_rank(date)

  result_json = f'''{{
    "up_down_rank": {up_down_rank.to_json(orient="records")},
    "up_down_aggregation": {up_down_aggregation.to_json(orient="records")},
    "up_limits_rank": {up_limits_rank.to_json(orient="records")},
    "down_limits_rank": {down_limits_rank.to_json(orient="records")}
  }}'''

  result_json = json.loads(result_json)
  return result_json

