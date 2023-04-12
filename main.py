import os
import uuid

from fastapi import FastAPI, BackgroundTasks

from processes import create_csv

app = FastAPI()


@app.get('/trigger_report')
async def trigger_report(background_tasks: BackgroundTasks):
    report_id = uuid.uuid4()

    background_tasks.add_task(create_csv, report_id)
    return {'report_id': report_id, 'status': 200, "message": "Report computation started in the background."}


@app.get('/get_report/{report_id}')
async def get_report(report_id: str):
    report_path = f'report_{report_id}.csv'
    if not os.path.exists(report_path):
        return {'status': 'Running'}
    return {'report_file': report_path, 'status': 200, 'message': 'The CSV file should be in your current folder'}
