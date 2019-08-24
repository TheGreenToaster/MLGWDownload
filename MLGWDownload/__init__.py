from datetime import datetime, timedelta
import logging
import os

import azure.functions as func

from .download import downloadMeterData

def main(mytimer: func.TimerRequest) -> None:

    now = datetime.now() 
    date = now.today() - timedelta(days=1)

    downloadMeterData(os.environ['MLGW_USERNAME'],os.environ['MLGW_PASSWORD'],date.strftime("%m/%d/%Y"),os.environ['STORAGE_CONNECTIONSTRING'],os.environ['STORAGE_CONTAINER'])
