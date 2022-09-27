from io import StringIO
import logging
from datetime import datetime
import logging_json
import json

class Logger():

    def __init__(self, config, azb, execution_id: int, job: str, country: str, **kwargs) -> None:
        self.config = config
        self.azb = azb
        self.execution_id = execution_id
        self.job = job.lower()
        self.country = country.lower()
        self.log_definition = kwargs.get('log_definition')
        self.table = kwargs.get('table')
        self.source = kwargs.get('source')
        self.job_step = kwargs.get('job_step')
        self.execution_date = datetime.now()
    
    def get_key(self, key):
        return self.config.get(key)

    def get_detailed_logger(self):
        formatter = logging_json.JSONFormatter(fields={
            "level_name": "levelname",
            "log_created": "created",
            "execution_time": "asctime",
            "file_name":"filename",
            "line_with_message":"lineno",
            "msg": "message"
        })
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
    
        buffer = StringIO()     
        buffer_logger = logging.StreamHandler(buffer)
        buffer_logger.setLevel(logging.INFO)
        buffer_logger.setFormatter(formatter)
        
        logger.addHandler(buffer_logger)

        return buffer
    
    def get_overall_log(self):
        container = self.get_container()
        log_path = self.get_detail_log_path()

        overall_log = {
            "execution_id":self.execution_id,
            "country":self.country,
            "source":self.source,
            "job":self.job,
            "job_step":self.job_step,
            "job_status":"",
            "job_start_time":self.execution_date.strftime("%Y-%m-%d-%H:%M:%S"),
            "job_end_time":"",
            "writing_settings":"",
            "container":container,
            "log_path":log_path
        }

        if self.job_step == None:
            del overall_log['job_step']
        
        return overall_log

    def update_overall_logs(self, logs, **kwargs):
        job_end_time = kwargs.get('job_end_time').strftime("%Y-%m-%d-%H:%M:%S")
        job_status = kwargs.get('job_status')
        writing_settings = kwargs.get('writing_settings')
        if self.country == None:
            country = kwargs.get('country')
        else:
            country = self.country
        if self.source == None:
            source = kwargs.get('source')
        else:
            source = self.source

        overall_logs = logs.update(
                    country = country,
                    source = source,
                    job_end_time = job_end_time, 
                    job_status = job_status,
                    writing_settings = writing_settings
                    )
        
        return overall_logs
    
         
    def get_overall_log_path(self):
        overall_log_path = self.get_key('overallLogPath')

        if self.job == 'ref':
            file_name = f"{self.table}-{self.execution_date.strftime('%H_%M_%S')}.json"
            overall_log_path = f"{overall_log_path}/{self.job}/{self.country}/{self.table}/{self.execution_date.strftime('%Y-%m-%d')}/{file_name}"
        elif self.job == 'raw':
            file_name = f"{self.job_step}-{self.execution_date.strftime('%H_%M_%S')}.json"
            overall_log_path = f"{overall_log_path}/{self.job}/{self.country}/{self.job_step}/{self.execution_date.strftime('%Y-%m-%d')}/{file_name}"
        else:
            file_name = f"{self.execution_date.strftime('%H_%M_%S')}.json"
            overall_log_path =  f"{overall_log_path}/bad_configuration/{file_name}"
    
        return overall_log_path

    def get_detail_log_path(self):
        detail_log_path = self.get_key('detailLogPath')

        if self.job == 'ref':
            if self.table == None:
                table = 'custom'
            else:
                table = self.table
            file_name = f"{table}-{self.execution_date.strftime('%H_%M_%S')}.json"
            detailed_log_path = f"{detail_log_path}/{self.job}/{self.country}/{table}/{self.execution_date.strftime('%Y-%m-%d')}/{file_name}"
        elif self.job == 'raw':
            if self.job_step == None:
                job_step = 'custom'
            else:
                job_step = self.job_step
            file_name = f"{job_step}-{self.execution_date.strftime('%H_%M_%S')}.json"
            detailed_log_path = f"{detail_log_path}/{self.job}/{self.country}/{job_step}/{self.execution_date.strftime('%Y-%m-%d')}/{file_name}"
        else:
            file_name = f"{self.execution_date.strftime('%H_%M_%S')}.json"
            detailed_log_path =  f"{detail_log_path}/bad_configuration/{file_name}"

        return detailed_log_path

    def get_container(self):
        return self.get_key('containerLog')

    def write_logger(self, **kwargs):
        detailed_log = kwargs.get('detailed_log')
        overall_log = kwargs.get('overall_log')
        container = self.get_container()
        detailed_log_path = self.get_detail_log_path()
        overall_log_path = self.get_overall_log_path()
        
        if detailed_log:
            self.azb.uploadFile(container, detailed_log_path, detailed_log.getvalue())
        if overall_log:
            self.azb.uploadFile(container, overall_log_path, json.dumps(overall_log))