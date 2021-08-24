from typing import Dict, Tuple, Union, List
import json
import time

import requests
from requests.sessions import Session

try:
    from . import exceptions
except ImportError:
    import exceptions

ESS_PARAM_NULL = "#NULL"


class SchedulerJob:
    def __init__(self, package: str, definition: str) -> None:
        self.package = package
        self.definition = definition
        self.parameters: List[str] = []

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}("{self.package}", "{self.definition}")'

    def __str__(self) -> str:
        return f"{self.__class__.__name__} (Package: {self.package}, Definition: {self.definition}, Parameters: {self.parameters})"

    @property
    def ess_parameter(self) -> str:
        if len(self.parameters) == 0:
            return ESS_PARAM_NULL
        return ",".join(self.parameters)

    @property
    def payload(self) -> Dict[str, Union[str, None]]:
        return {
            "OperationName": "submitESSJobRequest",
            "JobPackageName": self.package,
            "JobDefName": self.definition,
            "ESSParameters": self.ess_parameter,
            "ReqstId": None,
        }

    def add_parameter(self, parameter: Union[str, None]):
        if parameter is None:
            parameter = ESS_PARAM_NULL
        self.parameters.append(str(parameter))


class EnterpriseScheduler:
    def __init__(self, url: str, username: str, password: str) -> None:
        self.url = url
        self.username = username
        self.password = password

        self.verbose: bool = False
        self.max_poll: int = 500
        self.poll_interval: int = 10  # Sec

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}("{self.url}", "{self.username}", "{self.password}")'

    def __str__(self) -> str:
        return f"{self.__class__.__name__} (Url: {self.url}, Username: {self.username}, Password: ****)"

    @property
    def progress_status(self) -> List[str]:
        return ["WAIT", "BLOCKED", "RUNNING", "PAUSED", "COMPLETED", "READY"]

    @property
    def erp_integration(self) -> str:
        return f"{self.url}/fscmRestApi/resources/11.13.18.05/erpintegrations"

    @property
    def request(self) -> Session:
        cloud_request = requests.Session()
        cloud_request.headers.update(
            {
                "Authorization": f"Basic {self.username}:{self.password}",
                "content-type": "application/json",
            }
        )
        return cloud_request

    def get_job_monitor_url(self, request_id: str) -> str:
        return f"{self.erp_integration}?finder=ESSJobStatusRF;requestId={request_id}&onlyData=True&fields=RequestStatus"

    def run(self, job: SchedulerJob) -> Tuple[str, str]:
        request_id = self.submit(job)
        job_status = self.monitor(request_id)
        return (request_id, job_status)

    def submit(self, job: SchedulerJob) -> str:
        ess_response = self.request.post(
            self.erp_integration, data=json.dumps(job.payload)
        )
        ess_response.raise_for_status()
        request_id = ess_response.json()["ReqstId"]
        return request_id

    def monitor(self, request_id: str) -> str:
        ess_monitor_url = self.get_job_monitor_url(request_id)
        request_status = None
        progress_status = ["WAIT", "BLOCKED", "RUNNING", "PAUSED", "COMPLETED", "READY"]

        for _ in range(self.max_poll):
            time.sleep(self.poll_interval)
            monitor_response = self.request.get(ess_monitor_url)
            monitor_response.raise_for_status()
            request_status = monitor_response.json()["items"][0]["RequestStatus"]
            if request_status.upper() not in progress_status:
                break
        else:
            raise exceptions.LongRunningJobError(request_id)

        if request_status.startswith("ERROR"):
            raise exceptions.ScheduledJobError(request_id, request_status)

        return request_status


if __name__ == "__main__":
    ess_job = SchedulerJob("pack", "defn")
    ess_job.add_parameter("1")
    ess_job.add_parameter(None)
    ess_job.add_parameter(1)
    print(ess_job.payload)
    print(ess_job)

    sch = EnterpriseScheduler("https//url", "username", "pswd")
    print(ess_job.__repr__())
    print(sch.__repr__())

    # raise exceptions.ScheduledJobError("123", "ER")
