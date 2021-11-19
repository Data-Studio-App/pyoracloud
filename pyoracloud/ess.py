"""
Oracle Cloud Enterprise Schedule Service.
"""
from typing import Dict, Tuple, Union, List
import json
import time

try:
    from . import exceptions
    from . import env
except ImportError:
    import exceptions
    import env

ESS_PARAM_NULL = "#NULL"


class SchedulerJob:
    """
    Scheduler Job API
    """

    def __init__(self, package: str, definition: str) -> None:
        """
        Creates a new Scheduler Job.

        This class defines a individual Enterprise scheduler job.
        To run this job we will use run() in EnterpriseScheduler

        Args:
            package (str): The package name of the job.
            definition (str): The definition name of the job.

        Returns:
            None

        Example:
        >>> from pyoracloud import ess
        >>> my_package = "/oracle/apps/ess/financials/commonModules/shared/common/interfaceLoader/"
        >>> my_definition = "SyncBellNotifications"
        >>> job = ess.SchedulerJob(my_package, my_definition)
        """
        self.package = package
        self.definition = definition
        self.parameters: List[str] = []

    @property
    def ess_parameter(self) -> str:
        """
        Returns:
            str: The parameters as a string.
        """
        if len(self.parameters) == 0:
            return ESS_PARAM_NULL
        return ",".join(self.parameters)

    @property
    def payload(self) -> Dict[str, Union[str, None]]:
        """
        Returns:
            Dict[str, Union[str, None]]: The payload for the job.
        """
        return {
            "OperationName": "submitESSJobRequest",
            "JobPackageName": self.package,
            "JobDefName": self.definition,
            "ESSParameters": self.ess_parameter,
            "ReqstId": None,
        }

    def add_parameter(self, parameter: Union[str, None]):
        """
        Args:
            parameter (str): The parameter to add to the job.

        Example:
        --------
        >>> from pyoracloud import ess
        >>> ...
        >>> job = ess.SchedulerJob(my_package, my_definition)
        >>> job.add_parameter("PARAM1") # Add parameter 1
        >>> job.add_parameter(None)     # To add a #NULL as parameter 2
        >>> print(job.ess_parameter)
        PARAM1,#NULL
        """
        if parameter is None:
            parameter = ESS_PARAM_NULL
        self.parameters.append(str(parameter))


class EnterpriseScheduler:
    """
    Enterprise Scheduler API
    """

    def __init__(self, pod: env.Pod) -> None:
        """
        Creates a new Enterprise Scheduler.

        This class defines a Enterprise Scheduler, which can be used to submit
        a Job to the Oracle Cloud ESS.

        """
        self.pod = pod
        self.__run_request_id: str = None
        self.__run_status: str = None

    @property
    def run_status(self) -> str:
        return self.__run_status

    @property
    def run_request_id(self) -> str:
        return self.__run_request_id

    @property
    def progress_status(self) -> List[str]:
        return ["WAIT", "BLOCKED", "RUNNING", "PAUSED", "COMPLETED", "READY"]

    @property
    def erp_integration(self) -> str:
        uri: str = "fscmRestApi/resources/11.13.18.05/erpintegrations"
        return f"{self.pod.url}/{uri}"

    def get_job_monitor_url(self, request_id: str) -> str:
        """
        Args:
            request_id (str): The request id of the job.
        Returns:
            str: The URL to monitor the job.
        """
        url = self.erp_integration
        url = f"{url}?finder=ESSJobStatusRF"
        url = f"{url};requestId={request_id}"
        url = f"{url}&onlyData=True"
        url = f"{url}&fields=RequestStatus"
        return url

    def run(self, job: SchedulerJob) -> Tuple[str, str]:
        """
        Args:
            job (SchedulerJob): The job to run.
        Returns:
            Tuple[str, str]: The request id and status of the run.
        """
        self.__run_request_id = self.submit(job)
        self.__run_status = self.monitor(self.run_request_id)
        return self.__run_request_id, self.__run_status

    def submit(self, job: SchedulerJob) -> str:
        """
        Args:
            job (SchedulerJob): The job to submit.
        Returns:
            str: The request id of the job.
        """
        self.pod.display_message(f"Submitting {job}")
        self.pod.display_message(f"Url:  {self.erp_integration}")
        self.pod.display_message(f"Payload:  {json.dumps(job.payload)}")
        ess_response = self.pod.request.post(
            self.erp_integration, data=json.dumps(job.payload)
        )

        self.pod.display_message(f"Response: {ess_response.status_code}")
        ess_response.raise_for_status()

        request_id = ess_response.json()["ReqstId"]
        self.pod.display_message(f"Request Id : {request_id}")

        return request_id

    def monitor(self, request_id: str) -> str:
        """
        Args:
            request_id (str): The request id of the job.
        Returns:
            str: The status of the job.
        """
        ess_monitor_url = self.get_job_monitor_url(request_id)

        self.pod.display_message(f"Start monitoring request id {request_id}")
        self.pod.display_message(f"Url:  {ess_monitor_url}")
        self.pod.display_message(f"Max poll: {self.pod.max_poll}")
        self.pod.display_message(f"Poll interval: {self.pod.poll_interval} sec")

        request_status = None
        for _ in range(self.pod.max_poll):
            time.sleep(self.pod.poll_interval)
            monitor_response = self.pod.request.get(ess_monitor_url)
            monitor_response.raise_for_status()
            items = monitor_response.json()["items"]
            request_status = items[0]["RequestStatus"]
            self.pod.display_message(f"{monitor_response} {request_status}")

            if request_status.upper() not in self.progress_status:
                break
        else:
            raise exceptions.LongRunningJobError(request_id)

        self.raise_for_job_status(request_id, request_status)

        return request_status

    def raise_for_job_status(
        self, request_id: str = None, request_status: str = None
    ) -> None:
        """
        Args:
            request_id (str): The request id of the job.
            request_status (str): The status of the job.
        Returns:
            None: Raises an exception if the job ran with errors.
        """
        if request_status.startswith("ERROR"):
            raise exceptions.ScheduledJobError(request_id, request_status)
