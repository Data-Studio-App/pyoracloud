class LongRunningJobError(Exception):
    """Job is running longer than expected"""

    def __init__(self, rqst_id: str, msg: str = None) -> None:
        self.request_id = rqst_id
        self.message = msg

        if self.message is None:
            self.message = self.__doc__

        super().__init__(self.message)

    def __str__(self):
        return f"Request Id: {self.request_id}, Message: {self.message}"


class ScheduledJobError(Exception):
    """Job ran with error"""

    def __init__(self, rqst_id: str, status: str, msg: str = None) -> None:
        self.request_id = rqst_id
        self.job_status = status
        self.message = msg

        if self.message is None:
            self.message = self.__doc__
        super().__init__(self.message)

    def __str__(self):
        ret = f"Request Id: {self.request_id}"
        ret = f"{ret}, Job Status: {self.job_status}"
        ret = f"{ret}, Message: {self.message}"
        return ret
