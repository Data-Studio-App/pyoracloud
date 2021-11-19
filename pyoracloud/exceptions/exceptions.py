class LongRunningJobError(Exception):
    """
    Exception raised when the job is still running.
    """

    def __init__(self, rqst_id: str) -> None:
        self.request_id = rqst_id
        super().__init__(self.__doc__)

    def __str__(self):
        return f"Request Id: {self.request_id}, Message: {self.message}"


class ScheduledJobError(Exception):
    """
    Exception raised when the job ran with Error.
    """

    def __init__(self, rqst_id: str, status: str) -> None:
        self.request_id = rqst_id
        self.job_status = status
        super().__init__(self.__doc__)

    def __str__(self):
        return f"Request Id: {self.request_id}, Job Status: {self.job_status}"
