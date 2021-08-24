class LongRunningJobError(Exception):
    """Job is running longer than expected"""

    def __init__(self, request_id: str, message: str = None) -> None:
        self.request_id = request_id
        self.message = message

        if self.message is None:
            self.message = self.__doc__

        super().__init__(self.message)

    def __str__(self):
        return f"Request Id: {self.request_id}, Message: {self.message}"


class ScheduledJobError(Exception):
    """Job ran with error"""

    def __init__(self, request_id: str, job_status: str, message: str = None) -> None:
        self.request_id = request_id
        self.job_status = job_status
        self.message = message

        if self.message is None:
            self.message = self.__doc__

        super().__init__(self.message)

    def __str__(self):
        return f"Request Id: {self.request_id}, Job Status: {self.job_status}, Message: {self.message}"
