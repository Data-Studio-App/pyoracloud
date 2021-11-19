import requests
from requests.sessions import Session


class Pod:
    def __init__(
        self,
        url: str,
        username: str,
        password: str,
        verbose: bool = False,
        max_poll: int = 500,
        poll_interval: int = 10,
    ) -> None:
        self.url = url
        self.username = username
        self.password = password
        self.verbose = verbose
        self.max_poll = max_poll
        self.poll_interval = poll_interval  # Sec

    def get_request(
        self,
        headers={
            "content-type": "application/json",
        },
    ) -> Session:
        cloud_request = requests.Session()
        cloud_request.auth = (self.username, self.password)
        cloud_request.headers.update(headers)
        return cloud_request

    def display_message(self, message: str) -> None:
        if self.verbose:
            print(message)
