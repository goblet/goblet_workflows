class GobletWorkflowException(Exception):
    pass


class GobletWorkflowYAMLException(Exception):
    def __init__(self, message, code) -> None:
        self.mesage = message
        self.code = code
        super().__init__(f"Error code {code}:\n\n {message}")
