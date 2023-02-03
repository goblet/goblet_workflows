from goblet_workflows.steps import Step
from goblet_workflows.exceptions import GobletWorkflowException


class Branch(Step):
    def __init__(self, name: str, branches=[], shared=None, **kwargs) -> None:
        self.name: str = name
        self.kwargs = kwargs
        self.shared = shared
        self.branches = branches
        if self.branches:
            self.workflow = self.branches[0].workflow
            self.workflow.register_step(self)

    def __call__(self, branches):
        self.branches = branches
        if self.branches:
            self.workflow = self.branches[0].workflow
            self.workflow.register_step(self)
        return self

    def append(self, parent):
        self.branches.append(parent)

    def create_definition(self):
        if not self.branches:
            raise GobletWorkflowException("At least one branch step is required")
        optional = {}
        if self.shared:
            optional["shared"] = self.shared

        branches = []
        for v in self.branches:
            if isinstance(v, Step):
                branches.append(
                    {f"{v.name}_branch": {"steps": [v.create_definition()]}}
                )
            if isinstance(v, list):
                branches.append(
                    {
                        f"{v[0].name}_branch": {
                            "steps": [s.create_definition() for s in v]
                        }
                    }
                )

        definition = {self.name: {"parallel": {**optional, "branches": branches}}}
        return definition


class For(Step):
    def __init__(self, value: str, value_list: list, steps=[], **kwargs) -> None:
        self.value: str = value
        self.value_list: list = value_list
        self.kwargs = kwargs
        self.steps = steps
        # TODO: Unique name for for element
        self.name = "For"
        if self.steps:
            self.workflow = self.steps[0].workflow
            self.workflow.register_step(self)

    def __call__(self, steps):
        if not steps:
            raise GobletWorkflowException("At least one step is required")
        self.steps = steps
        if isinstance(self.steps, Step):
            self.steps = [steps]
        self.workflow = self.steps[0].workflow
        self.workflow.register_step(self)
        return self

    def create_definition(self):
        if not self.steps:
            raise GobletWorkflowException("At least one step is required")

        definition = {
            "for": {
                "value": self.value,
                "in": self.value_list,
                "steps": [s.create_definition() for s in self.steps],
            }
        }
        return definition
