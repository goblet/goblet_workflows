from goblet_workflows.workflow import Workflow
from goblet_workflows.exceptions import GobletWorkflowException


class Step:
    def __init__(self, workflow: Workflow, name: str, **kwargs) -> None:
        self.name: str = name
        self.kwargs = kwargs
        self.workflow = workflow
        workflow.register_step(self)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Step) and other.name == self.name

    def __gt__(self, other):
        if isinstance(other, MultiStep):
            for i in range(len(other.steps)):
                if i == 0:
                    first = self
                else:
                    first = other.steps[i - 1]
                self.workflow.add_relationship(first, other.steps[i])
        else:
            self.workflow.add_relationship(self, other)
        return other

    def create_definition(self):
        raise NotImplementedError("create_definition")


class MultiStep:
    def __init__(self, workflow: Workflow, **kwargs) -> None:
        self.workflow = workflow
        self.kwargs = kwargs

    @property
    def steps(self):
        return self._steps()

    def _steps(self):
        raise NotImplementedError("_steps")

    def __gt__(self, other):
        self.workflow.add_relationship(self.steps[-1], other)
        return other


class AssignStep(Step):
    def create_definition(self):
        assignments = [{k: v} for k, v in self.kwargs.items()]
        definition = {self.name: {"assign": assignments}}
        return definition


class CompleteStep(Step):
    def __init__(self, workflow: Workflow, name: str, return_str: str) -> None:
        super().__init__(workflow, name)
        self.return_str = return_str

    def create_definition(self):
        definition = {self.name: {"return": self.return_str}}
        return definition


class HttpStep(Step):
    def create_definition(self):
        optional = {}
        if self.kwargs.get("result"):
            optional["result"] = self.kwargs["result"]
        definition = {
            self.name: {
                "call": f"http.{self.kwargs['call']}",
                "args": self.kwargs["args"],
                **optional,
            }
        }
        return definition


class ConnectionStep(Step):
    def create_definition(self):
        optional = {}
        if self.kwargs.get("result"):
            optional["result"] = self.kwargs["result"]
        definition = {
            self.name: {
                "call": f"http.{self.kwargs['call']}",
                "args": self.kwargs["args"],
                **optional,
            }
        }
        return definition


class BQStep(Step):
    CALL = "googleapis.bigquery.v2.jobs.query"

    def __init__(
        self, workflow: Workflow, name: str, query: str, result="query", **kwargs
    ) -> None:
        self.query = query
        super().__init__(
            workflow,
            name,
            call=self.CALL,
            result=result,
            args={
                "projectId": '${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}',
                "body": {
                    "useLegacySql": "false",
                    "query": self.query,
                    **kwargs.get("body", {}),
                },
            },
            **kwargs,
        )

    def create_definition(self):
        optional = {}
        if self.kwargs.get("result"):
            optional["result"] = self.kwargs["result"]
        definition = {
            self.name: {
                "call": self.kwargs["call"],
                "args": self.kwargs["args"],
                **optional,
            }
        }
        return definition


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


class DataformSteps(MultiStep):
    def __init__(self, workflow: Workflow, git_branch, **kwargs) -> None:
        super().__init__(workflow, **kwargs)
        self.git_branch = git_branch

    def _steps(self):
        return [
            HttpStep(
                self.workflow,
                "createCompilationResult",
                call="post",
                args={
                    "url": '${"https://dataform.googleapis.com/v1beta1/" + repository + "/compilationResults"}',
                    "body": {"gitCommitish": self.git_branch},
                    "auth": {"type": "OAuth2"},
                },
                result="compilationResult",
            ),
            HttpStep(
                self.workflow,
                "createWorkflowInvocation",
                call="post",
                args={
                    "url": '${"https://dataform.googleapis.com/v1beta1/" + repository + "/workflowInvocations"}',
                    "body": {"compilationResult": "${compilationResult.body.name}"},
                    "auth": {"type": "OAuth2"},
                },
                result="workflowInvocation",
            ),
            CompleteStep(
                self.workflow, "complete", return_str="${workflowInvocation.body.name}"
            ),
        ]
