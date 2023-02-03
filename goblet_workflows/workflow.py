from ruamel.yaml import YAML, representer
import io
import sys
from googleapiclient.errors import HttpError

from goblet_workflows.exceptions import (
    GobletWorkflowException,
    GobletWorkflowYAMLException,
)
from goblet_workflows.client import (
    create_workflow_client,
    create_scheduler_client,
    get_default_project,
    get_default_location,
)

from goblet_workflows.controls import Branch

import logging

log = logging.getLogger("goblet_workflows")
log.setLevel(logging.INFO)

# ignore aliases when dumping
representer.RoundTripRepresenter.ignore_aliases = lambda x, y: True


class Workflow:
    def __init__(self, name, params=None, serviceAccount=None, **kwargs) -> None:
        self.steps = {}
        self.task_list = []
        self.name = name
        self.params = params
        self.serviceAccount = serviceAccount
        self._schedule = None
        self.counter = 1

    def register_step(self, step):
        self.steps[step.name] = step

    def add_relationship(self, child, parent):
        if (
            not self.check_in_nested_list(child, self.task_list)
            and len(self.task_list) > 0
        ):
            raise GobletWorkflowException(
                f"{child.name} doesnt exist in current workflow"
            )
        if isinstance(parent, list):
            parent = Branch(name=f"branch-{self.counter}", branches=parent)
            self.counter += 1
        if child in self.task_list:
            if not parent:
                raise GobletWorkflowException("Parent is None")
            index = self.task_list.index(child)
            # child is last
            if index == len(self.task_list) - 1:
                self.task_list.append(parent)
            # handing branching
            else:
                if isinstance(self.task_list[index + 1], Branch):
                    self.task_list[index + 1].append(parent)
                else:
                    self.task_list[index + 1] = Branch(
                        name=f"branch-{self.counter}",
                        branches=[self.task_list[index + 1], parent],
                    )
                    self.counter += 1
        # child is in nested branch
        elif self.check_in_nested_list(child, self.task_list):
            self.insert_nested_child(child, parent, self.task_list)
        else:
            self.task_list.append(child)
            if parent:
                self.task_list.append(parent)

    def check_in_nested_list(self, child, task_list):
        for item in task_list:
            if child == item:
                return True
            if isinstance(item, list):
                return self.check_in_nested_list(child, item)
            if isinstance(item, Branch):
                return self.check_in_nested_list(child, item.branches)
        return False

    def insert_nested_child(self, child, parent, task_list):
        for index, item in enumerate(task_list):
            if child == item:
                task_list[index] = Branch(
                    name=f"branch-{self.counter}",
                    branches=[task_list[index], parent],
                )
                self.counter += 1
                return
            if isinstance(item, list):
                return self.insert_nested_child(child, parent, item)
            if isinstance(item, Branch):
                return self.insert_nested_child(child, parent, item.branches)

    def set_schedule(
        self, schedule, timezone="UTC", httpTarget={}, serviceAccount=None, **kwargs
    ):
        if not serviceAccount and not self.serviceAccount:
            raise ValueError(
                "Missing serviceAccount defined in schedule or in workflow"
            )
        self._schedule = {
            "name": f"projects/{get_default_project()}/locations/{get_default_location()}/jobs/goblet-worfklow-{self.name}",
            "schedule": schedule,
            "timeZone": timezone,
            "httpTarget": {
                "uri": f"https://workflowexecutions.googleapis.com/v1/projects/{get_default_project()}/locations/{get_default_location()}/workflows/{self.name}/executions",
                "oauthToken": {
                    "serviceAccountEmail": serviceAccount or self.serviceAccount
                },
                **httpTarget,
            },
            **kwargs,
        }

    def deploy_scheduler(self):
        scheduler_client = create_scheduler_client()
        try:
            scheduler_client.execute("create", params={"body": self._schedule})
            log.info(f"created scheduled job for {self.name}")
        except HttpError as e:
            if e.resp.status == 409:
                log.info(f"updated scheduled job for {self.name}")
                scheduler_client.execute(
                    "patch",
                    parent_key="name",
                    parent_schema=self._schedule["name"],
                    params={"body": self._schedule},
                )
            else:
                raise e

    def create_definition(self):
        main_definition = {}
        if self.params:
            main_definition["params"] = self.params

        steps = []
        for task in self.task_list:
            # TODO: handle case when task is itself a list
            # Should create branch behind scenes or return error
            steps.append(task.create_definition())
        main_definition["steps"] = steps
        return {"main": main_definition}

    def get_yaml_defintion(self):
        definition = self.create_definition()
        yaml = YAML()
        buf = io.BytesIO()
        yaml.dump(definition, buf)
        return buf.getvalue().decode()

    def print_yaml(self):
        definition = self.create_definition()
        yaml = YAML()
        yaml.dump(definition, sys.stdout)

    def deploy(self):
        body = {
            "name": f"projects/{get_default_project()}/locations/{get_default_location()}/workflows/{self.name}",
            "description": "goblet workflow",
            "sourceContents": self.get_yaml_defintion(),
            "serviceAccount": self.serviceAccount,
        }

        try:
            workflow_client = create_workflow_client()
            operation = workflow_client.execute(
                "create", params={"workflowId": self.name, "body": body}
            )
            log.info(f"Deploying workflow {self.name}...")
        except HttpError as e:
            if e.resp.status == 409:
                operation = workflow_client.execute(
                    "patch",
                    parent_schema=body["name"],
                    parent_key="name",
                    params={"body": body},
                )
                log.info(f"Workflow {self.name} already exists. Updating workflow...")
            else:
                raise e

        if self._schedule:
            self.deploy_scheduler()

        resp = workflow_client.wait_for_operation(operation["name"])
        if resp.get("error"):
            raise GobletWorkflowYAMLException(**resp["error"])
