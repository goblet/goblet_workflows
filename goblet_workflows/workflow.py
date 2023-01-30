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
    get_default_project,
    get_default_location,
)

import logging

log = logging.getLogger("goblet_workflows")
log.setLevel(logging.INFO)

# ignore aliases when dumping
representer.RoundTripRepresenter.ignore_aliases = lambda x, y: True


class Workflow:
    steps = {}
    task_list = []

    def __init__(self, name, params=None, **kwargs) -> None:
        self.name = name
        self.params = params

    def register_step(self, step):
        self.steps[step.name] = step

    def add_relationship(self, child, parent):
        if child not in self.task_list and len(self.task_list) > 0:
            raise GobletWorkflowException(
                f"{child.name} doesnt exist in current workflow"
            )
        if child in self.task_list:
            if not parent:
                raise GobletWorkflowException("Parent is None")
            index = self.task_list.index(child)
            # child is last
            if index == len(self.task_list) - 1:
                self.task_list.append(parent)
            else:
                self.task_list[index + 1] = [parent, self.task_list[index + 1]]

        else:
            self.task_list.append(child)
            if parent:
                self.task_list.append(parent)

    def create_definition(self):
        main_definition = {}
        if self.params:
            main_definition["params"] = self.params

        steps = []
        for task in self.task_list:
            # TODO: handle case when task is itself a list
            # Should create breanch behind scenes or return error
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

        resp = workflow_client.wait_for_operation(operation["name"])
        if resp.get("error"):
            raise GobletWorkflowYAMLException(**resp["error"])
