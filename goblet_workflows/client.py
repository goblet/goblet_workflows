from goblet_gcp_client import Client


def create_workflow_client():
    return Client(
        "workflows",
        "v1",
        calls="projects.locations.workflows",
        parent_schema="projects/{project_id}/locations/{location_id}",
    )


def create_execution_client():
    return Client(
        "workflowexecutions",
        "v1",
        calls="projects.locations.workflows.executions",
        parent_schema="projects/{project_id}/locations/{location_id}",
    )


def create_scheduler_client():
    return Client(
        "cloudscheduler",
        "v1",
        calls="projects.locations.jobs",
        parent_schema="projects/{project_id}/locations/{location_id}",
    )
