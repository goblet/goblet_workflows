import os
import importlib.util
from contextlib import contextmanager
import sys


@contextmanager
def add_to_path(p):
    old_path = sys.path
    sys.path = sys.path[:]
    sys.path.insert(0, p)
    try:
        yield
    finally:
        sys.path = old_path


def get_workflow_from_module(m):
    # TODO: select specific workflow
    from goblet_workflows import Workflow

    for obj in dir(m):
        if isinstance(getattr(m, obj), Workflow):
            return getattr(m, obj), obj


def get_workflow(main_file="main.py"):
    """Look for main.py or main_file if defined and return workflow instance."""
    dir_path = os.path.realpath(".")
    spec = importlib.util.spec_from_file_location("main", f"{dir_path}/{main_file}")
    main = importlib.util.module_from_spec(spec)
    with add_to_path(dir_path):
        spec.loader.exec_module(main)
        workflow, app_name = get_workflow_from_module(main)
    return workflow
