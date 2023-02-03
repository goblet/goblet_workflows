import click
import logging
import sys

from goblet_workflows.utils import get_workflow
from goblet_workflows.__version__ import __version__

logging.basicConfig()


@click.group()
def main():
    pass


@main.command()
def help():
    click.echo(
        "Use goblet --help. You can also view the full docs for goblet at https://goblet.github.io/goblet/docs/build/html/index.html"
    )


@main.command()
def version():
    """current goblet version"""
    click.echo(__version__)


@main.command()
@click.option("--schedule", "schedule", is_flag=True)
@click.option("-f", "--file", "file", envvar="FILE")
def deploy(schedule, file):
    """
    Deploy a workflow
    """
    try:
        workflow = get_workflow(file or "main.py")
        if schedule:
            workflow.deploy_scheduler()
        else:
            workflow.deploy()

    except FileNotFoundError as not_found:
        click.echo(
            f"Missing {not_found.filename}. Make sure you are in the correct directory and this file exists"
        )
        sys.exit(1)


@main.command()
def print():
    """
    Deploy a workflow
    """
    try:
        workflow = get_workflow()
        workflow.print_yaml()

    except FileNotFoundError as not_found:
        click.echo(
            f"Missing {not_found.filename}. Make sure you are in the correct directory and this file exists"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
