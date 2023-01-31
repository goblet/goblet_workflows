from goblet_workflows.workflow import Workflow
from goblet_workflows.steps import AssignStep, DataformSteps

# SET VARS
WORKFLOW_NAME = "dataform-example"
SERVICE_ACCOUNT = "SERVICE_ACCOUNT"
PROJECT_ID = "PROJECT_ID"
REPOSITORY_LOCATION = "us-central1"
REPOSITORY_ID = "REPOSITORY_ID"
DATAFORM_GIT_BRANCH = "main"

wf = Workflow(WORKFLOW_NAME, serviceAccount=SERVICE_ACCOUNT)
wf.set_schedule("0 * * * *")

assign = AssignStep(
    wf,
    "init",
    repository=f"projects/{PROJECT_ID}/locations/{REPOSITORY_LOCATION}/repositories/{REPOSITORY_ID}",
)

df = DataformSteps(wf, git_branch=DATAFORM_GIT_BRANCH)

assign > df
