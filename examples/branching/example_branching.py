from goblet_workflows.workflow import Workflow
from goblet_workflows.steps import AssignStep, HttpStep, Branch

wf = Workflow("branching-example", params=["args", "var1"])

http_defaults = {"auth": {"type": "OIDC"}, "timeout": 120}

upack_args = AssignStep(
    wf,
    "upack_args",
    var1="${var1}",
    project_id="${args.project_id}"
)

step_1 = HttpStep(
    wf,
    "step_1",
    call="post",
    args={
        "url": '${"https://us-central1-" + args.project_id + ".cloudfunctions.net/step1"}',
        **http_defaults,
    },
)
step_2 = HttpStep(
    wf,
    "step_2",
    call="post",
    args={
        "url": '${"https://us-central1-" + args.project_id + ".cloudfunctions.net/step2"}',
        "body": {"var": "${var1}"},
        **http_defaults,
    },
)

step_3 = HttpStep(
    wf,
    "step_3",
    call="post",
    args={
        "url": '${"https://us-central1-" + args.project_id + ".cloudfunctions.net/step3"}',
        **http_defaults,
    },
)
step_4 = HttpStep(
    wf,
    "step_4",
    call="post",
    args={
        "url": '${"https://us-central1-" + args.project_id + ".cloudfunctions.net/step4"}',
        **http_defaults,
    },
)
step_5 = HttpStep(
    wf,
    "step_5",
    call="post",
    args={
        "url": '${"https://us-central1-" + args.project_id + ".cloudfunctions.net/step5"}',
        **http_defaults,
    },
)

branch = Branch(
    name="branch",
    shared=["var2"]
)

# Define Workflow Order
upack_args > step_1 > step_2 > branch([step_3, [step_4, step_5]])

