from goblet_workflows.workflow import Workflow
from goblet_workflows.steps import AssignStep, BQStep

w = Workflow("bigquery-step-example", params=["column"])

upack_args = AssignStep(w, "upack_args", column="${column}")

bq_step = BQStep(w, "bq_step", "select {column} from DATASET limit 1")

bq_step_2 = BQStep(
    w, "bq_step_2", "select colomn2 from DATASET2 where column1 = {bq_step[0]} "
)

upack_args > bq_step > bq_step_2
