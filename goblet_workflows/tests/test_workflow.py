from goblet_workflows.steps import Step
from goblet_workflows import Workflow, Branch


class TestWorkflow:
    def test_step_sequence(self):
        wf = Workflow("test_step_sequence")
        step1 = Step(wf, "1")
        step2 = Step(wf, "2")
        step3 = Step(wf, "3")

        step1 > step2 > step3

        assert len(wf.task_list) == 3

    def test_step_branching(self):
        wf = Workflow("test_step_branching")
        step1 = Step(wf, "1")
        step2 = Step(wf, "2")
        step3 = Step(wf, "3")

        step1 > step2

        assert len(wf.task_list) == 2
        assert wf.task_list[0] == step1

        step1 > step3

        assert len(wf.task_list) == 2
        assert wf.task_list[0] == step1
        assert isinstance(wf.task_list[1], Branch)

    def test_step_branching_list(self):
        wf = Workflow("test_step_branching_list")
        step1 = Step(wf, "1")
        step2 = Step(wf, "2")
        step3 = Step(wf, "3")

        step1 > [step2, step3]

        assert len(wf.task_list) == 2
        assert wf.task_list[0] == step1
        assert isinstance(wf.task_list[1], Branch)
