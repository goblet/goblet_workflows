from goblet_workflows.steps import Step
from goblet_workflows import Workflow


class TestWorkflow:
    def test_gt(self):
        wf = Workflow("test-gt")
        step1 = Step(wf, "1")
        step2 = Step(wf, "2")
        step3 = Step(wf, "3")
        step4 = Step(wf, "4")
        step5 = Step(wf, "5")

        step1 > step2

        assert len(wf.task_list) == 2
        assert wf.task_list[0] == step1

        step1 > step3

        assert len(wf.task_list) == 2
        assert wf.task_list[0] == step1
        assert len(wf.task_list[1]) == 2
        wf.print_yaml()

        # TODO: NESTED child
        # step3 > [step4, step5]

        # assert len(wf.task_list) == 2
        # import pdb; pdb.set_trace()
        # assert len(wf.task_list[1][1]) == 2

    def test_gt(self):
        wf = Workflow("test-gt")
        step1 = Step(wf, "1")
        step2 = Step(wf, "2")
        step3 = Step(wf, "3")

        step1 > [step2, step3]
