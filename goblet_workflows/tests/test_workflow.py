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

    def test_check_in_nested_list(self):
        wf = Workflow("test_check_in_nested_list")
        step1 = Step(wf, "1")
        step2 = Step(wf, "2")
        step3 = Step(wf, "3")
        task_list = [step1, step1, [step2]]

        assert wf.check_in_nested_list(step1, task_list)
        assert wf.check_in_nested_list(step2, task_list)
        assert not wf.check_in_nested_list(step3, task_list)

    def test_insert_in_nested_list(self):
        wf = Workflow("test_insert_in_nested_list")
        step1 = Step(wf, "1")
        step2 = Step(wf, "2")
        step3 = Step(wf, "3")
        task_list = [step1, Branch("branch", branches=[step1, step2])]
        wf.insert_nested_child(step2, step3, task_list)
        assert isinstance(task_list[1].branches[1], Branch)

    def test_step_nested_branching(self):
        wf = Workflow("test_step_nested_branching")
        step1 = Step(wf, "1")
        step2 = Step(wf, "2")
        step3 = Step(wf, "3")
        step4 = Step(wf, "4")

        step1 > [step2, step3]
        step2 > step4

        assert len(wf.task_list) == 2
        assert wf.task_list[0] == step1
        assert isinstance(wf.task_list[1], Branch)
        # Assert step 2 and step4 are branched
        assert step2 in wf.task_list[1].branches[0].branches
        assert step4 in wf.task_list[1].branches[0].branches

    def test_step_double_nested_branching(self):
        wf = Workflow("test_step_nested_branching")
        step1 = Step(wf, "1")
        step2 = Step(wf, "2")
        step3 = Step(wf, "3")
        step4 = Step(wf, "4")
        step5 = Step(wf, "5")

        step1 > [step2, step3]
        step2 > [step4, step5]

        assert len(wf.task_list) == 2
        assert wf.task_list[0] == step1
        assert isinstance(wf.task_list[1], Branch)
        assert step2 == wf.task_list[1].branches[0].branches[0]
        assert step4 in wf.task_list[1].branches[0].branches[1].branches
        assert step5 in wf.task_list[1].branches[0].branches[1].branches
