from goblet_workflows.steps import Step
from goblet_workflows import Workflow, Branch, HttpStep
from goblet_gcp_client import get_responses, get_replay_count


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


class TestWorkflowDeployment:
    def test_deploy_workflow(self, monkeypatch):
        monkeypatch.setenv("G_HTTP_TEST", "REPLAY")
        monkeypatch.setenv("G_TEST_NAME", "deploy_workflow")
        monkeypatch.setenv("GOOGLE_LOCATION", "us-central1")
        monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test_project")
        # monkeypatch.setenv("G_TEST_DATA_DIR", join(dirname(__file__), "data"))
        monkeypatch.setenv("G_REPLAY_COUNT", "0")

        wf = Workflow("test_deploy_workflow")
        step1 = HttpStep(wf, "1", args={"url": "test.com"})
        step2 = HttpStep(wf, "2", args={"url": "test.com"})

        step1 > step2

        wf.deploy()

        resp = get_responses("deploy_workflow")
        assert (
            resp[1]["body"]["metadata"]["target"]
            == "projects/test_project/locations/us-central1/workflows/test_deploy_workflow"
        )
        assert get_replay_count() == 2

    def test_update_workflow(self, monkeypatch):
        monkeypatch.setenv("G_HTTP_TEST", "REPLAY")
        monkeypatch.setenv("G_TEST_NAME", "update_workflow")
        monkeypatch.setenv("GOOGLE_LOCATION", "us-central1")
        monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test_project")
        # monkeypatch.setenv("G_TEST_DATA_DIR", join(dirname(__file__), "data"))
        monkeypatch.setenv("G_REPLAY_COUNT", "0")

        wf = Workflow("test_deploy_workflow")
        step1 = HttpStep(wf, "1", args={"url": "test.com"})
        step2 = HttpStep(wf, "2", args={"url": "test.com"})

        step1 > step2

        wf.deploy()

        resp = get_responses("update_workflow")
        assert (
            resp[1]["body"]["metadata"]["target"]
            == "projects/test_project/locations/us-central1/workflows/test_deploy_workflow"
        )
        assert get_replay_count() == 3

    def test_deploy_workflow_schedule(self, monkeypatch):
        monkeypatch.setenv("G_HTTP_TEST", "REPLAY")
        monkeypatch.setenv("G_TEST_NAME", "deploy_workflow_schedule")
        monkeypatch.setenv("GOOGLE_LOCATION", "us-central1")
        monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test_project")
        monkeypatch.setenv("G_REPLAY_COUNT", "0")

        wf = Workflow(
            "test_deploy_workflow",
            serviceAccount="test@test-project.iam.gserviceaccount.com",
        )
        step1 = HttpStep(wf, "1", args={"url": "test.com"})
        step2 = HttpStep(wf, "2", args={"url": "test.com"})
        wf.set_schedule("* * * * *")

        step1 > step2

        wf.deploy()

        resp = get_responses("deploy_workflow_schedule")
        assert "test_deploy_workflow" in resp[1]["body"]["httpTarget"]["uri"]
        assert (
            resp[2]["body"]["metadata"]["target"]
            == "projects/test_project/locations/us-central1/workflows/test_deploy_workflow"
        )
        assert get_replay_count() == 3

    def test_execute_workflow(self, monkeypatch):
        monkeypatch.setenv("G_HTTP_TEST", "REPLAY")
        monkeypatch.setenv("G_TEST_NAME", "execute_workflow")
        monkeypatch.setenv("GOOGLE_LOCATION", "us-central1")
        monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test_project")
        # monkeypatch.setenv("G_TEST_DATA_DIR", join(dirname(__file__), "data"))
        monkeypatch.setenv("G_REPLAY_COUNT", "0")

        wf = Workflow("test_deploy_workflow")
        wf.execute()

        resp = get_responses("execute_workflow")
        assert "test_deploy_workflow" in resp[0]["body"]["name"]
        assert "ACTIVE" != resp[0]["body"]["state"]
        assert get_replay_count() == 2
