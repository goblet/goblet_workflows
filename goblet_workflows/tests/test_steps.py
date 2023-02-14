from goblet_workflows import Workflow, DataformSteps, AssignStep


class TestSteps:
    def test_dataform_steps(self):
        WORKFLOW_NAME = "test_dataform"
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

        assert wf.create_definition() == {
            "main": {
                "steps": [
                    {
                        "init": {
                            "assign": [
                                {
                                    "repository": "projects/PROJECT_ID/locations/us-central1/repositories/REPOSITORY_ID"
                                }
                            ]
                        }
                    },
                    {
                        "createCompilationResult": {
                            "call": "http.post",
                            "args": {
                                "url": '${"https://dataform.googleapis.com/v1beta1/" + repository + "/compilationResults"}',
                                "body": {"gitCommitish": "main"},
                                "auth": {"type": "OAuth2"},
                            },
                            "result": "compilationResult",
                        }
                    },
                    {
                        "createWorkflowInvocation": {
                            "call": "http.post",
                            "args": {
                                "url": '${"https://dataform.googleapis.com/v1beta1/" + repository + "/workflowInvocations"}',
                                "body": {
                                    "compilationResult": "${compilationResult.body.name}"
                                },
                                "auth": {"type": "OAuth2"},
                            },
                            "result": "workflowInvocation",
                        }
                    },
                    {"complete": {"return": "${workflowInvocation.body.name}"}},
                ]
            }
        }
