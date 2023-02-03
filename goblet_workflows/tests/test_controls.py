from goblet_workflows import Workflow, Branch, For, HttpStep, AssignStep


class TestWorkflow:
    def test_branch(self):
        wf = Workflow("test_branch")
        step1 = HttpStep(wf, "1", {"url": "xxx.com"})
        step2 = HttpStep(wf, "2", {"url": "xxx.com"})
        step3 = HttpStep(wf, "3", {"url": "xxx.com"})

        step1 > Branch("branch", branches=[step2, step3])

        assert wf.create_definition() == {
            "main": {
                "steps": [
                    {"1": {"args": {"url": "xxx.com"}, "call": "http.post"}},
                    {
                        "branch": {
                            "parallel": {
                                "branches": [
                                    {
                                        "2_branch": {
                                            "steps": [
                                                {
                                                    "2": {
                                                        "args": {"url": "xxx.com"},
                                                        "call": "http.post",
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "3_branch": {
                                            "steps": [
                                                {
                                                    "3": {
                                                        "args": {"url": "xxx.com"},
                                                        "call": "http.post",
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                ]
                            }
                        }
                    },
                ]
            }
        }

    def test_for(self):
        wf = Workflow("test_for")
        assign = AssignStep(wf, "assign", workflowScope="foo")
        step1 = HttpStep(wf, "1", {"url": "xxx.com"})
        step2 = HttpStep(wf, "2", {"url": "xxx.com"})

        assign > For(value="loop_value", value_list=[1, 2, 3, 4], steps=[step1, step2])

        assert wf.create_definition() == {
            "main": {
                "steps": [
                    {"assign": {"assign": [{"workflowScope": "foo"}]}},
                    {
                        "for": {
                            "in": [1, 2, 3, 4],
                            "steps": [
                                {
                                    "1": {
                                        "args": {"url": "xxx.com"},
                                        "call": "http.post",
                                    }
                                },
                                {
                                    "2": {
                                        "args": {"url": "xxx.com"},
                                        "call": "http.post",
                                    }
                                },
                            ],
                            "value": "loop_value",
                        }
                    },
                ]
            }
        }
