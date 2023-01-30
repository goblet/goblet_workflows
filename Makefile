SPHINXBUILD = sphinx-build

default: html

html:
	cd docs && make html

pypi:
	python3 setup.py sdist bdist_wheel;
	twine upload --skip-existing dist/*;

lint:
	flake8 goblet_workflows

coverage:
	coverage run -m pytest goblet_workflows/tests;
	coverage report -m --include="goblet_workflows/*" --omit="goblet_workflows/tests/*";

tests:
	pytest goblet_workflows/tests;
