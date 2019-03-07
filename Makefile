test:
	python3.6 -m unittest discover -s tests

install:
	pip install .[sql-server,s3]
