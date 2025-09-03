temporal-start:
	temporal server start-dev

temporal-add-search-attributes:
	temporal operator search-attribute create --name="WaitingForResume" --type="Bool"

temporal-worker-start:
	python -m temporal.worker
