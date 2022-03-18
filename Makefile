.PHONY: container test

include .env

container:
	AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID); AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY); aws ecr get-login-password --region $(AWS_REGION) | docker login --username AWS --password-stdin $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com
	docker build -t $(CONTAINER_NAME) .
	docker tag $(CONTAINER_NAME):latest $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(CONTAINER_NAME):latest
	docker push $(AWS_ACCOUNT_ID).dkr.ecr.${AWS_REGION}.amazonaws.com/$(CONTAINER_NAME):latest