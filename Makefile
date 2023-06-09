
.PHONY: mock-gen
mock-gen:
	go install github.com/golang/mock/mockgen@v1.6.0
	mockgen -destination=mocks/elasticsearch_handler.go -package=mocks github.com/disaster37/es-handler/v8 ElasticsearchHandler