gen_http_client:
	echo "Running demo server..."
	- pkill reactor_nctrl
	# - kill $(lsof -ti :3000)
	cargo run --features swagger --bin reactor_nctrl -- --port 3000 /tmp &
	SERVER_PID=$!
	sleep 5
	echo "Generating client"
	# docker run --rm --network=host -v $PWD:/local -u $(id -u):$(id -g)  \
	# 	openapitools/openapi-generator-cli generate -i http://host.docker.internal:3000/api-doc/openapi.json \
	# 	-g rust -o /local/rpc_client/ --additional-properties=packageName=reactor-client
	docker run --rm --network=host -v $PWD:/local -u $(id -u):$(id -g)  \
		openapitools/openapi-generator-cli generate -i http://localhost:3000/api-doc/openapi.json \
		-g rust -o /local/rpc_client/ --additional-properties=packageName=reactor-client
	kill ${SERVER_PID}
