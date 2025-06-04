# Tasks For Reactor


## gen_client

> Generates rpc_client using openapi client generator

~~~sh
echo "Running demo server..."
cd examples/ping_pong && cargo run --features swagger,dynop node --port 3000 &
SERVER_PID=$!
sleep 5
echo "Generating client"
docker run --rm --network=host -v $PWD:/local -u $(id -u):$(id -g)  \
  openapitools/openapi-generator-cli generate -i http://localhost:3000/api-doc/openapi.json \
  -g rust -o /local/rpc_client/ --additional-properties=packageName=reactor-client
kill $SERVER_PID
~~~


