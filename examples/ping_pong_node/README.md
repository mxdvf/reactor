## This example can be run in two modes:- with and without codegen

### Running Without Codegen
In this case the library is already present in `target/debug` on the node.

#### Build Operator
`cd ping_pong_actor`
`cargo build`

#### Start Node
`cargo run node --port 3000 ../ping_pong_actor/target/debug`
#### Start Job
`cargo run node job-manager`


### Running With Codegen
In this case the library is compiled by the node using the source sent by the job manger.
#### Start Node
`cargo run --features dynop node --port 3000 target/debug`
#### Start Job
`cargo run --features dynop node job-manager`
