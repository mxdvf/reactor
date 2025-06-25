## This example can be run in two modes:- with and without codegen

### Running Without Codegen
In this case the library is already present in `target/debug` on the node.

#### Build generic Node Controller
`cd ../node_controller`
`cargo build --release`

#### Build Operator, Job Manager and Codegen specific Node Controller
`cargo build --release`

#### Start Node Controller
`make node_controller`

#### Start Job Manager
`make job_manager`



[FIXME]
### Running With Codegen
In this case the library is compiled by the node using the source sent by the job manger.
#### Start Node
`cargo run --features dynop node --port 3000 target/debug`
#### Start Job
`cargo run --features dynop node job-manager`
