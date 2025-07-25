install: install_node install_jobc build_ops
install_node:
	cargo install --debug --path ../../generic_nctrl/

install_jobc:
	cargo install --debug --path ../../generic_jctrl/

build_ops:
	cargo build

kill_node:
	- kill $(lsof -ti :3000)

node: kill_node
	reactor_nctrl --port 3000 target/release
	
job:
	reactor_jctrl ping_pong.toml
