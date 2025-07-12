install: install_node install_jobc build_ops
	
install_node:
	cargo install --path ../../generic_nctrl/

install_jobc:
	cargo install --path ../../generic_jctrl/

build_ops:
	cargo build --release

node:
	reactor_nctrl --port 3000 target/release
	
job:
	reactor_jctrl ping_pong.toml
