use std::collections::BTreeMap;

use futures::future::join_all;
use placement::{Hostname, LogicalOp, PhysicalOp, PlacementManager};
use reactor_client::{
    self,
    models::{RemoteActorInfo, SpawnArgs},
};

pub mod placement;

struct NodeHandle {
    client_config: reactor_client::apis::configuration::Configuration,
    actors: Vec<RemoteActorInfo>,
    operators: Vec<LogicalOp>,
}

impl NodeHandle {
    async fn register_op(&mut self, op_info: &LogicalOp) {
        reactor_client::apis::default_api::register_op(
            &self.client_config,
            reactor_client::models::RegistrationArgs {
                args: op_info.compile_info.clone(),
                op_name: op_info.name.clone(),
            },
        )
        .await
        .unwrap();
        self.operators.push(op_info.clone());
    }

    async fn place(&mut self, physical_op: &PhysicalOp) -> RemoteActorInfo {
        let remote_actor_info = reactor_client::apis::default_api::start_actor(
            &self.client_config,
            SpawnArgs {
                actor_name: physical_op.actor_name.clone(),
                operator_name: physical_op.logical.name.clone(),
            },
        )
        .await
        .unwrap();
        self.actors.push(remote_actor_info.clone());
        remote_actor_info
    }

    async fn notify_remote_actor_added(&self, remote_actor: &RemoteActorInfo) {
        reactor_client::apis::default_api::actor_added(&self.client_config, remote_actor.clone())
            .await
            .unwrap();
    }

    async fn stop_all_actors(&self) {
        reactor_client::apis::default_api::stop_all_actors(&self.client_config)
            .await
            .unwrap();
    }
}

pub struct JobController<PM> {
    pm: PM,
    nodes: BTreeMap<String, NodeHandle>,
}

impl<PM: PlacementManager> JobController<PM> {
    pub fn new(pm: PM) -> JobController<PM> {
        JobController {
            pm,
            nodes: BTreeMap::new(),
        }
    }
    pub fn register_node(&mut self, name: &str, hostname: Hostname) {
        self.nodes.insert(
            name.to_string(),
            NodeHandle {
                client_config: self.client_config(hostname),
                actors: Vec::new(),
                operators: Vec::new(),
            },
        );
    }

    pub async fn register_op(&mut self, op_info: &LogicalOp, node_name: &str) {
        let node_handle = self
            .nodes
            .get_mut(node_name)
            .expect("Node must be register before placement");
        node_handle.register_op(op_info).await;
    }

    pub async fn start_job(&mut self, ops: Vec<LogicalOp>) {
        for op in ops {
            for physical_op in self.pm.place(&op) {
                let remote_actor_info = self
                    .nodes
                    .get_mut(&physical_op.nodename)
                    .expect("Node must be register before placement")
                    .place(&physical_op)
                    .await;
                let handles: Vec<_> = self
                    .nodes
                    .iter()
                    .filter_map(|(k, v)| {
                        if *k != physical_op.nodename {
                            Some(v)
                        } else {
                            None
                        }
                    })
                    .map(|node| async {
                        node.notify_remote_actor_added(&remote_actor_info).await;
                    })
                    .collect();
                join_all(handles).await;
            }
        }
    }

    pub async fn stop_job(mut self) {
        while let Some((_, node_handle)) = self.nodes.pop_first() {
            node_handle.stop_all_actors().await;
        }
    }

    fn client_config(
        &self,
        hostname: Hostname,
    ) -> reactor_client::apis::configuration::Configuration {
        let mut config = reactor_client::apis::configuration::Configuration::default();
        config.base_path = format!("http://{}:{}", hostname, 3000);
        config
    }
}
