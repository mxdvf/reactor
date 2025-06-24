use std::{
    collections::{BTreeMap, HashMap},
    iter,
};

pub type Hostname = &'static str;

pub struct Placement {
    hostname_to_num: BTreeMap<&'static str, u32>,
}

impl Placement {
    pub fn num(&self) -> u32 {
        self.hostname_to_num.values().sum::<u32>()
    }
    pub fn iter(&self) -> impl Iterator<Item = Hostname> + '_ {
        self.hostname_to_num
            .iter()
            .flat_map(|(hostname, &count)| iter::repeat_n(*hostname, count as usize))
    }
}

#[derive(Debug, Clone)]
pub struct LibInfo {
    pub name: String,
    pub compile_info: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct LogicalOp {
    pub name: String,
    pub lib_name: String,
}

#[derive(Debug, Clone)]
pub struct PhysicalOp {
    pub logical: LogicalOp,
    pub nodename: String,
    pub actor_name: String,
    pub idx: u32,
    pub peers: u32,
}

/// Takes logical Op  and places it on single or multiple Nodes. Returns list of Physical operator where a logical operator is placed
pub trait PlacementManager {
    fn place(&self, op_info: &LogicalOp) -> impl Iterator<Item = PhysicalOp>;
}

pub struct ManualPlacementManager {
    pub map: HashMap<String, Vec<PhysicalOp>>,
}

impl PlacementManager for ManualPlacementManager {
    fn place(&self, op_info: &LogicalOp) -> impl Iterator<Item = PhysicalOp> {
        self.map.get(&op_info.name).unwrap().iter().cloned()
    }
}
