use anyhow::Ok;

use crate::Shared;
use crate::types::DataServer;

pub struct RegisterInstanceOp {
    pub data_server: Vec<DataServer>,
    pub shared: Shared,
}

impl RegisterInstanceOp {
    pub async fn run(&self) -> anyhow::Result<()>{
        Ok(())
    }
}