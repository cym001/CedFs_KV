use crate::Shared;

pub struct RequestEndOp {
    pub request_id: String,
    pub shared: Shared,
}

impl RequestEndOp {
    pub async fn run(&self) -> anyhow::Result<()> {
        self.shared.active_squence.free(&self.request_id);
        tracing::info!("RequestEndOp: removed request {}", self.request_id);
        Ok(())
    }
}
