use super::{generation::ComputeAbsentProof, GenerationProvisionerError};
use crate::store::StoredAnchor;
use async_trait::async_trait;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CleanupProgress {
    Pending,
    Absent(ComputeAbsentProof),
}

/// Narrow lifecycle seam shared by suspension and fail-closed worker recycle.
///
/// Cleanup is derived entirely from the durable anchor. It deliberately does
/// not require the currently loaded worker profile: an anchor pinned to a
/// retired profile revision must remain reclaimable.
///
/// The caller must hold this session's [`super::SessionLocks`] guard for the
/// entire operation. Correctness also requires the trusted controller RBAC to
/// be the only writer of managed compute children. The final LIST plus
/// deterministic GET sequence is deliberately redundant, but it is not an
/// atomic Kubernetes snapshot; session serialization and sole-writer RBAC are
/// what make the returned proof safe to consume.
#[async_trait]
pub trait LifecycleProvisioner: Send + Sync {
    async fn reconcile_compute_absent(
        &self,
        anchor: &StoredAnchor,
    ) -> Result<CleanupProgress, GenerationProvisionerError>;
}
