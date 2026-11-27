//! Identifiers for Origamic overlay networks.

use uuid::Uuid;

/// The shared network identifier for the Omni network.
///
/// Nodes using this identifier participate in the globally shared Omni network,
/// allowing them to communicate with other nodes using the same identifier.
/// Other identifiers can be used to create separate overlay networks.
///
/// Its canonical textual representation is
/// `00000000-0000-0000-0000-000000000000`.
pub const OMNI_NETWORK_ID: Uuid = Uuid::nil();
pub const OMNI2_NETWORK_ID: Uuid = Uuid::max();
