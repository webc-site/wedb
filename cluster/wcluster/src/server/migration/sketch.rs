use crate::server::migration::sketch_status::SketchStatus;
use parking_lot::RwLock;

/// libs/cluster/Server/Migration/Sketch.cs:Sketch
pub struct Sketch {
    pub status: RwLock<SketchStatus>,
}

impl Sketch {
    pub fn new() -> Self {
        Self { status: RwLock::new(SketchStatus::Initializing) }
    }
    
    pub fn set_status(&self, status: SketchStatus) {
        *self.status.write() = status;
    }
    
    pub fn clear(&self) {}
}

impl Default for Sketch {
    fn default() -> Self {
        Self::new()
    }
}

impl Sketch {
    /// libs/cluster/Server/Migration/Sketch.cs:TryHashAndStore
    pub fn try_hash_and_store(&self) -> bool { true }

    /// libs/cluster/Server/Migration/Sketch.cs:HashAndStore
    pub fn hash_and_store(&self) {}

    /// libs/cluster/Server/Migration/Sketch.cs:Probe
    pub fn probe(&self) -> bool { true }
}
