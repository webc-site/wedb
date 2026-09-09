use std::marker::PhantomData;

/// libs/client/AsyncPool.cs:AsyncPool
pub struct AsyncPool<T> {
    _marker: PhantomData<T>,
}

impl<T> AsyncPool<T> {
    /// libs/client/AsyncPool.cs:Get
    pub fn get(&self) -> Option<T> { None }
    /// libs/client/AsyncPool.cs:GetAsync
    pub async fn get_async(&self) -> Option<T> { None }
    /// libs/client/AsyncPool.cs:TryGet
    pub fn try_get(&self) -> Option<T> { None }
    /// libs/client/AsyncPool.cs:Return
    pub fn return_item(&self, _item: T) {}
    /// libs/client/AsyncPool.cs:Dispose
    pub fn dispose(&self) {}
}

/// libs/client/LightEpoch.cs:LightEpoch
pub struct LightEpoch;

impl LightEpoch {
    /// libs/client/LightEpoch.cs:ActiveInstanceCount
    pub fn active_instance_count() -> i32 { 0 }
    /// libs/client/LightEpoch.cs:ResetAllInstances
    pub fn reset_all_instances() {}
    /// libs/client/LightEpoch.cs:Dispose
    pub fn dispose(&self) {}
    /// libs/client/LightEpoch.cs:ThisInstanceProtected
    pub fn this_instance_protected(&self) -> bool { false }
    /// libs/client/LightEpoch.cs:TrySuspend
    pub fn try_suspend(&self) -> bool { false }
    /// libs/client/LightEpoch.cs:ProtectAndDrain
    pub fn protect_and_drain(&self) {}
    /// libs/client/LightEpoch.cs:SuspendResume
    pub fn suspend_resume(&self) {}
    /// libs/client/LightEpoch.cs:Suspend
    pub fn suspend(&self) {}
    /// libs/client/LightEpoch.cs:Resume
    pub fn resume(&self) {}
    /// libs/client/LightEpoch.cs:BumpCurrentEpoch
    pub fn bump_current_epoch(&self) {}
}

/// libs/client/CompletionEvent.cs:CompletionEvent
pub struct CompletionEvent;

/// libs/client/TcsWrapper.cs:TcsWrapper
pub struct TcsWrapper;

/// libs/client/Utility.cs:Utility
pub struct Utility;
