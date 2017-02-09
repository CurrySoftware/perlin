/// One element in the pipeline. Call it by calling apply
/// It will do one of three things with a token
/// 1. Consume it
/// 2. Change it and pass it on
/// 3. Do nothing and pass it on
pub trait PipelineElement<T> {
    fn apply(&self, &str, &mut T);
}


/// Helper trait.
/// Is usefull to be able to ergonomically construct pipelines
pub trait CanChain<T>{
    fn chain_to(T) -> Self;
}

/// Antagonist to `CanChain<T>`
pub trait CanAppend where Self: Sized {
    fn append_to<LHS>(self) -> LHS
        where LHS: CanChain<Self> {
        LHS::chain_to(self)
    }
}

impl<T> CanAppend for T {}

#[macro_export]
macro_rules! pipeline {
    ( $cur:expr, $t:ty > $($x:ty)>+) => {
        pipeline!(pipeline!($cur, $($x)>+), $t)                                    
    };
    ( $cur:expr, $t:ty) => {
        $cur.append_to::<$t>()
    };
    ( $cur:expr ) => {
        $cur;
    }
}
