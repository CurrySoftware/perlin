#[macro_export]
macro_rules! use_parent_crate{
    ($($x:ident)::*) =>  {
        use $crate::$($x)::*;
    }
}
