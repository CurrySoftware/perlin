#[macro_export]
macro_rules! query_pipeline {
    ($($x:tt)*) => {
        Box::new(move |index, query| {
            use $crate::language::CanApply;
            use $crate::query::{Funnel, Operator, Operand, AndConstructor, OrConstructor};

            let mut pipeline = inner_query_pipe!(;index; $($x)*);
            pipeline.apply(query);
            pipeline.to_operand()
        })
    }
}
