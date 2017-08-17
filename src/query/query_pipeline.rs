#[macro_export]
macro_rules! operand {
    (;$INDEX:ident; $chaining_op:ident [$operator:ident in $this_field:ident]) => {
        Funnel::create(ChainingOperator::$chaining_op, &$INDEX.$this_field)
    };
}
#[macro_export]
macro_rules! inner_query_pipe {
    (;$INDEX:ident;
     > $($x:tt)*) => {
        // >
        inner_query_pipe!(;$INDEX; $($x)*)
    };
    (;$INDEX:ident;
     $element:ident($($param:expr),+)
     | $chain:ident [$operator:ident in $this_field:ident] $($x:tt)*) =>
    // Element(params) | [OP in field]
    {
        $element::create($($param),+ ,
                         operand!(;$INDEX; $chain [$operator in $this_field]),
                         inner_query_pipe!(;$INDEX; $($x)*))
    };
    (;$INDEX:ident;
     $element:ident($($param:expr),+) $($x:tt)*) =>
    // Element(params)
    {
        $element::create($($param),+ , inner_query_pipe!(;$INDEX; $($x)*))
    };
    (;$INDEX:ident;
     $element:ident | $chain:ident [$operator:ident in $this_field:ident] $($x:tt)*) =>
    // Element | [OP in field]
    {
        $element::create(
            operand!(;$INDEX; $chain [$operator in $this_field]),
            inner_query_pipe!(;$INDEX; $($x)*))
    };
    (;$INDEX:ident;
     $chain:ident [$operator:ident in $this_field:ident]) => {
        // Must [All in field]
        Funnel::create(ChainingOperator::$chain, &$INDEX.$this_field)
    };
    // (;$INDEX:ident;
    //  $chain:ident [$operator:ident in $this_field:ident] $($x:tt)*) => {
    //     // Must [All in field]
    //     SplitFunnel::create(
    //         ChainingOperator::$chain, Combinator::$operator, &$INDEX.$this_field,
    //         inner_query_pipe!(;$INDEX; $($x)*))
    // };
    (;$INDEX:ident;
     $element:ident $($x:tt)*) =>
    // Element
    {
        $element::create(inner_query_pipe!(;$INDEX; $($x)*))
    };
    () => {}
}

#[macro_export]
macro_rules! query_pipeline {
    ($($x:tt)*) => {
        Box::new(move |index, mut query| {
            use $crate::language::CanApply;
            use $crate::query::{ToOperands, Weight, Funnel, Operand, ChainingOperator};
            use perlin_core::utils::seeking_iterator::PeekableSeekable;

            // Build the pipeline
            let mut pipeline = inner_query_pipe!(;index; $($x)*);
            // Run the query-string through it
            pipeline.apply(&query.query);
            // And retrieve all operands
            pipeline.to_operands()
        })
    }
}
