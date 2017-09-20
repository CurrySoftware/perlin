#[macro_export]
macro_rules! operand {
    (;$INDEX:ident; [$operator:ident in $this_field:ident]) => {
        Funnel::create(&$INDEX.$this_field)
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
     | [$operator:ident in $this_field:ident] $($x:tt)*) =>
    // Element(params) | [OP in field]
    {
        $element::create($($param),+ ,
                         operand!(;$INDEX; [$operator in $this_field]),
                         inner_query_pipe!(;$INDEX; $($x)*))
    };
    (;$INDEX:ident;
     $element:ident($($param:expr),+) $($x:tt)*) =>
    // Element(params)
    {
        $element::create($($param),+ , inner_query_pipe!(;$INDEX; $($x)*))
    };
    (;$INDEX:ident;
     $element:ident
     | [$operator:ident in $this_field:ident] $($x:tt)*) =>
    // Element | [OP in field]
    {
        $element::create(
            operand!(;$INDEX; [$operator in $this_field]),
            inner_query_pipe!(;$INDEX; $($x)*))
    };
    (;$INDEX:ident;
     [$operator:ident in $this_field:ident]) => {
        // [All in field]
        Funnel::create(&$INDEX.$this_field)
    };
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
            use $crate::query::{ToOperands, Weight, Funnel, Operand};
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
