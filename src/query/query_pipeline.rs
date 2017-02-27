#[macro_export]
macro_rules! operand {
    (;$INDEX:ident; $chaining_op:ident [$operator:ident in $this_field:ident]) => {
        Funnel::create(ChainingOperator::$chaining_op, Operator::$operator, &$INDEX.$this_field)
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
        Funnel::create(ChainingOperator::$chain, Operator::$operator, &$INDEX.$this_field)
    };
    (;$INDEX:ident;
     $chain:ident [$operator:ident in $this_field:ident] $($x:tt)*) => {
        SplitFunnel::create(
            ChainingOperator::$chain, Operator::$operator, &$INDEX.$this_field,
            inner_query_pipe!(;$INDEX; $($x)*))
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
        Box::new(move |index, query| {
            use $crate::language::CanApply;
            use $crate::query::{And, Or, ToOperands, SplitFunnel, Funnel, Operator, ChainingOperator};

            let mut pipeline = inner_query_pipe!(;index; $($x)*);
            pipeline.apply(query);
            let operands = pipeline.to_operands();
            let mut must_bucket = Vec::new();
            let mut may_bucket = Vec::new();
            for (op, operand) in operands {
                match op {
                    ChainingOperator::Must => {
                        must_bucket.push(operand);
                    },
                    ChainingOperator::May => {
                        may_bucket.push(operand);
                    },
                    _ => {panic!()}
                }
            };
         //   println!("Mays: {:?}", may_bucket.len());
            if !may_bucket.is_empty() {
                must_bucket.push(Box::new(Or::create(may_bucket)));
            }
         //   println!("Musts: {:?}", must_bucket.len());
            Box::new(And::create(must_bucket))
        })
    }
}
