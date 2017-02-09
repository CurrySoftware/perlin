/// One element in the pipeline. Call it by calling apply
/// It will do one of three things with a token
/// 1. Consume it
/// 2. Change it and pass it on
/// 3. Do nothing and pass it on
pub trait PipelineElement {
    fn apply<'a>(&self, &str, callback: Box<FnMut(&str) + 'a>);
}

pub struct Pipeline {
    elements: Vec<Box<PipelineElement>>,
}

impl Pipeline {
    pub fn new(elements: Vec<Box<PipelineElement>>) -> Self {
        Pipeline { elements: elements }
    }

    pub fn push<TFn: FnMut(&str)>(&self, data: &str, sink: &mut TFn) {
        call_pipe(data, &self.elements, sink);
    }
}

fn call_pipe<TFn: FnMut(&str)>(data: &str, elements: &[Box<PipelineElement>], sink: &mut TFn) {
    if !elements.is_empty() {
        let cb = make_callback(&elements[1..], sink);
        elements[0].apply(data, cb);
    } else {
        sink(data)
    }
}

fn make_callback<'a, TFn: FnMut(&str) + 'a>(elements: &'a [Box<PipelineElement>],
                                            sink: &'a mut TFn)
                                            -> Box<FnMut(&str) + 'a> {
    Box::new(move |data: &str| call_pipe(data, elements, sink))
}


// macro_rules! pipeline {
//     ( $cur:expr, $t:ty > $($x:ty)>+) => {
//         pipeline!(pipeline!($cur, $($x)>+), $t)
//     };
//     ( $cur:expr, $t:ty) => {
//         $cur.append_to::<$t>()
//     };
//     ( $cur:expr ) => {
//         $cur;
//     }
// }
