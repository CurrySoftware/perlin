use language::CanApply;

pub struct StopwordFilter<CB> {
    stopwords: Vec<String>,
    callback: CB
}

impl<CB> StopwordFilter<CB> {
    pub fn create(stopwords: Vec<String>, callback: CB) -> Self{
        StopwordFilter{
            stopwords: stopwords,
            callback: callback
        }
    }
}


impl<CB: CanApply<String>> CanApply<String> for StopwordFilter<CB> {
    type Output = CB::Output;
    
    fn apply(&mut self, input: String) {
        if let Err(_) = self.stopwords.binary_search(&input) {
            self.callback.apply(input);
        }
    }
}
