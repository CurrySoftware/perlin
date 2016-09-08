/// Mimics the functionality of the `try!` macro for `Option`s.
/// Evaluates `Some(x)` to x. Else it returns `None`.
#[macro_export]
macro_rules! try_option{
    ($operand:expr) => {
        if let Some(x) = $operand {
            x
        } else {
            return None;
        }
    }
}
