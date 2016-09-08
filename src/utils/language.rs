//! This module will provide the necessary utilities to handle language well in
//! regard of Information Retrieval.
//!
//! It will contain methods for tokenization, stemming, normalization and so on.
//!
//! At the moment though, it only provides a very basic analyzer method.

/// Analyzes a string and returns a vector of terms.
/// Tokenizes at non-alphanumerical characters and turns the tokens to
/// lowercase.
///
/// # Example
/// "hans. .PETER!" will be transformed into ["hans", "peter"]
pub fn basic_analyzer(input: &str) -> Vec<String> {
    input.split(|c: char| !c.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(|term| term.to_lowercase())
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use super::basic_analyzer;

    #[test]
    fn lowercase() {
        assert_eq!(basic_analyzer("HANS"), vec!["hans"]);
    }

    #[test]
    fn tokenization() {
        assert_eq!(basic_analyzer("one small step for man"),
                   vec!["one", "small", "step", "for", "man"]);
    }

    #[test]
    fn non_alphanum() {
        assert_eq!(basic_analyzer("!I!wouldn't: mind :SOme? ?BoiLed%Eggs<()>pLEAse"),
                   vec!["i", "wouldn", "t", "mind", "some", "boiled", "eggs", "please"]);
    }

}
