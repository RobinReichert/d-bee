
pub fn add(a : u64, b : u64) -> u64{
    a+b
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn t(){
        assert_eq!(add(5, 4), 9);
    }

}
