pub trait Random {
    // produces a random value of this type
    // return: a random value
    fn random() -> Self;

    // produces a random value in the range specified
    // lower: the inclusive lower bound for the random value
    // upper: the exclusive upper bound for the random value
    // return: a random value between the specified range
    fn random_in_range(lower: &Self, upper: &Self) -> Self;
}