pub trait EuclideanDistance {
    // get the distance between this point and the other
    fn distance(&self, other: &Self) -> f64;

    // add the other value to this value
    fn add(&self, other: &Self) -> Self;

    // subtract another value from this value
    fn sub(&self, other: &Self) -> Self;

    // divide this value by a scalar
    fn scalar_div(&self, scalar: f64) -> Self;

    // get the origin for this euclidean space
    fn origin() -> Self;

}