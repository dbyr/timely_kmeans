use rand::Rng;
use std::str::from_utf8;
use f64;

use crate::euclidean_distance::EuclideanDistance;
use crate::random::Random;
use crate
::common::{
    TrainingError,
    Attributable
};
use std::fmt::Result as FmtResult;
use std::fmt::{
    Debug,
    Formatter
};

// abomonation required to use datatype with timely
#[derive(PartialEq, Default, Clone, Abomonation)]
pub struct Point {
    x: f64,
    y: f64
}

impl Point{
    pub fn new(x: f64, y: f64) -> Point {
        Point{x: x, y: y}
    }
    pub fn get_x(&self) -> f64 {
        return self.x;
    }
    pub fn get_y(&self) -> f64 {
        return self.y;
    }


    pub fn point_from_vec(buff: &Vec<u8>) -> Result<Point, TrainingError> {
        let mut rep_parts = from_utf8(&buff[..]).unwrap().split_ascii_whitespace();
        let x = match rep_parts.next() {
            Some(v) => {
                match v.parse::<f64>() {
                    Ok(x_val) => x_val,
                    Err(_) => {
                        return Err(TrainingError::InvalidData);
                    }
                }
            },
            None => {
                return Err(TrainingError::InvalidData);
            }
        };
        let y = match rep_parts.next() {
            Some(v) => {
                match v.parse::<f64>() {
                    Ok(y_val) => y_val,
                    Err(_) => {
                        return Err(TrainingError::InvalidData);
                    }
                }
            },
            None => {
                return Err(TrainingError::InvalidData);
            }
        };
        return Ok(Point::new(x, y));
    }
}

impl Debug for Point {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        // Write strictly the first element into the supplied output
        // stream: `f`. Returns `fmt::Result` which indicates whether the
        // operation succeeded or failed. Note that `write!` uses syntax which
        // is very similar to `println!`.
        write!(f, "({:.2},{:.2})", self.get_x(), self.get_y())
    }
}

impl EuclideanDistance for Point {
    fn distance(&self, rhs: &Point) -> f64 {
        ((self.x - rhs.x).powi(2) + (self.y - rhs.y).powi(2)).sqrt()
    }

    fn add(&self, other: &Point) -> Point {
        Point{x: self.x + other.x, y: self.y + other.y}
    }

    fn sub(&self, other: &Point) -> Point {
        Point{x: self.x - other.x, y: self.y - other.y}
    }

    fn scalar_div(&self, scalar: f64) -> Point {
        Point{x: self.x / scalar, y: self.y / scalar}
    }

    fn origin() -> Point {
        Self::default()
    }
}

impl Attributable for Point {
    fn attribute_names() -> String {
        return "x,y".to_string();
    }
    fn attribute_values(&self) -> String {
        return format!("{},{}", self.x, self.y).to_string();
    }
}

impl Random for Point {
    fn random() -> Point {
        let mut rng = rand::thread_rng();
        Point{
            x: rng.gen(), 
            y: rng.gen()
        }
    }

    // provide a box within which to generate points
    fn random_in_range(lower: &Point, upper: &Point) -> Point {
        let mut rng = rand::thread_rng();
        Point{
            x: rng.gen_range(lower.x, upper.x), 
            y: rng.gen_range(lower.y, upper.y)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::euclidean_distance::EuclideanDistance;
    use super::Point;
    use crate::common::TrainingError;

    #[test]
    fn test_point_from_vec() {
        // test integers
        let expected = Point::new(1.0, 2.0);
        let actual = Point::point_from_vec(&"1 2".to_owned().into_bytes());
        assert_eq!(Ok(expected), actual, "Point::point_from_vec failed: test integers");
    
        // test floats
        let expected = Point::new(1.3, 2.75);
        let actual = Point::point_from_vec(&"1.3 2.75".to_owned().into_bytes());
        assert_eq!(Ok(expected), actual, "Point::point_from_vec failed: test floats");
    
        // test negatives
        let expected = Point::new(-1.0, 2.0);
        let actual = Point::point_from_vec(&"-1 2".to_owned().into_bytes());
        assert_eq!(Ok(expected), actual, "Point::point_from_vec failed: test negatives");
    
        // test failures
        let expected = TrainingError::InvalidData;
        let actual = Point::point_from_vec(&"abc".to_owned().into_bytes());
        assert_eq!(Err(expected), actual, "Point::point_from_vec failed: test failures");
    }

    #[test]
    fn test_point_euclidean_ops() {
        let point1 = Point::new(1.0, 2.0);
        let point2 = Point::new(4.0, 6.0);
        let point3 = Point::new(2.34, 5.11);

        // test distance
        let dist = point1.distance(&point2);
        assert!(dist < 5.000001);
        assert!(dist > 4.999999);
        let dist = point3.distance(&point3);
        assert_eq!(dist, 0.0);

        // test add
        let added = point1.add(&point2);
        assert_eq!(Point::new(5.0, 8.0), added);

        // test scalar division
        let divd = point2.scalar_div(2.0);
        assert_eq!(Point::new(2.0, 3.0), divd);
    }
}