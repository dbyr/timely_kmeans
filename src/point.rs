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
#[derive(PartialEq, Default, Copy, Clone, Abomonation)]
pub struct Point {
    x: f64,
    y: f64
}

#[allow(dead_code)]
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