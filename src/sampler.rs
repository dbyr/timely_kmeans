// the idea here is to build the kmeans model from sampled
// data only - that is, sampled 5-12% of the total data
// and use that in such a way that creating the model would
// be much more suited to tdf

use crate::point::Point;
use timely::dataflow::*;
use rand::{thread_rng, Rng};
use timely::dataflow::operators::filter::Filter;

pub trait SampleData {
    fn sample_data(&self, rate: u8) -> Self;
}

impl<G: Scope> SampleData for Stream<G, Point> {
    fn sample_data(&self, rate: u8) -> Stream<G, Point> {
        let mut gen = thread_rng();
        self.filter(
            move |x| gen.gen_range(0, 100) < rate
        )
    }
}