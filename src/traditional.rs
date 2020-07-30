use crate::point::Point;
use timely::dataflow::operators::{Operator, Broadcast, Map, Concat};
use timely::dataflow::*;
use timely::dataflow::operators::Exchange;
// use rand::{Rng, thread_rng};
use std::f64;
use timely::dataflow::channels::pact::Pipeline;
use timely::Data;
use std::collections::HashMap;
use std::borrow::ToOwned;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

trait ClosestNeighbour<G: Scope, D1: Data, D2: Data> {
    fn closest_neighbour(&self, sampled: &Stream<G, D1>) -> Stream<G, D2>;
}

impl<G: Scope> ClosestNeighbour<G, Point, (f64, Point)> for Stream<G, (f64, Point)> {
    fn closest_neighbour(&self, sampled: &Stream<G, Point>)
                         -> Stream<G, (f64, Point)> {
        self.binary_frontier(
            sampled,
            Pipeline,
            Pipeline,
            "Find closest neighbours",
            |_, _| {
                let sampled: HashMap<u32, Vec<Point>> = HashMap::new();
                move |data, samples, output| {

                }
            }
        )
    }
}

trait SelectSamples<G: Scope, D1: Data, D2: Data> {
    fn sampled_data(&self, total_weight: f64) -> (Stream<G, D1>, Stream<G, D2>);
}

pub trait SelectRandom<G: Scope, D1: Data, D2: Data> {
    fn select_random(&self, id: usize) -> (Stream<G, D1>, Stream<G, D2>);
}
trait SelectLocalRandom<G: Scope, D1: Data, D2: Data> {
    fn select_local_random(&self) -> (Stream<G, D1>, Stream<G, D2>);
}

impl<G: Scope, D: Data> SelectLocalRandom<G, D, D> for Stream<G, D> {
    fn select_local_random(&self) -> (Stream<G, D>, Stream<G, D>) {
        // adapted from "partition" operator
        let mut builder = OperatorBuilder::new("Selected local random".to_owned(), self.scope());

        let mut input = builder.new_input(self, Pipeline);
        let (mut data_output, data_stream) = builder.new_output();
        let (mut selected_output, selected_stream) = builder.new_output();

        builder.build(move |_| {
            let mut vector = Vec::new();
            let mut firsts = HashMap::new();
            move |_frontiers| {
                let mut data_handle = data_output.activate();
                let mut selected_handle = selected_output.activate();
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    let first = firsts.entry(time.time().clone()).or_insert(true);
                    let mut data_session = data_handle.session(&time);
                    let mut selected_session = selected_handle.session(&time);
                    for datum in vector.drain(..) {
                        // this kind of sucks... but it's the best
                        // possible as far as I can tell with tdf
                        if *first {
                            *first = false;
                            selected_session.give(datum);
                        } else {
                            data_session.give(datum);
                        }
                    }
                });
            }
        });
        (selected_stream, data_stream)
    }
}


impl<G: Scope> SelectRandom<G, Point, Point> for Stream<G, Point> {
    fn select_random(&self, id: usize) -> (Stream<G, Point>, Stream<G, Point>) {
        // self.select_local_random()
        let (local_selected, data) = self.select_local_random();
        let (global_selected, send_backs) = local_selected
            .map(move |d| (id, d))
            .exchange(|_| 0u64)
            .select_local_random();

        let selected = global_selected
            .map(|d| d.1)
            .broadcast();
        let remaining = data
            .concat(
                &send_backs
                    .exchange(|d| d.0 as u64)
                    .map(|d| d.1)
            );

        (selected, remaining)
    }
}