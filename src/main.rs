#[macro_use]
extern crate abomonation_derive;

mod point;
mod random;
mod euclidean_distance;
mod common;

use point::Point;
use timely::dataflow::operators::{Operator, Broadcast, Capability, Capture, Branch, Partition, Map, Concatenate};
use timely::dataflow::*;
use timely::dataflow::operators::{Input, Exchange};
use rand::{thread_rng, Rng};
use std::f64;
use timely::dataflow::channels::pact::{Pipeline, ParallelizationContract, Exchange as Exchanger};
use timely::dataflow::operators::generic::{OperatorInfo, FrontieredInputHandle, OutputHandle};
use timely::dataflow::channels::pushers::Tee;
use timely::Data;
use timely::progress::Timestamp;
use std::collections::HashMap;

/*
Potential solution:
- Get the initial point first (simply select one random from
each worker as below and send them to proc 0 which selects
a single global one and broadcasts)
- Each "round" (as described in scalable kmeans paper) stores the global
"selected" values in a 
 */

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();
        let index = worker.index();

        let _sampled = worker.dataflow(|scope| {
            let (sampled, data) = scope.input_from(&mut input)
                .select_random(index);


        });

        for i in 0..10 {
            println!("worker {} sending round {}", index, i);
            input.send(
                Point::new(i as f64+5.0, i as f64)
            );
            input.advance_to(input.epoch() + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    }).unwrap();
}

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

trait SelectRandom<G: Scope, D1: Data, D2: Data> {
    fn select_random(&self, id: usize) -> (Stream<G, D1>, Stream<G, D2>);
}

impl<G: Scope> SelectRandom<G, Point, Point> for Stream<G, Point> {
    fn select_random(&self, id: usize) -> (Stream<G, Point>, Stream<G, Point>) {
        let mut gen = thread_rng();
        let mut initial: Option<Point> = None;
        let all_to_0 = Exchanger::new(|x: &(usize, Point)| 0u64);
        let mut streams = self.unary(
                Pipeline, // p0 selects initial
                "Select initial locally",
                |_, _| {
                    let mut prob = f64::MAX;
                    let mut select: f64;
                    let mut counted = 1f64;
                    move |input, output| {
                        while let Some((time, datum)) = input.next() {
                            let mut session = output.session(&time);
                            let d = datum[0];
                            prob = f64::MAX / counted;
                            select = gen.gen_range(0.0, f64::MAX);
                            if select <= prob {
                                initial = Some(d);
                            }
                            session.give(d);
                            counted += 1f64;
                        }
                    }
                }
            ).partition(
                2,
                |d| if d == initial.unwrap() {(0, d)} else {(1, d)}
            );
        let s2 = streams.pop().unwrap();
        let s1 = streams.pop().unwrap().map(|p| (id, p));

        let mut streams = s1.unary(
                all_to_0,
                "Select global initial",
                // this is repeated code from above until i can figure out
                // how to implement the closure as an external method
                |_, _| {
                    let mut prob = f64::MAX;
                    let mut select: f64;
                    let mut counted = 1f64;
                    move |input, output| {
                        while let Some((time, datum)) = input.next() {
                            let mut session = output.session(&time);
                            let d = datum[0];
                            prob = f64::MAX / counted;
                            select = gen.gen_range(0.0, f64::MAX);
                            if select <= prob {
                                initial = Some(d.1);
                            }
                            session.give(d);
                            counted += 1f64;
                        }
                    }
                }
            ).partition(
                2,
                |d| if d.1 == initial.unwrap() {(0, d)} else {(1, d)}
            );
        let send_backs = streams.pop().unwrap()
            .exchange(|d| d.0 as u64)
            .map(|d| d.1);
        let glob_initial = streams.pop().unwrap()
            .map(|d| d.1);

        let ret1 = glob_initial.broadcast();
        let ret2 = s2.concatenate(send_backs);
        (ret1, ret2)
    }
}