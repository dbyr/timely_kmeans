#[macro_use]
extern crate abomonation_derive;

mod point;
mod random;
mod euclidean_distance;
#[allow(dead_code)]
mod common;
mod traditional;
mod sampler;

use point::Point;
use traditional::SelectRandom;
// use sampler::SampleData;
use timely::dataflow::operators::{Input, Inspect, Probe};
use timely::dataflow::{InputHandle, ProbeHandle};
use std::f64;

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
        let printable1 = index;
        let printable2 = index;

        // traditional
        worker.dataflow(|scope| {
            let (sampled, data) =
                scope.input_from(&mut input).select_random(index.clone());
            sampled
                .inspect_batch(move |t, x|
                    x.iter().for_each(|v| println!("worker {} sampled: {:?} w/ t={:?}", printable1, v, t))
                )
                .probe_with(&mut probe);
            data
                .inspect_batch(move |t, x|
                    x.iter().for_each(|v| println!("worker {} data: {:?} w/ t={:?}", printable2, v, t))
                )
                .probe_with(&mut probe);
        });

        for i in 0..3 {
            println!("worker {} sending round {}", index, i);
            for j in 0..10 {
                input.send(
                    Point::new((i + j) as f64+5.0, index as f64)
                );
            }
            input.advance_to(input.epoch() + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    }).unwrap();
}

