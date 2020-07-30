#[macro_use]
extern crate abomonation_derive;

mod point;
mod random;
mod euclidean_distance;
mod common;
mod traditional;
mod sampler;

use point::Point;
use traditional::SelectRandom;
use sampler::SampleData;
use timely::dataflow::operators::{Operator, Broadcast, Capability, Capture, Branch, Partition, Map, Concat, Inspect, Probe};
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

        // traditional
        let _sampled = worker.dataflow(|scope| {
            let (sampled, data) = scope.input_from(&mut input)
                .select_random(index);
            sampled.inspect(|x| println!("sampled: {:?}", x)).probe_with(&mut probe);
            data.inspect(|x| println!("data: {:?}", x)).probe_with(&mut probe);
        });

        // let _sampled = worker.dataflow(|scope| {
        //     let sampled = scope.input_from(&mut input)
        //         .sample_data(50)
        //         .inspect(|x| println!("sampled {:?}", x))
        //         .probe_with(&mut probe);
        // });

        for i in 0..10 {
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

