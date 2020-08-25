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
use traditional::{SelectRandom, ClosestNeighbour, SumDistances};
// use sampler::SampleData;
use timely::dataflow::operators::{Input, Inspect, Probe, Map};
use timely::dataflow::{InputHandle, ProbeHandle};
use std::f64;
use crate::traditional::SelectSamples;

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
        // let mut initial_probe = ProbeHandle::new();
        // let mut distance_probe = ProbeHandle::new();
        let mut sum_probe = ProbeHandle::new();
        let index = worker.index();

        // select the initial random point
        worker.dataflow(|scope| {
            let (sampled, data) =
                scope.input_from(&mut input).select_random(index.clone());
            sampled
                .inspect_batch(move |t, x|
                    x.iter().for_each(move |v| println!("worker {} sampled: {:?} w/ t={:?}", index.clone(), v, t))
                );
                // .probe_with(&mut initial_probe);
            // data
            //     .inspect_batch(move |t, x|
            //         x.iter().for_each(move |v| println!("worker {} data: {:?} w/ t={:?}", index.clone(), v, t))
            //     );

            // calculate the distance for the randomly selected point
            let initial_distanced = data
                .map(|p| (f64::MAX, p))
                .closest_neighbour(&sampled)
                .inspect_batch(move |t, v|
                    v.iter().for_each(|x|println!("{:?} is {} away at time {:?}", x.1, x.0, t))
                );
                // .probe_with(&mut distance_probe);

            let (summed, piped) = initial_distanced.sum_square_distances();
            summed
                .inspect_batch(move |t, v|
                    v.iter().for_each(|x| println!("sum to {} at {:?}", x, t))
                )
                .probe_with(&mut sum_probe);

            let (sampled, data) =
                piped.sample_data(&summed.map(|v| (v, 3usize)));

            sampled.inspect_batch(move |t, v|
                v.iter().for_each(|x|println!("sampled {:?} at time {:?}", x.1, t))
            );
            data.inspect_batch(move |t, v|
                v.iter().for_each(|x|println!("passed {:?} at time {:?}", x.1, t))
            );
        });

        for i in 0..1 {
        //     let i = 0;
            println!("worker {} sending round {}", index, i);
            for j in 0..10 {
                input.send(
                    Point::new((i + j) as f64+5.0, index as f64)
                );
            }
            input.advance_to(input.epoch() + 1);
            // while initial_probe.less_than(input.time()) {
            //     std::thread::sleep(std::time::Duration::from_millis(500));
            //     worker.step();
            // }
            // while distance_probe.less_than(input.time()) {
            //     std::thread::sleep(std::time::Duration::from_millis(500));
            //     worker.step();
            // }
            while sum_probe.less_than(input.time()) {
                std::thread::sleep(std::time::Duration::from_millis(500));
                worker.step();
            }
        }
    }).unwrap();
}

