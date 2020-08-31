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
// use traditional::{SelectRandom, ClosestNeighbour, SumDistances, UpdateCategories};
// use sampler::SampleData;
use timely::dataflow::operators::{Input, Inspect};
// use timely::dataflow::operators::capture::replay::Replay;
use timely::dataflow::{InputHandle, ProbeHandle};
use std::f64;
use crate::traditional::{KMeansPPInitialise, LloydsIteration};
// use crate::euclidean_distance::EuclideanDistance;

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
        let sum_probe = ProbeHandle::new();
        let index = worker.index();

        // select the initial random point
        worker.dataflow(|scope| {
            let (data, cats) = scope
                .input_from(&mut input)
                .kmeans_pp_initialise(2, index);
            cats.inspect_batch(|t, data| {
                data.iter().for_each(|v| println!("time {:?} cats: {:?}", t, v))
            });
            data.inspect_batch(|t, data| {
                data.iter().for_each(|v| println!("time {:?} data: {:?}", t, v))
            });
            let _final_cats = data.lloyds_iteration(&cats, 100)
                .inspect_batch(|t, data| {
                    data.iter().for_each(|v| println!("time {:?} final cats: {:?}", t, v))
                });

            // let (mut sampled, mut data) =
            //     scope.input_from(&mut input)
            //         .select_random(index.clone());
            // sampled = sampled
            //     .inspect_batch(move |t, x|
            //         x.iter().for_each(move |v| println!("worker {} sampled: {:?} w/ t={:?}", index.clone(), v, t))
            //     );
            // data = data
            //     .inspect_batch(move |t, x|
            //         x.iter().for_each(move |v| println!("worker {} passed on: {:?} w/ t={:?}", index.clone(), v, t))
            //     );
            //
            //     // .probe_with(&mut initial_probe);
            // // data
            // //     .inspect_batch(move |t, x|
            // //         x.iter().for_each(move |v| println!("worker {} data: {:?} w/ t={:?}", index.clone(), v, t))
            // //     );
            //
            // // calculate the distance for the randomly selected point
            // let (sampled, categories) = sampled.duplicate();
            // let initial_distanced = data
            //     .map(|p| (f64::MAX, p))
            //     .closest_neighbour(&sampled)
                // .inspect_batch(move |t, v|
                //     v.iter().for_each(|x|println!("{:?} is {} away at time {:?}", x.1, x.0, t))
                // )
            ;
            //
            // let (summed, piped) =
            //     initial_distanced.sum_square_distances();
            // piped
            //     .inspect_batch(move |t, v|
            //         v.iter().for_each(|x| println!("sum to {:?} at {:?}", x, t))
            //     )
                // .probe_with(&mut sum_probe)
            ;
            //
            // let (mut sampled, mut data) =
            //     piped.select_weighted_initial(&summed);
            // sampled = sampled
            //     .inspect_batch(move |t, v|
            //     v.iter().for_each(|x|println!("weighted sample {:?} at {:?}", x.1, t))
            // )
            // ;
            // data = data
            //     .inspect_batch(move |t, v|
            //         v.iter().for_each(|x|println!("passed on {:?} at {:?}", x.1, t))
            //     )
            // ;
            //     // piped.sample_data(&summed.map(|v| (v, 3usize)));
            //
            // let cats = sampled.map(|v| v.1)
            //     .concat(&categories)
            //     .inspect_batch(move |t, v|
            //     v.iter().for_each(|x|println!("sampled {:?} at time {:?}", x, t))
            // ).create_categories();
            // data = data.inspect_batch(move |t, v|
            //     v.iter().for_each(|x|println!("passed {:?} at time {:?}", x.1, t))
            // );
            //
            // let (reuse, mut new_cats) = data
            //     .update_categories(&cats);
            // new_cats = new_cats.inspect_batch(move |t, v|
            //     v.iter().for_each(|x|println!("cat {:?} at time {:?}", x, t))
            // );
        });

        for i in 0..1 {
        //     let i = 0;
            println!("worker {} sending round {}", index, i);
            if index == 0 {
                for j in 0..10 {
                    input.send(
                        Point::new((i + j) as f64+5.0, index as f64)
                    );
                }
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

