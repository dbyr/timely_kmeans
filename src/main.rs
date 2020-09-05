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
use timely::dataflow::operators::{Input, Inspect, Capture, Map};
// use timely::dataflow::operators::capture::replay::Replay;
use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::capture::Extract;
// use std::f64;
use crate::traditional::{KMeansPPInitialise, LloydsIteration};
// use crate::euclidean_distance::EuclideanDistance;
use std::io::{BufReader, BufRead};
use std::fs::File;
use std::path::Path;
use std::sync::mpsc::{channel, Sender, Receiver};

// takes a file of data to build the classifier and returns
// the generated categories or None if training failed
fn kmeans_pp(data_path: String, cats: usize) {//-> Option<Vec<Point>> {
    // let guard =
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputHandle::new();
        // let mut initial_probe = ProbeHandle::new();
        // let mut distance_probe = ProbeHandle::new();
        let sum_probe = ProbeHandle::new();
        let index = worker.index();
        let data = File::open(data_path.clone()).unwrap();

        // select the initial random point
        // let cats =
            worker.dataflow(|scope| {
            let (data, cats) = scope
                .input_from(&mut input)
                .kmeans_pp_initialise(cats, index);
            cats.inspect_batch(|t, data| {
                data.iter().for_each(|v| println!("time {:?} initial cats: {:?}", t, v))
            });
            // data.inspect_batch(|t, data| {
            //     data.iter().for_each(|v| println!("time {:?} data: {:?}", t, v))
            // });
            data.lloyds_iteration(&cats, 100)
                .inspect_batch(|t, data| {
                    data.iter().for_each(|v| {
                        println!("time {:?} final cats: {:?}", t, v);
                        // for item in v {
                        //     sender.send(*item);
                        // }
                    })
                });
        });

        // send the point data from the file
        let reader = BufReader::new(data);
        let mut lines = reader.lines();
        while let Some(Ok(line)) = lines.next() {
            match Point::point_from_vec(&line.into_bytes()) {
                Ok(v) => input.send(v),
                Err(_) => continue
            }
        }

        input.advance_to(input.epoch() + 1);
        while sum_probe.less_than(input.time()) {
            worker.step();
        }

        // cats.flat_map(|vec| vec.into_iter())
        //     .inspect_batch(|t, d|
        //     d.iter().for_each(|v| println!("cats: {:?} at {:?}", v, t)))
        // ;
            // .capture()
    }).unwrap();
    // let out = vec!();
    // let events = guard.join()
    // while let Some(event) =

    // let out = guard.join().remove(0).unwrap().extract();
    // println!("extracted: {:?}", out);

    // let cats_opt = guard.join().remove(0);
    // match cats_opt {
    //     Ok(res) => match res.recv() {
    //         Ok(v) => match v {
    //
    //         }
    //             Some(v.1),
    //         Err(e) => None
    //     },
    //     Err(e) => None
    // }
    // Some(Vec::new())
}

fn main() {
    // let (sender, receiver) = channel();
    let data = "../rust_classifiers/data/easy_clusters_rand".to_string();
    println!("final cats: {:?}", kmeans_pp(data, 15));
}

