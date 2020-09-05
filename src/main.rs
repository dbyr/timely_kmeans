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
use std::io::{BufReader, BufRead, LineWriter, Write};
use std::fs::File;
use std::sync::mpsc::channel;
use crate::euclidean_distance::EuclideanDistance;
use crate::common::Attributable;

fn gen_file_names(size: usize) -> Vec<String> {
    let mut paths = vec!();
    for i in 0..size {
        paths.push(format!(
            "../rust_classifiers/data/mpi/2procs/easy_clusters{}",
            i
        ));
    }
    paths
}

// takes a file of data to build the classifier and returns
// the generated categories or None if training failed
fn kmeans_pp(data_path: String, cats: usize) -> Option<Vec<Point>> {
    let cat_receivers =
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputHandle::new();
        let sum_probe = ProbeHandle::new();
        let index = worker.index();
        // let data = File::open(data_path.clone()).unwrap();
        let (sender, receiver) = channel();

        // only for use with local testing
        let mut paths = gen_file_names(worker.peers());


        // select the initial random point
        // let cats =
        worker.dataflow(|scope| {
            let (data, cats) = scope
                .input_from(&mut input)
                .kmeans_pp_initialise(cats, index);
            // cats.inspect_batch(|t, data| {
            //     data.iter().for_each(|v| println!("time {:?} initial cats: {:?}", t, v))
            // });
            data.lloyds_iteration(&cats, 100)
                .inspect_batch(move |_t, data| {
                    for v in data {
                        // println!("time {:?} final cats: {:?}", t, v);
                        for item in v {
                            match sender.send(*item) {
                                // get the compiler off my back
                                _ => ()
                            }
                        }
                    }
                });
        });

        // send the point data from the file
        let data = File::open(paths.remove(index)).unwrap();
        let reader = BufReader::new(data);
        // let reader = BufReader::new(data);
        let mut lines = reader.lines();
        while let Some(Ok(line)) = lines.next() {
            match Point::point_from_vec(&line.into_bytes()) {
                Ok(v) => input.send(v),
                Err(_) => continue
            }
        }

        // advance the dataflow
        input.advance_to(input.epoch() + 1);
        while sum_probe.less_than(input.time()) {
            worker.step();
        }
        return receiver;
    }).unwrap();

    // collect a vector of categories from the channel and return it
    let mut events = cat_receivers.join();
    let receiver = events.remove(0);
    match receiver {
        Ok(receiver) => Some(receiver.iter().collect::<Vec<Point>>()),
        Err(_) => None
    }
}

fn point_vec_from_file(file: &File) -> Result<Box<Vec<Point>>, ()> {
    let mut data = Box::new(Vec::new());
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let buf = match line {
            Ok(l) => l.into_bytes(),
            Err(_) => return Err(())
        };
        let val = match Point::point_from_vec(&buf) {
            Ok(v) => v,
            Err(_) => return Err(())
        };
        data.push(val);
    }
    return Ok(data);
}

fn classify<T>(cats: &Vec<T>, datum: &T) -> usize
where T: EuclideanDistance {
    let mut cat = 0;
    let mut dist = f64::MAX;
    for (i, c) in cats.iter().enumerate() {
        let new_dist = c.distance(datum);
        if new_dist < dist {
            cat = i;
            dist = new_dist;
        }
    }
    cat
}

fn classify_csv<T>(
    classifier: &Vec<T>,
    file: &File,
    data: &Vec<T>
) -> Result<(), ()>
    where T: EuclideanDistance + Attributable {
    let mut writer = LineWriter::new(file);
    match writer.write_all(T::attribute_names().as_bytes()) {
        Ok(v) => v,
        Err(_) => return Err(())
    }
    match writer.write_all(b",cat\n") {
        Ok(v) => v,
        Err(_) => return Err(())
    }

    for val in data {
        match writer.write_all(val.attribute_values().as_bytes()) {
            Ok(v) => v,
            Err(_) => return Err(())
        }
        let cat = classify(classifier,val);

        match writer.write_all(format!(",{}\n", cat).as_bytes()) {
            Ok(v) => v,
            Err(_) => return Err(())
        }
    }
    match writer.flush() {
        Ok(v) => v,
        Err(_) => return Err(())
    }
    return Ok(());
}

fn main() {
    let input_file = "../rust_classifiers/data/easy_clusters_rand".to_string();
    let output_file = "../rust_classifiers/data/timely_output.csv".to_string();

    // run kmeans
    let data = "../rust_classifiers/data/easy_clusters_rand".to_string();
    let cats = match kmeans_pp(data, 15) {
        Some(v) => v,
        None => {
            println!("Kmeans failed");
            return;
        }
    };
    println!("final cats: {:?}", cats);


    // do the optional classification of the whole file
    let f_out = match File::create(output_file) {
        Ok(f) => f,
        Err(_) => {
            println!("Couldn't create clustered data file");
            return;
        },
    };

    let f = match File::open(input_file) {
        Ok(f) => f,
        Err(_) => {
            println!("Couldn't open file containing data");
            return;
        },
    };
    let data = match point_vec_from_file(&f) {
        Ok(v) => v,
        Err(_) => {
            println!("Couldn't read data from file");
            return;
        }
    };
    match classify_csv(&cats, &f_out, &data) {
        Ok(_) => return,
        Err(_) => {
            println!("Could not write csv file");
            return;
        },
    }
}

