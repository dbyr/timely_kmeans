extern crate timely_kmeans;

use timely_kmeans::point::Point;
// use traditional::{SelectRandom, ClosestNeighbour, SumDistances, UpdateCategories};
// use sampler::SampleData;
use timely::dataflow::operators::{Input, Inspect, Probe};
// use timely::dataflow::operators::capture::replay::Replay;
use timely::dataflow::{InputHandle, ProbeHandle};
use std::f64;
use timely_kmeans::traditional::{KMeansPPInitialise, LloydsIteration, ScalableInitialise};
// use crate::euclidean_distance::EuclideanDistance;
use std::io::{BufReader, BufRead, LineWriter, Write};
use std::fs::File;
use std::sync::mpsc::channel;
use timely_kmeans::euclidean_distance::EuclideanDistance;
use timely_kmeans::common::Attributable;

fn gen_file_names(prefix: &String, size: usize) -> Vec<String> {
    let mut paths = vec!();
    for i in 0..size {
        paths.push(format!(
            "{}{}",
            *prefix,
            i
        ));
    }
    paths
}

fn get_my_file(size: usize, index: usize) -> String {
    format!(
        "{}{}{}{}",
        "../rust_classifiers/data/mpi/",
        size,
        "procs/easy_clusters",
        index
    )
}

// takes a file of data to build the classifier and returns
// the generated categories or None if training failed
#[allow(dead_code)]
fn kmeans_scalable(_data_path: String, cats: usize) -> Option<Vec<Point>> {
    let cat_receivers =
        timely::execute_from_args(std::env::args(), move |worker| {
            let mut input = InputHandle::new();
            let mut initialise_probe = ProbeHandle::new();
            let mut lloyd_probe = ProbeHandle::new();
            let index = worker.index();
            // let data = File::open(data_path.clone()).unwrap();
            let (sender, receiver) = channel();

            // only for use with local testing
            // let mut paths = gen_file_names(&data_path, worker.peers());


            // select the initial random point
            // let cats =
            worker.dataflow(|scope| {
                let data = scope.input_from(&mut input);
                let cats = data
                    .scalable_initialise(cats, index)
                    .probe_with(&mut initialise_probe);
                data.lloyds_iteration(&cats, 100)
                    .inspect_batch(move |_t, data| {
                        for v in data {
                            for item in v {
                                match sender.send(*item) {
                                    // get the compiler off my back
                                    _ => ()
                                }
                            }
                        }
                    })
                    .probe_with(&mut lloyd_probe);
            });

            // send the point data from the file
            // let data = File::open(paths.remove(index)).unwrap();
            let data = File::open(get_my_file(worker.peers(), index)).unwrap();
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
            while initialise_probe.less_than(input.time()) {
                worker.step();
            }
            while lloyd_probe.less_than(input.time()) {
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

// takes a file of data to build the classifier and returns
// the generated categories or None if training failed
#[allow(dead_code)]
fn kmeans_pp(_data_path: String, cats: usize) -> Option<Vec<Point>> {
    let cat_receivers =
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputHandle::new();
        let mut lloyd_probe = ProbeHandle::new();
        let mut init_probe = ProbeHandle::new();
        let index = worker.index();
        // let data = File::open(data_path.clone()).unwrap();
        let (sender, receiver) = channel();

        // only for use with local testing
        // let mut paths = gen_file_names(&data_path, worker.peers());


        // select the initial random point
        worker.dataflow(|scope| {
            let data = scope.input_from(&mut input);
            let cats = data
                .kmeans_pp_initialise(cats, index)
                .probe_with(&mut init_probe);
            data.lloyds_iteration(&cats, 100)
                .inspect_batch(move |_t, data| {
                    for v in data {
                        for item in v {
                            match sender.send(*item) {
                                // get the compiler off my back
                                _ => ()
                            }
                        }
                    }
                })
                .probe_with(&mut lloyd_probe);
        });

        // send the point data from the file
        // let data = File::open(paths.remove(index)).unwrap();
        let data = File::open(get_my_file(worker.peers(), index)).unwrap();
        let reader = BufReader::new(data);
        // let reader = BufReader::new(data);
        let mut lines = reader.lines();
        while let Some(Ok(line)) = lines.next() {
            match Point::point_from_vec(&line.into_bytes()) {
                Ok(v) => input.send(v),
                Err(_) => continue
            }
        }
        input.close();

        // advance the dataflow
        while !init_probe.done() {
            worker.step();
        }
        while !lloyd_probe.done() {
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
    // let data = "../rust_classifiers/data/mpi/1procs/easy_clusters".to_string();
    let data = "../rust_classifiers/data/mpi/2procs/easy_clusters".to_string();
    // let cats = match kmeans_pp(data, 15) {
    let cats = match kmeans_scalable(data, 15) {
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

