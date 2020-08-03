use crate::point::Point;
use timely::dataflow::operators::{Operator, Broadcast, Map, Concat, Capability, FrontierNotificator};
use timely::dataflow::*;
use timely::dataflow::operators::Exchange;
use rand::{Rng, thread_rng};
use std::f64;
use timely::dataflow::channels::pact::Pipeline;
use timely::{Data, PartialOrder};
use std::collections::HashMap;
use std::borrow::ToOwned;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::progress::Timestamp;

trait ClosestNeighbour<G: Scope, D1: Data, D2: Data> {
    fn closest_neighbour(&self, sampled: &Stream<G, D1>) -> Stream<G, D2>;
}

trait SelectSamples<G: Scope, D1: Data, D2: Data> {
    fn sampled_data(&self, total_weight: f64) -> (Stream<G, D1>, Stream<G, D2>);
}

pub trait SelectRandom<G: Scope, D1: Data, D2: Data> {
    fn select_random(&self, id: usize) -> (Stream<G, D1>, Stream<G, D2>);
}
pub trait SelectLocalRandom<G: Scope, D1: Data, D2: Data> {
    fn select_local_random(&self) -> (Stream<G, D1>, Stream<G, D2>);
}

impl<G: Scope> ClosestNeighbour<G, Point, (f64, Point)> for Stream<G, (f64, Point)> {
    fn closest_neighbour(&self, sampled: &Stream<G, Point>)
                         -> Stream<G, (f64, Point)> {
        self.binary(
            sampled,
            Pipeline,
            Pipeline,
            "Find closest neighbours",
            |_, _| {
                // let mut stash = HashMap::new();
                move |data, samples,  output| {
                    // insert the sampled data for this iteration
                    // while let Some((time, data)) = samples.next() {
                    //     let sampled = stash.entry(time.time().clone()).or_insert(Vec::new());
                    //     for datum in data.into_iter() {
                    //         sampled.push(datum);
                    //     }
                    // }

                    // compare all the other data with the sampled to re-calcaulte their nearest neighbour

                }
            }
        )
    }
}

fn smallest_time<'r, 'a, T: 'a + Ord + PartialOrder + Clone>(
    chain: &'r AntichainRef<'a, T>,
    larger_than: &T
) -> Option<&'r T> {
    let mut smallest = None;
    chain.iter().for_each(|v| {
        if v.less_equal(larger_than) {
            println!("Too small");
        } // don't change anything
        else if smallest.is_none() {
            println!("Smallest is none");
            smallest = Some(v);
        } else if v.less_than(smallest.as_ref().unwrap()) {
            println!("Smaller");
            smallest = Some(v);
        }
    });
    smallest
}

// currently this method is implemented in a trivial and not-actually-random
// manner, I am still unsure how to implement true random selection
// using tdf (it's easy with a normal stream, but with the way the capabilities
// work in tdf it doesn't appear to be possible to do it that way)
impl<G: Scope, D: Data> SelectLocalRandom<G, D, D> for Stream<G, D> {
    fn select_local_random(&self) -> (Stream<G, D>, Stream<G, D>) {
        let mut builder = OperatorBuilder::new("Selected local random".to_owned(), self.scope());
        // builder.set_notify(true);
        let mut gen = thread_rng();

        // set up the input and output points for this stream
        let mut input = builder.new_input(self, Pipeline);
        let (mut data_output, data_stream) = builder.new_output();
        let (mut selected_output, selected_stream) = builder.new_output();

        // builds the operator
        builder.build(move |mut caps| {

            let mut vector = Vec::new();
            let mut firsts = HashMap::new();
            let mut selected_cap = caps.pop();
            let mut data_cap = caps.pop();

            move |frontiers| {
                if selected_cap.is_none() { // is this even possible?
                    return;
                }
                let mut data_handle = data_output.activate();
                let mut selected_handle = selected_output.activate();
                input.for_each(|time, data| {
                    let mut data_session = data_handle.session(data_cap.as_ref().unwrap());
                    data.swap(&mut vector);
                    let mut first = firsts.entry(time.time().clone()).or_insert(
                        (1f64, None)
                    );

                    // loop through all data individually, maintain a single value
                    // to be selected once all data has passed
                    for datum in vector.drain(..) {
                        let select = gen.gen_range(0f64, f64::MAX);
                        let prob = f64::MAX / first.0;
                        if prob >= select {
                            let mut to_send = None;
                            std::mem::swap(&mut to_send, &mut first.1);
                            if let Some(p) = to_send {
                                data_session.give(p);
                            }
                            first.1 = Some(datum);
                        } else {
                            data_session.give(datum);
                        }
                        first.0 += 1f64;
                    }
                });

                // now send the randomly selected value
                let mut frontier = frontiers[0].frontier(); // should only be one to match the input
                if !firsts.is_empty() {
                    for (time, first) in firsts.iter_mut() {
                        if !frontier.less_equal(time) {
                            let mut to_send = None;
                            std::mem::swap(&mut to_send, &mut first.1);
                            if let Some(s) = to_send {
                                let mut selected_session = selected_handle.session(selected_cap.as_ref().unwrap());
                                selected_session.give(s);
                            }

                            // attempt to either downgrade or drop the capabilities of outputs
                            let new_time = smallest_time(&frontier, time);
                            match new_time {
                                Some(t) => {
                                    println!("Downgraded");
                                    data_cap.as_mut().unwrap().downgrade(t);
                                    selected_cap.as_mut().unwrap().downgrade(t);
                                },
                                None => {
                                    println!("Dropped");
                                    data_cap = None;
                                    selected_cap = None;
                                }
                            }
                        }
                    }
                } else {

                    // if its empty and the timestamp has been surpassed, then
                    // this must be the last of the data
                    if let Some(t) = data_cap.as_ref() {
                        if !frontier.less_equal(t.time()) {
                            data_cap = None;
                        }
                    }
                    if let Some(t) = selected_cap.as_ref() {
                        if !frontier.less_equal(t.time()) {
                            selected_cap = None;
                        }
                    }
                }
                firsts.retain(|t, first| first.1.is_some());
            }
        });
        (selected_stream, data_stream)
    }
}


impl<G: Scope> SelectRandom<G, Point, Point> for Stream<G, Point> {
    fn select_random(&self, id: usize) -> (Stream<G, Point>, Stream<G, Point>) {
        // selects a random value locally & independently, then globally
        let
        let (local_selected, data) = self.select_local_random();
        let (global_selected, send_backs) = local_selected
            .map(move |d| (id, d))
            .exchange(|_| 0u64)
            .select_local_random();

        // sends the globally selected value to all procs
        let selected = global_selected
            .map(|d| d.1)
            .broadcast();

        // sends data not selected for global value back to the originating procs
        let remaining = data
            .concat(
                &send_backs
                    .exchange(|d| d.0 as u64)
                    .map(|d| d.1)
            );

        (selected, remaining)
    }
}