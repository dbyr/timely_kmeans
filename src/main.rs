#[macro_use]
extern crate abomonation_derive;

mod point;
mod random;
mod euclidean_distance;
mod common;

use point::Point;
use timely::dataflow::operators::{Operator, Broadcast, Capability};
use timely::dataflow::*;
use timely::dataflow::operators::{Input, Exchange};
use timely::dataflow::channels::pact::Exchange as Pact;
use rand::{thread_rng, Rng};
use std::f64;
use timely::dataflow::channels::pact::{Pipeline, ParallelizationContract};
use timely::dataflow::operators::generic::{OperatorInfo, FrontieredInputHandle, OutputHandle};
use timely::dataflow::channels::pushers::Tee;
use timely::Data;
use timely::progress::Timestamp;

const DEPLETION_RATE: f64 = 2.0;

// fn randomly_sample_one_from_stream<G: Scope, D1: Data, D2: Data, L, P>(
//     _: Capability<G::Timestamp>,
//     _: OperatorInfo
// ) -> L
// where L: FnMut(
//         &mut FrontieredInputHandle<
//             G::Timestamp, D1,
//             P::Puller
//         >,
//         &mut OutputHandle<
//             G::Timestamp,
//             D2,
//             Tee<G::Timestamp, D2>
//         >
// ) + 'static, P: ParallelizationContract<G::Timestamp, D1> {
//     let mut prob = f64::MAX;
//     let mut select: f64;
//     let mut selected = None;
//     let mut counted = 1f64;
//     move |
//         input: &mut FrontieredInputHandle<G::Timestamp, D1, P::Puller>,
//         output: &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>
//         >
//     | {
//         while let Some((time, datum)) = input.next() {
//             prob = f64::MAX / counted;
//             select = gen.gen_range(0.0, f64::MAX);
//             if select <= prob {
//                 selected = Some((time, datum));
//             }
//             counted += 1f64;
//         }
//         if let Some((time, datum)) = selected {
//             let mut session = output.session(&time);
//             session.give(datum);
//         }
//     }
// }

// get initial point
//   split stream into initial point and rest
// get initial weights
//   save weight in a timestamp -> weight map
//

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();
        let index = worker.index();
        let mut gen = thread_rng();

        // let mut selections = Vec::new();
        let mut calculated = 0;

        let send_to_0 = Pact::new(|p: &Point| 0u64);

        worker.dataflow(|scope| {
            let extracted = scope.input_from(&mut input)
                .unary_frontier(
                    send_to_0, // p0 selects initial
                    "Select initial",
                    |_, _| {
                        let mut prob = f64::MAX;
                        let mut select: f64;
                        let mut counted = 1f64;
                        move |input, output| {
                            let mut selected = None;
                            while let Some((time, datum)) = input.next() {
                                prob = f64::MAX / counted;
                                select = gen.gen_range(0.0, f64::MAX);
                                if select <= prob {
                                    selected = Some((time, datum[0].clone()));
                                }
                                counted += 1f64;
                            }
                            if let Some((time, datum)) = selected {
                                let mut session = output.session(&time);
                                session.give(datum);
                            }
                        }
                    }
                )
                .unary_frontier(
                    Pipeline,
                    "Select global initial",
                    // this is repeated code from above until i can figure out
                    // how to implement the closure as an external method
                    |_, _| {
                        let mut prob = f64::MAX;
                        let mut select: f64;
                        let mut counted = 1f64;
                        move |input, output| {
                            let mut selected = None;
                            while let Some((time, datum)) = input.next() {
                                prob = f64::MAX / counted;
                                select = gen.gen_range(0.0, f64::MAX);
                                if select <= prob {
                                    selected = Some((time, datum[0].clone()));
                                }
                                counted += 1f64;
                            }
                            if let Some((time, datum)) = selected {
                                let mut session = output.session(&time);
                                session.give(datum);
                            }
                        }
                    }
                )
                .broadcast();
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

// trait UnionFind {
//     fn union_find(&self) -> Self;
// }
//
// impl<G: Scope> UnionFind for Stream<G, (usize, usize)> {