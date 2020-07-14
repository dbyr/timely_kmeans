extern crate rust_classifiers;
use rust_classifiers::example_datatypes::point::Point;
use rust_classifiers::euclidean_distance::EuclideanDistance;

use timely::dataflow::operators::{Inspect, Broadcast};
use timely::dataflow::operators::map::Map;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{
    Accumulate,
    Operator
};
use timely::dataflow::*;
use timely::dataflow::operators::{Input, Exchange, Probe};
use rand::{thread_rng, Rng};
use std::f64;
use timely::dataflow::operators::generic::{FrontieredInputHandle, OutputHandle};

const DEPLETION_RATE: f64 = 2.0;

fn select_single_random_from_stream<G: Scope, T, D, P>(
    &mut FrontieredInputHandle<T, D, P>,
    &mut OutputHandle<T, D, P>)
-> Stream<G, Point> {

}

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();
        let index = worker.index();
        let mut gen = thread_rng();

        let mut selections = Vec::new();
        let mut calculated = 0;

        worker.dataflow(|scope| {
            scope.input_from(&mut input)
                .unary_frontier(
                    Exchange::new(|p: &Point| {0}), // p0 selects initial
                    "Select initial",
                    |_, _| {
                        let mut prob = f64::MAX;
                        let mut select;
                        let mut selected = None;
                        move |input, output| {
                            while let Some((time, datum)) = input.next() {
                                select = gen.gen_range::<f64, f64, f64>(
                                    0.0,
                                    f64::MAX
                                );
                                if select <= prob {
                                    selected = Some((time, datum));
                                    prob /= DEPLETION_RATE;
                                } else {
                                    let mut session = output.session(&time);
                                    session.give(datum);
                                }
                            }
                            if let Some((time, datum)) = selected {

                            }
                        }
                    }
                )
                .
        });

        //     scope.input_from(&mut input)
        //         .broadcast() // everyone sends their local totals to each other
        //         .accumulate( // everyone calculates the new global means
        //             vec!(),
        //             move |totals: &mut Vec<(Point, usize)>, 
        //             locals: timely_communication::message::RefOrMut<Vec<Vec<(Point, usize)>>>| {
        //                 for local in locals.iter() {
        //                     for (i, pair) in local.iter().enumerate() {
        //                         if totals.len() <= i {
        //                             totals.push((pair.0.clone(), pair.1));
        //                         } else {
        //                             totals[i].0.add(&pair.0);
        //                             totals[i].1 += pair.1;
        //                         }
        //                     }
        //                 }
        //             }
        //         )
        //         .map(|point_sums| {
        //             point_sums
        //             .into_iter()
        //             .map(|point_sum| point_sum.0.scalar_div(point_sum.1 as f64))
        //             .collect::<Vec<Point>>()
        //         })
        //         .inspect(move |v| println!("worker {} sees {:?}", index, v))
        //         .probe_with(&mut probe);
        // });

        // for i in 0..10 {
        //     println!("worker {} sending round {}", index, i);
        //     input.send(vec!(
        //         // these pairs will be the "sum" and "count" of values
        //         // associated with each mean (in this case, two)
        //         (Point::new(i as f64+5.0, i as f64), index + 1), // first mean
        //         (Point::new(-i as f64-5.0, i as f64), index + 1) // second mean
        //     ));
        //     input.advance_to(input.epoch() + 1);
        //     // while probe.less_than(input.time()) {
        //         worker.step();
        //     // }
        // }
    }).unwrap();
}