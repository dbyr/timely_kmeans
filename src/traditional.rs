use crate::point::Point;
use std::f64;
use timely::dataflow::channels::pact::Pipeline;
use timely::{Data, PartialOrder};
use std::collections::HashMap;
use std::borrow::ToOwned;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::progress::frontier::AntichainRef;
use rand::{Rng, thread_rng};
use timely::dataflow::operators::{Operator, Exchange, Broadcast, Map, Concat};
use timely::dataflow::{
    Stream,
    Scope
};

use crate::euclidean_distance::EuclideanDistance;

pub trait SumDistances<G: Scope, D1: Data, D2: Data> {
    fn sum_square_distances(&self) -> (Stream<G, D2>, Stream<G, D1>);
}

trait SumLocalDistances<G: Scope, D1: Data, D2: Data> {
    fn sum_local_squared_distances(&self) -> (Stream<G, D2>, Stream<G, D1>);
}

trait SumStream<G: Scope, D1: Data> {
    fn sum(&self) -> Stream<G, D1>;
}

pub trait ClosestNeighbour<G: Scope, D1: Data, D2: Data> {
    fn closest_neighbour(&self, sampled: &Stream<G, D1>) -> Stream<G, D2>;
}

pub trait SelectSamples<G: Scope, D1: Data, D2: Data> {
    fn sample_data(&self, selection_ratios: &Stream<G, D2>) -> (Stream<G, D1>, Stream<G, D1>);
}

pub trait SelectWeightedInitial<G: Scope, D1: Data, D2: Data> {
    fn select_weighted_initial(&self, sums: &Stream<G, D2>) -> (Stream<G, D1>, Stream<G, D1>);
}

pub trait SelectRandom<G: Scope, D1: Data, D2: Data> {
    fn select_random(&self, id: usize) -> (Stream<G, D1>, Stream<G, D2>);
}
trait SelectLocalRandom<G: Scope, D1: Data, D2: Data> {
    fn select_local_random(&self) -> (Stream<G, D1>, Stream<G, D2>);
}

// TODO: this operator will need to have data flow control when using lots of data
impl<G: Scope> SelectWeightedInitial<G, (f64, Point), f64> for Stream<G, (f64, Point)> {
    fn select_weighted_initial(&self, sums: &Stream<G, f64>)
        -> (Stream<G, (f64, Point)>, Stream<G, (f64, Point)>)
    {
        let mut builder = OperatorBuilder::new("Select samples".to_owned(), self.scope());

        // set up the input and output points for this stream
        let mut data_input = builder.new_input(self, Pipeline);
        let mut ratio_input = builder.new_input(selection_ratios, Pipeline);
        let (mut sampled_output, sampled_stream) = builder.new_output();
        let (mut data_output, data_stream) = builder.new_output();
        let mut generator = rand::thread_rng();

        // builds the operator
        builder.build(move |mut caps| {
            let mut probs = HashMap::new();
            let mut to_sample = HashMap::new();
            let mut data_cap = caps.pop();
            let mut sampled_cap = caps.pop();

            move |frontiers| {
                if data_cap.is_none() {
                    return;
                }
                while let Some((cap, weight)) = ratio_input.next() {
                    let rate = probs.entry(cap.retain()).or_insert(0f64);
                    *rate = generator.gen_range(0.0, weight[0]);
                }
                while let Some((cap, data)) = data_input.next() {
                    let incoming_data = to_sample
                        .entry(cap.time().clone())
                        .or_insert_with(Vec::new);
                    incoming_data.append(&mut data.replace(Vec::new()));
                }
                let rates_frontier = &frontiers[0].frontier();
                let sample_frontier = &frontiers[1].frontier();
                if !probs.is_empty() {
                    for (time, prob) in probs.iter_mut() {
                        if !rates_frontier.less_equal(time.time()) {

                            let mut sample_handle = sampled_output.activate();
                            let mut sample_sesh = sample_handle.session(sampled_cap.as_ref().unwrap());
                            let mut data_handle = data_output.activate();
                            let mut data_sesh = data_handle.session(data_cap.as_ref().unwrap());
                            if let Some(data) = to_sample.get_mut(time.time()) {
                                if *prob >= 0.0 {
                                    while let Some(datum) = data.pop() {
                                        *prob -= datum.0;
                                        if *prob <= 0.0 {
                                            sample_sesh.give(datum);
                                            break;
                                        } else {
                                            data_sesh.give(datum);
                                        }
                                    }
                                }
                                while let Some(datum) = data.pop() {
                                    data_sesh.give(datum);
                                }
                            }
                            weight.0 = f64::NAN;

                            // downgrade the capabilities
                            let new_time = smallest_time(&sample_frontier, time.time());
                            match new_time {

                                Some(t) => {
                                    sampled_cap.as_mut().unwrap().downgrade(t);
                                    data_cap.as_mut().unwrap().downgrade(t);
                                }
                                None => {
                                    sampled_cap = None;
                                    data_cap = None;
                                }
                            }
                        }
                    }
                    probs.retain(|_, weight| !weight.0.is_nan());
                } else {
                    if let Some(t) = sampled_cap.as_ref() {
                        if !sample_frontier.less_equal(t) {
                            sampled_cap = None;
                            data_cap = None;
                        }
                    }
                }
            }
        });
        (sampled_stream, data_stream)
    }
}

// TODO: this operator will need to have data flow control when using lots of data
// ratios are (sum_of_distances_squared, number_of_categories)
impl<G: Scope> SelectSamples<G, (f64, Point), (f64, usize)> for Stream<G, (f64, Point)> {
    fn sample_data(&self, selection_ratios: &Stream<G, (f64, usize)>)
        -> (Stream<G, (f64, Point)>, Stream<G, (f64, Point)>)
    {
        let mut builder = OperatorBuilder::new("Select samples".to_owned(), self.scope());

        // set up the input and output points for this stream
        let mut data_input = builder.new_input(self, Pipeline);
        let mut ratio_input = builder.new_input(selection_ratios, Pipeline);
        let (mut sampled_output, sampled_stream) = builder.new_output();
        let (mut data_output, data_stream) = builder.new_output();
        let mut generator = rand::thread_rng();

        // builds the operator
        builder.build(move |mut caps| {
            let mut rates = HashMap::new();
            let mut to_sample = HashMap::new();
            let mut data_cap = caps.pop();
            let mut sampled_cap = caps.pop();

            move |frontiers| {
                if data_cap.is_none() {
                    return;
                }
                while let Some((cap, weight)) = ratio_input.next() {
                    let rate = rates.entry(cap.retain()).or_insert((0f64, 0));
                    *rate = weight[0];
                }
                while let Some((cap, data)) = data_input.next() {
                    let incoming_data = to_sample
                        .entry(cap.time().clone())
                        .or_insert_with(Vec::new);
                    incoming_data.append(&mut data.replace(Vec::new()));
                }
                let rates_frontier = &frontiers[0].frontier();
                let sample_frontier = &frontiers[1].frontier();
                if !rates.is_empty() {
                    for (time, weight) in rates.iter_mut() {
                        if !rates_frontier.less_equal(time.time()) {

                            let mut sample_handle = sampled_output.activate();
                            let mut sample_sesh = sample_handle.session(sampled_cap.as_ref().unwrap());
                            let mut data_handle = data_output.activate();
                            let mut data_sesh = data_handle.session(data_cap.as_ref().unwrap());
                            if let Some(data) = to_sample.get_mut(time.time()) {
                                while let Some(datum) = data.pop() {
                                    let prob = generator.gen_range(0.0, 1.0);
                                    let select = (datum.0.powi(2) * weight.1 as f64) / weight.0;
                                    if prob < select {
                                        sample_sesh.give(datum);
                                    } else {
                                        data_sesh.give(datum);
                                    }
                                }
                            }
                            weight.0 = f64::NAN;

                            // downgrade the capabilities
                            let new_time = smallest_time(&sample_frontier, time.time());
                            match new_time {

                                Some(t) => {
                                    sampled_cap.as_mut().unwrap().downgrade(t);
                                    data_cap.as_mut().unwrap().downgrade(t);
                                }
                                None => {
                                    sampled_cap = None;
                                    data_cap = None;
                                }
                            }
                        }
                    }
                    rates.retain(|_, weight| !weight.0.is_nan());
                } else {
                    if let Some(t) = sampled_cap.as_ref() {
                        if !sample_frontier.less_equal(t) {
                            sampled_cap = None;
                            data_cap = None;
                        }
                    }
                }
            }
        });
        (sampled_stream, data_stream)
    }
}

impl<G: Scope> SumStream<G, f64> for Stream<G, f64> {
    fn sum(&self) -> Stream<G, f64> {
        self.unary_frontier(
            Pipeline,
            "Sum values",
            |_, _| {
                let mut sums = HashMap::new();
                move |input, output| {
                    while let Some((cap, data)) = input.next() {
                        let sum = sums.entry(cap.retain()).or_insert(Some(0f64))
                            .as_mut().unwrap();
                        for datum in data.iter() {
                            *sum += *datum;
                        }
                    }
                    for (time, sum_opt) in sums.iter_mut() {
                        if !input.frontier().less_equal(time.time()) {
                            let mut session = output.session(time);
                            let sum = sum_opt.unwrap();
                            session.give(sum);
                            *sum_opt = None;
                        }
                    }
                    sums.retain(|_, v| v.is_some());
                }
            }
        )
    }
}

impl<G: Scope> SumDistances<G, (f64, Point), f64> for Stream<G, (f64, Point)> {
    fn sum_square_distances(&self) -> (Stream<G, f64>, Stream<G, (f64, Point)>) {
        let (sum, piped) = self.sum_local_squared_distances();
        let glob_sum = sum.exchange(|_| 0).sum().broadcast();
        (glob_sum, piped)
    }
}

impl<G: Scope> SumLocalDistances<G, (f64, Point), f64> for Stream<G, (f64, Point)> {
    fn sum_local_squared_distances(&self) -> (Stream<G, f64>, Stream<G, (f64, Point)>) {
        let mut builder = OperatorBuilder::new("Select local random".to_owned(), self.scope());

        // set up the input and output points for this stream
        let mut input = builder.new_input(self, Pipeline);
        let (mut sum_output, sum_stream) = builder.new_output();
        let (mut pipe_output, pipe_stream) = builder.new_output();

        // builds the operator
        builder.build(move |mut caps| {
            let mut sums = HashMap::new();
            let mut pipe_cap = caps.pop();
            let mut sum_cap = caps.pop();

            move |frontiers| {
                if sum_cap.is_none() {
                    return;
                }
                while let Some((cap, data)) = input.next() {
                    let sum = sums.entry(cap.time().clone()).or_insert(
                        0f64
                    );
                    let mut pipe_handle = pipe_output.activate();
                    let mut pipe_sesh = pipe_handle.session(pipe_cap.as_ref().unwrap());
                    for val in data.iter() {
                        *sum += val.0.powi(2);
                        pipe_sesh.give(*val);
                    }
                }
                let frontier = &frontiers[0].frontier();
                if !sums.is_empty() {
                    for (time, sum) in sums.iter_mut() {
                        if !frontier.less_equal(time) {

                            // send the sum along its way
                            let mut sum_handle = sum_output.activate();
                            let mut sum_sesh = sum_handle.session(sum_cap.as_ref().unwrap());
                            sum_sesh.give(*sum);
                            *sum = f64::NAN;

                            // downgrade the capabilities
                            let new_time = smallest_time(&frontier, time);
                            match new_time {
                                Some(t) => {
                                    sum_cap.as_mut().unwrap().downgrade(t);
                                    pipe_cap.as_mut().unwrap().downgrade(t);
                                }
                                None => {
                                    sum_cap = None;
                                    pipe_cap = None;
                                }
                            }
                        }
                    }
                    sums.retain(|_, sum| !sum.is_nan());
                } else {
                    if let Some(t) = pipe_cap.as_ref() {
                        if !frontier.less_equal(t) {
                            pipe_cap = None;
                        }
                    }
                    if let Some(t) = sum_cap.as_ref() {
                        if !frontier.less_equal(t) {
                            sum_cap = None;
                        }
                    }
                }
            }
        });
        (sum_stream, pipe_stream)
    }
}

// TODO: this operator will need to have data flow control when using lots of data
// https://timelydataflow.github.io/timely-dataflow/chapter_4/chapter_4_3.html#flow-control
impl<G: Scope> ClosestNeighbour<G, Point, (f64, Point)> for Stream<G, (f64, Point)> {
    fn closest_neighbour(&self, sampled: &Stream<G, Point>)
                         -> Stream<G, (f64, Point)> {
        self.binary_frontier(
            sampled,
            Pipeline,
            Pipeline,
            "Find closest neighbours",
            move |_, _| {
                let mut sample_stash = HashMap::new();
                let mut data_stash = HashMap::new();
                move |data, samples,  output| {
                    // insert the sampled data for this iteration
                    while let Some((time, data)) = samples.next() {
                        let (sampled, _) = sample_stash.entry(time.time().clone()).or_insert((Vec::new(), time.retain()));
                        for datum in data.iter() {
                            sampled.push(*datum);
                        }
                    }

                    // compare all the other data with the sampled to re-calcaulte their nearest neighbour
                    while let Some((time, data)) = data.next() {
                        let points = data_stash.entry(time.time().clone()).or_insert(Vec::new());
                        for datum in data.iter() {
                            points.push(*datum);
                        }
                    }
                    for (time, (stashed, cap)) in sample_stash.iter_mut() {

                        // ensure we have all samples before proceeding
                        if !samples.frontier().less_equal(&time) && !data.frontier().less_equal(&time) {
                            let mut session = output.session(cap);
                            let mut sampled = Vec::new();
                            let to_update = data_stash.remove(time).unwrap_or_else(|| Vec::new());
                            std::mem::swap(&mut sampled, stashed);
                            for (old_dist, point) in to_update {
                                for sampled in sampled.iter() {
                                    let new_dist = point.distance(sampled);
                                    if new_dist < old_dist {
                                        session.give((new_dist, point));
                                    } else {
                                        session.give((old_dist, point));
                                    }
                                }
                            }
                        }
                    }
                    sample_stash.retain(|_, (x, _)| !x.is_empty());
                    data_stash.retain(|_, x| !x.is_empty());
                }
            }
        )
    }
}

// selects the smallest time from chain larger than larger_than
fn smallest_time<'r, 'a, T: 'a + Ord + PartialOrder + Clone>(
    chain: &'r AntichainRef<'a, T>,
    larger_than: &T
) -> Option<&'r T> {
    let mut smallest = None;
    chain.iter().for_each(|v| {
        if !v.less_equal(larger_than) && smallest.is_none() {
            smallest = Some(v);
        } else if !smallest.is_none() && v.less_than(smallest.as_ref().unwrap()) {
            smallest = Some(v);
        }
    });
    smallest
}

// selects a single value from the stream randomly and evenly among all values
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
                let frontier = frontiers[0].frontier(); // should only be one to match the input
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
                                    data_cap.as_mut().unwrap().downgrade(t);
                                    selected_cap.as_mut().unwrap().downgrade(t);
                                },
                                None => {
                                    data_cap = None;
                                    selected_cap = None;
                                }
                            }
                        }
                    }
                } else {

                    // if firsts is empty and the timestamp has been surpassed, then
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
                firsts.retain(|_, first| first.1.is_some());
            }
        });
        (selected_stream, data_stream)
    }
}

// selects a single random value evenly from among all workers
impl<G: Scope> SelectRandom<G, Point, Point> for Stream<G, Point> {
    fn select_random(&self, id: usize) -> (Stream<G, Point>, Stream<G, Point>) {

        // selects a random value locally & independently, then globally
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