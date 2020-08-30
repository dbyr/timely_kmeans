use crate::point::Point;
use std::f64;
use timely::dataflow::channels::pact::Pipeline;
use timely::{Data, PartialOrder};
use std::collections::HashMap;
use std::borrow::ToOwned;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::progress::frontier::AntichainRef;
use rand::{Rng, thread_rng};
use timely::dataflow::operators::{
    Operator,
    Exchange,
    Broadcast,
    Map,
    Concat,
    Accumulate,
    Inspect,
    Feedback,
    Enter,
    ConnectLoop,
    BranchWhen,
    Leave
};
use timely::dataflow::{
    Stream,
    Scope
};
use timely::dataflow::scopes::child::Child;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;
use timely::communication::Data as CommData;
use timely::progress::timestamp::PathSummary;

// allow the use of iteration that then re-accumulates to the
// base timestamp
#[derive(PartialEq, PartialOrd, Clone, Hash, Ord, Default, Debug, Eq, Abomonation)]
struct LoopStamp(u64, u64);
impl PartialOrder for LoopStamp {
    fn less_than(&self, other: &Self) -> bool {
        if self.0 < other.0 {
            true
        } else if self.0 == other.0 && self.1 < other.1 {
            true
        } else {
            false
        }
    }
    fn less_equal(&self, other: &Self) -> bool {
        if self.0 < other.0 {
            true
        } else if self.0 == other.0 && self.1 <= other.1 {
            true
        } else {
            false
        }
    }
}
impl PathSummary<LoopStamp> for LoopStamp {
    fn results_in(&self, other: &LoopStamp) -> Option<LoopStamp> {
        let mut result = LoopStamp(0, 0);
        let r0 = self.0.overflowing_add(other.0);
        let r1 = self.1.overflowing_add(other.1);
        if r0.1 || r1.1 {
            return None;
        } else {
            result.0 = r0.0;
            result.1 = r1.0
        }
        Some(result)
    }
    fn followed_by(&self, other: &Self) -> Option<Self> {
        self.results_in(other)
    }
}
// impl CommData for LoopStamp {}
impl Timestamp for LoopStamp {
    type Summary = LoopStamp;
}
impl Refines<u64> for LoopStamp {
    fn to_inner(other: u64) -> LoopStamp {
        LoopStamp(other, 0)
    }
    fn to_outer(self) -> u64 {
        if self.1 > 0 {
            self.0 + 1
        } else {
            self.0
        }
    }

    fn summarize(path: <LoopStamp as Timestamp>::Summary)
        -> <u64 as Timestamp>::Summary {
        if path.1 > 0 {
            path.0 + 1
        } else {
            path.0
        }
    }
}

use crate::euclidean_distance::EuclideanDistance;

pub trait KMeansPPInitialise<G: Scope, D1: Data, D2: Data> {
    fn kmeans_pp_initialise(&self, k: usize, wid: usize) -> (Stream<G, D1>, Stream<G, D2>);
}

pub trait ScalableInitialise<G: Scope, D1: Data, D2: Data> {
    fn scalable_initialise(&self, cats: usize) -> (Stream<G, D1>, Stream<G, D2>);
}

pub trait LloydsIteration<G: Scope, D1: Data> {
    fn lloyds_iteration(&self, cats: &Stream<G, D1>) -> Stream<G, D1>;
}

impl<G: Scope<Timestamp=u64>> KMeansPPInitialise<G, (f64, Point), Vec<Point>> for Stream<G, Point> {
    fn kmeans_pp_initialise(&self, k: usize, wid: usize)
    -> (Stream<G, (f64, Point)>, Stream<G, Vec<Point>>)
    {
        // select first initial data point
        let (mut cats, raw_data) = self.select_random(wid);

        // select 'k - 1' more initial data points
        let (data, cats) = self.scope().scoped::<LoopStamp, _, _>(
            "Select k scope",
            move |iter_scope| {

                // set up the iteration
                let (cat_iter_handle, cat_iter_stream) =
                    (*iter_scope).feedback(LoopStamp(0, 1));
                let (data_iter_handle, data_iter_stream) =
                    (*iter_scope).feedback(LoopStamp(0, 1));

                // enter the subscope so iterations don't get mixed in together
                let cats = cats.enter(iter_scope);
                let mut raw_data = raw_data
                    .map(|v| (f64::MAX, v))
                    .enter(iter_scope);

                // run the iteration
                raw_data = raw_data
                    .concat(&data_iter_stream)
                    .closest_neighbour(&cats.concat(&cat_iter_stream));
                let (sums, raw_data) = raw_data
                    .sum_square_distances();
                let (new_cat, passed) = raw_data
                    .select_weighted_initial(&sums);
                let (new_cat, addition) = new_cat
                    .map(|v| v.1)
                    .duplicate();
                new_cat
                    .branch_when(move |time|
                        *time >= LoopStamp(time.0, (k - 1) as u64)
                    ).0
                    .connect_loop(cat_iter_handle);
                let (reiter, passed) = passed
                    .branch_when(move |time|
                        *time >= LoopStamp(time.0, (k - 1) as u64)
                    );
                reiter.connect_loop(data_iter_handle);

                // return the chosen categories and passed-on data
                (passed.leave(), addition.leave())
            });
        (data, cats.create_categories())
    }
}

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

trait SelectSamplesLocal<G: Scope, D1: Data, D2: Data> {
    fn sample_data_local(&self, selection_ratios: &Stream<G, D2>) -> (Stream<G, D1>, Stream<G, D1>);
}

pub trait SelectWeightedInitial<G: Scope, D1: Data, D2: Data> {
    fn select_weighted_initial(&self, sums: &Stream<G, D2>) -> (Stream<G, D1>, Stream<G, D1>);
}

trait SelectWeightedInitialLocal<G: Scope, D1: Data, D2: Data> {
    fn select_weighted_initial_local(&self, sums: &Stream<G, D2>) -> (Stream<G, D1>, Stream<G, D1>);
}

pub trait SelectRandom<G: Scope, D1: Data, D2: Data> {
    fn select_random(&self, id: usize) -> (Stream<G, D1>, Stream<G, D2>);
}
trait SelectLocalRandom<G: Scope, D1: Data, D2: Data> {
    fn select_local_random(&self) -> (Stream<G, D1>, Stream<G, D2>);
}

pub trait UpdateCategories<G: Scope, D1: Data, D2: Data, D3: Data> {
    fn update_categories(&self, cats: &Stream<G, D2>) -> (Stream<G, D1>, Stream<G, D3>);
}

trait UpdateCategoriesLocal<G: Scope, D1: Data, D2: Data, D3: Data> {
    fn update_categories_local(&self, cats: &Stream<G, D2>) -> (Stream<G, D1>, Stream<G, D3>);
}

pub trait CreateCategories<G: Scope, D1: Data> {
    fn create_categories(&self) -> Stream<G, Vec<D1>>;
}

pub trait DuplicateStream<G: Scope, D1: Data> {
    fn duplicate(&self) -> (Stream<G, D1>, Stream<G, D1>);
}

impl<G: Scope, D1: Data + Clone> DuplicateStream<G, D1> for Stream<G, D1> {
    fn duplicate(&self) -> (Stream<G, D1>, Stream<G, D1>) {
        let mut builder = OperatorBuilder::new("Select samples".to_owned(), self.scope());

        // set up the input and output points for this stream
        let mut input = builder.new_input(self, Pipeline);
        let (mut output0, stream0) = builder.new_output();
        let (mut output1, stream1) = builder.new_output();

        // builds the operator
        builder.build(move |_| {
            move |_| {
                while let Some((cap, data)) = input.next() {
                    let mut handle0 = output0.activate();
                    let mut sesh0 = handle0.session(&cap);
                    let mut handle1 = output1.activate();
                    let mut sesh1 = handle1.session(&cap);
                    for datum in data.iter() {
                        sesh0.give(datum.clone());
                        sesh1.give(datum.clone());
                    }
                }
            }
        });
        (stream0, stream1)
    }
}

impl<G: Scope> CreateCategories<G, Point> for Stream<G, Point> {
    fn create_categories(&self) -> Stream<G, Vec<Point>> {
        self.unary_frontier(
            Pipeline,
            "Create categories",
            |_, _| {
                let mut cats = HashMap::new();
                move |input, output| {
                    while let Some((cap, points)) = input.next() {
                        let mut now_cats =
                            cats.entry(cap.retain()).or_insert_with(Vec::new);
                        for point in points.iter() {
                            now_cats.push(*point);
                        }
                    }
                    for (time, cats) in cats.iter_mut() {
                        if !input.frontier().less_equal(time.time()) {
                            let mut out = Vec::new();
                            let mut session = output.session(time);
                            std::mem::swap(&mut out, cats);
                            session.give(out);
                        }
                    }
                    cats.retain(|_, v| !v.is_empty());
                }
            }
        )
    }
}

impl<G: Scope> UpdateCategories<G, (f64, Point), Vec<Point>, Vec<(bool, Point)>>
for Stream<G, (f64, Point)> {
    fn update_categories(&self, cats: &Stream<G, Vec<Point>>)
                               -> (Stream<G, (f64, Point)>, Stream<G, Vec<(bool, Point)>>)
    {
        let (cat_compare, cat_update) = cats.duplicate();
        let (data, mut new_cats) =
            self.update_categories_local(&cat_update);
        let glob_new_cats = new_cats
            .exchange(|_| 0)
            .accumulate(
            vec!(),
            |sums, data|
                for v in data.iter() {
                    if sums.is_empty() {
                        *sums = vec!((Point::origin(), 0); v.len());
                    }
                    for (i, pair) in v.iter().enumerate() {
                        sums[i].0 = sums[i].0.add(&pair.0);
                        sums[i].1 += pair.1;
                    }
                }
            )
            .broadcast();

        let result = glob_new_cats.binary_frontier(
            &cat_compare,
            Pipeline,
            Pipeline,
            "Compare new categories with old",
            |_, _| {
                let mut old_cats = HashMap::new();
                let mut new_cats = HashMap::new();
                move |in_new, in_old, out| {
                    while let Some((cap, mut dat)) = in_new.next() {
                        new_cats.insert(cap.retain(), dat[0].clone());
                    }
                    while let Some((cap, mut dat)) = in_old.next() {
                        old_cats.insert(cap.retain(), Some(dat[0].clone()));
                    }
                    for (time, old_cats_opt) in old_cats.iter_mut() {
                        let old_cats = old_cats_opt.as_ref().unwrap();
                        if !in_new.frontier().less_equal(time.time()) {
                            let mut sesh = out.session(time);
                            let mut results = vec!();
                            if let Some(new_cats) = new_cats.remove(time) {
                                for (old, new) in old_cats.iter().zip(new_cats.iter()) {
                                    let new_cat = new.0.scalar_div(new.1 as f64);
                                    if new_cat == *old {
                                        results.push((false, new_cat));
                                    } else {
                                        results.push((true, new_cat));
                                    }
                                }
                            }
                            sesh.give(results);
                            *old_cats_opt = None;
                        }
                    }
                    old_cats.retain(|_, h| !h.is_none());
                }
            }
        );

        (data, result)
    }
}
// Input: Stream of points and distance to their previous closest categories
// and a second stream containing a vec of available categories
// Output: An equivalent (but updated) stream to the "self" input
// and a second stream of new categories and whether or not they changed
// from the given input categories
impl<G: Scope> UpdateCategoriesLocal<G, (f64, Point), Vec<Point>, Vec<(Point, usize)>>
for Stream<G, (f64, Point)> {
    fn update_categories_local(&self, cats: &Stream<G, Vec<Point>>)
        -> (Stream<G, (f64, Point)>, Stream<G, Vec<(Point, usize)>>)
    {
        let mut builder = OperatorBuilder::new("Select samples".to_owned(), self.scope());

        // set up the input and output points for this stream
        let mut data_input = builder.new_input(self, Pipeline);
        let mut cats_input = builder.new_input(cats, Pipeline);
        let (mut cats_output, cats_stream) = builder.new_output();
        let (mut data_output, data_stream) = builder.new_output();

        // builds the operator
        builder.build(move |mut caps| {
            let mut old_cats = HashMap::new();
            let mut new_cats = HashMap::new();
            let mut to_sample = HashMap::new();
            let mut data_cap = caps.pop();
            let mut cats_cap = caps.pop();

            move |frontiers| {
                if data_cap.is_none() {
                    return;
                }
                while let Some((cap, mut weight)) = cats_input.next() {
                    let time = cap.time().clone();
                    let cats = old_cats.entry(cap.retain())
                        .or_insert_with(Vec::new);
                    *cats = weight.get(0).unwrap().clone();
                    new_cats.insert(time, vec!((Point::origin(), 0); cats.len()));
                }
                while let Some((cap, data)) = data_input.next() {
                    let incoming_data = to_sample
                        .entry(cap.time().clone())
                        .or_insert_with(Vec::new);
                    incoming_data.append(&mut data.replace(Vec::new()));
                }
                // TODO: Make sure both frontiers are taken into account
                let data_frontier = &frontiers[0].frontier();
                if !old_cats.is_empty() {
                    for (time, cats) in old_cats.iter_mut() {
                        if !data_frontier.less_equal(time.time()) {

                            let mut cats_handle = cats_output.activate();
                            let mut cats_sesh = cats_handle.session(cats_cap.as_ref().unwrap());
                            let mut data_handle = data_output.activate();
                            let mut data_sesh = data_handle.session(data_cap.as_ref().unwrap());
                            let new_cat_list = new_cats.get_mut(time.time()).unwrap();
                            if let Some(data) = to_sample.get_mut(time.time()) {
                                while let Some(mut datum) = data.pop() {
                                    datum.0 = f64::MAX;
                                    let mut cat_i = 0;
                                    for (i, cat) in cats.iter().enumerate() {
                                        let new_dist = datum.1.distance(cat);
                                        if new_dist < datum.0 {
                                            datum.0 = new_dist;
                                            cat_i = i;
                                        }
                                    }
                                    new_cat_list[cat_i].0 = new_cat_list[cat_i].0.add(&datum.1);
                                    new_cat_list[cat_i].1 += 1;
                                    data_sesh.give(datum);
                                }
                            }
                            let mut cats_new = vec!();
                            for (point, sum) in new_cat_list {
                                cats_new.push((*point, *sum));
                            }
                            cats_sesh.give(cats_new);
                            *cats = Vec::new();

                            // downgrade the capabilities
                            let new_time = smallest_time(&data_frontier, time.time());
                            match new_time {

                                Some(t) => {
                                    cats_cap.as_mut().unwrap().downgrade(t);
                                    data_cap.as_mut().unwrap().downgrade(t);
                                }
                                None => {
                                    cats_cap = None;
                                    data_cap = None;
                                }
                            }
                        }
                    }
                    old_cats.retain(|_, prob| !prob.is_empty());
                } else {
                    if let Some(t) = data_cap.as_ref() {
                        if !data_frontier.less_equal(t) {
                            cats_cap = None;
                            data_cap = None;
                        }
                    }
                }
            }
        });
        (data_stream, cats_stream)
    }
}

impl<G: Scope> SelectWeightedInitial<G, (f64, Point), f64> for Stream<G, (f64, Point)> {
    fn select_weighted_initial(&self, sums: &Stream<G, f64>)
                                     -> (Stream<G, (f64, Point)>, Stream<G, (f64, Point)>)
    {
        let (sampled, data) = self
            .select_weighted_initial_local(sums);
        let (sub_sums, sampled) = sampled
            .exchange(|_| 0)
            .sum_local_squared_distances();
        let (mut glob_sampled, rejected) = sampled
            .select_weighted_initial_local(&sub_sums);
        glob_sampled = glob_sampled.broadcast();
        (glob_sampled, data.concat(&rejected))
    }
}

// TODO: this operator will need to have data flow control when using lots of data
// left is the sampled value, right is the passed on data
impl<G: Scope> SelectWeightedInitialLocal<G, (f64, Point), f64> for Stream<G, (f64, Point)> {
    fn select_weighted_initial_local(&self, sums: &Stream<G, f64>)
        -> (Stream<G, (f64, Point)>, Stream<G, (f64, Point)>)
    {
        let mut builder = OperatorBuilder::new("Select samples".to_owned(), self.scope());

        // set up the input and output points for this stream
        let mut data_input = builder.new_input(self, Pipeline);
        let mut ratio_input = builder.new_input(sums, Pipeline);
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
                while let Some((cap, weight)) = ratio_input.next() {
                    let rate = probs.entry(cap.time().clone()).or_insert(0f64);
                    *rate = generator.gen_range(0.0, weight[0]);
                }
                while let Some((cap, data)) = data_input.next() {
                    let incoming_data = to_sample
                        .entry(cap.time().clone())
                        .or_insert_with(Vec::new);
                    incoming_data.append(&mut data.replace(Vec::new()));
                }
                let ratio_frontier = &frontiers[1].frontier();
                let data_frontier = &frontiers[0].frontier();
                if !probs.is_empty() {
                    for (time, prob) in probs.iter_mut() {
                        if !data_frontier.less_equal(sampled_cap.as_ref().unwrap().time())
                         && !ratio_frontier.less_equal(sampled_cap.as_ref().unwrap().time()) {

                            let mut sample_handle = sampled_output.activate();
                            let mut sample_sesh = sample_handle.session(sampled_cap.as_ref().unwrap());
                            let mut data_handle = data_output.activate();
                            let mut data_sesh = data_handle.session(data_cap.as_ref().unwrap());
                            if let Some(data) = to_sample.get_mut(time) {
                                if *prob > 0.0 {
                                    while let Some(datum) = data.pop() {
                                        *prob -= datum.0.powi(2);
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
                            *prob = f64::NAN;

                            // downgrade the capabilities
                            let new_time =
                                smallest_time(&data_frontier, sampled_cap.as_ref().unwrap().time());
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
                    probs.retain(|_, prob| !prob.is_nan());
                } else if let (Some(t0), Some(t1)) =
                            (sampled_cap.as_ref(), data_cap.as_ref()) {
                    if !data_frontier.less_equal(t0)
                    && !data_frontier.less_equal(t1)
                    && !ratio_frontier.less_equal(t0)
                    && !ratio_frontier.less_equal(t1) {
                        sampled_cap = None;
                        data_cap = None;
                    }
                }
            }
        });
        (sampled_stream, data_stream)
    }
}

impl<G: Scope> SelectSamples<G, (f64, Point), (f64, usize)> for Stream<G, (f64, Point)> {
    fn sample_data(&self, selection_ratios: &Stream<G, (f64, usize)>)
                   -> (Stream<G, (f64, Point)>, Stream<G, (f64, Point)>)
    {
        let (sampled, data) =
            self.sample_data_local(selection_ratios);
        (sampled.broadcast(), data)
    }
}

// TODO: this operator will need to have data flow control when using lots of data
// ratios are (sum_of_distances_squared, number_of_categories)
// left is sampled, right is passed on data
impl<G: Scope> SelectSamplesLocal<G, (f64, Point), (f64, usize)> for Stream<G, (f64, Point)> {
    fn sample_data_local(&self, selection_ratios: &Stream<G, (f64, usize)>)
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
                let sample_frontier = &frontiers[0].frontier();
                if !rates.is_empty() {
                    for (time, weight) in rates.iter_mut() {
                        if !sample_frontier.less_equal(time.time()) {

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
                } else if let Some(t) = sampled_cap.as_ref() {
                    if !sample_frontier.less_equal(t) {
                        sampled_cap = None;
                        data_cap = None;
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
        -> Stream<G, (f64, Point)>
    {
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