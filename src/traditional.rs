use crate::point::Point;
use crate::common::StreamSplitter::{RightStream, LeftStream};
use std::f64;
use timely::dataflow::channels::pact::Pipeline;
use timely::{Data, ExchangeData};
use std::collections::HashMap;
use std::borrow::ToOwned;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use rand::{Rng, thread_rng};
use timely::dataflow::operators::{Operator, Exchange, Broadcast, Map, Concat, Accumulate, Feedback, Enter, ConnectLoop, BranchWhen, Leave, Branch, Inspect};
use timely::dataflow::{
    Stream,
    Scope
};
use timely::order::Product;

use crate::euclidean_distance::EuclideanDistance;

pub trait KMeansPPInitialise<G: Scope, D: Data> {
    fn kmeans_pp_initialise(&self, k: usize, wid: usize) -> Stream<G, D>;
}

pub trait ScalableInitialise<G: Scope, D: Data> {
    fn scalable_initialise(&self, k: usize, wid: usize) -> Stream<G, D>;
}

pub trait LloydsIteration<G: Scope, D: Data> {
    fn lloyds_iteration(&self, cats: &Stream<G, D>, limit: u64) -> Stream<G, D>;
}

impl<G: Scope<Timestamp=u64>> ScalableInitialise<G, Vec<Point>>
for Stream<G, Point> {
    fn scalable_initialise(&self, k: usize, wid: usize)
        -> Stream<G, Vec<Point>>
    {
        // select first initial data point
        let (cats, raw_data) = self.select_random(wid);
        let raw_data = raw_data.map(|v| (f64::MAX, v))
            .closest_neighbour(&cats);
        let initial_sum = raw_data
            .sum_square_distances()
            .map(|v| v.log10() as u64);

        let initial_cats = self.scope().iterative::<u64, _, _>(
            move |iter_scope| {
                // set up the iteration
                let (cond_iter_handle, cond_iter_stream) =
                    (*iter_scope).feedback(Product::new(0, 1));
                let (cat_iter_handle, cat_iter_stream) =
                    (*iter_scope).feedback(Product::new(0, 1));
                let (data_iter_handle, data_iter_stream) =
                    (*iter_scope).feedback(Product::new(0, 1));

                // enter the subscope so iterations don't get mixed in together
                let cats = cats
                    .enter(iter_scope)
                    .concat(&cat_iter_stream);
                let raw_data = raw_data
                    .enter(iter_scope)
                    .concat(&data_iter_stream);
                let cond = initial_sum
                    .enter(iter_scope)
                    .concat(&cond_iter_stream);

                // run the iteration
                let raw_data = raw_data.closest_neighbour(&cats);
                let sums = raw_data.sum_square_distances();
                let (sampled, passed) = raw_data
                    .sample_data(&sums.map(move |v| (v, k)));
                let sampled = sampled.map(|v| v.1);

                sampled.branch_after(
                        &cond,
                        |time, val| {
                            time.inner >= *val
                        }
                    ).0.connect_loop(cat_iter_handle);
                passed.branch_after(
                        &cond,
                        |time, val| {
                            time.inner >= *val
                        }
                    ).0.connect_loop(data_iter_handle);
                cond.branch_after(
                    &cond,
                    |time, val| {
                        time.inner >= *val
                    }
                ).0.connect_loop(cond_iter_handle);

                // use the samples to create the cateogires, then
                // return the chosen categories and passed-on data
                sampled.leave().kmeans_pp_initialise(k, wid)
            }
        );
        initial_cats
    }
}

impl<G: Scope<Timestamp=u64>> LloydsIteration<G, Vec<Point>>
for Stream<G, Point> {
    fn lloyds_iteration(&self, cats: &Stream<G, Vec<Point>>, limit: u64)
        -> Stream<G, Vec<Point>>
    {
        self.scope().iterative::<u64, _, _>(
            move |iter_scope| {
                let (data_iter_handle, data_iter_stream) =
                    (*iter_scope).feedback(Product::new(0, 1));
                let (cat_iter_handle, cat_iter_stream) =
                    (*iter_scope).feedback(Product::new(0, 1));

                // enter the scope to begin the iteration
                let data = self
                    .map(|v| (f64::MAX, v))
                    .enter(iter_scope).concat(&data_iter_stream);
                let cats = cats
                    .enter(iter_scope).concat(&cat_iter_stream)
                    .inspect_batch(
                        |t, v| println!("cats at {:?}: {:?}", t.inner, v)
                    );

                // iterate until either 100 iterations or convergence
                let (data, new_cats) =
                    data.update_categories(&cats);
                let data = data
                    .branch(move |t, d| d.0.is_nan() || t.inner >= limit);
                let new_cats = new_cats
                    .branch(move |t, d|
                        d.iter().all(|v| !v.0) || t.inner >= limit
                    );
                new_cats.0
                    .map(|v| v.into_iter().map(|v| v.1).collect())
                    .inspect_batch(
                        |t, v|
                            println!("looping cats at {:?}: {:?}", t.inner, v)
                    )
                    .connect_loop(cat_iter_handle);
                data.0
                    .connect_loop(data_iter_handle);

                // return the final categories
                new_cats.1
                    .inspect_batch(
                        |t, v|
                            println!("returning cats at {:?}: {:?}", t.inner, v)
                    )
                    .map(|v|
                        v.into_iter().map(|v| v.1).collect()
                    )
                    .leave()
            }
        )
    }
}

impl<G: Scope<Timestamp=u64>> KMeansPPInitialise<G, Vec<Point>> for Stream<G, Point> {
    fn kmeans_pp_initialise(&self, k: usize, wid: usize)
    -> Stream<G, Vec<Point>>
    {
        // select first initial data point
        let (cats, raw_data) = self.select_random(wid);

        // select 'k - 1' more initial data points
        let cats = self.scope().iterative::<u64, _, _>(
            move |iter_scope| {

                // set up the iteration
                let (cat_iter_handle, cat_iter_stream) =
                    (*iter_scope).feedback(Product::new(0, 1));
                let (data_iter_handle, data_iter_stream) =
                    (*iter_scope).feedback(Product::new(0, 1));

                // enter the subscope so iterations don't get mixed in together
                let cats = cats
                    .enter(iter_scope)
                    .concat(&cat_iter_stream);
                let raw_data = raw_data
                    .map(|v| (f64::MAX, v))
                    .enter(iter_scope)
                    .concat(&data_iter_stream);

                // run the iteration
                let raw_data = raw_data
                    .closest_neighbour(&cats);
                let sums = raw_data
                    .sum_local_squared_distances();
                let (new_cat, passed) = raw_data
                    .select_weighted_initial(&sums);
                let new_cat = new_cat
                    .map(|v| v.1)
                    .branch_when(move |time|
                        k > 1 && time.inner >= (k - 2) as u64
                    );
                new_cat.0.connect_loop(cat_iter_handle);
                passed.branch_when(move |time|
                    k > 1 && time.inner >= (k - 2) as u64
                ).0.connect_loop(data_iter_handle);

                // return the chosen categories
                cats.concat(&new_cat.1).leave()
            });
        cats.create_categories()
    }
}

// traits for internal use only
trait SumDistances<G: Scope, D: Data> {
    fn sum_square_distances(&self) -> Stream<G, D>;
}

trait SumLocalDistances<G: Scope, D: Data> {
    fn sum_local_squared_distances(&self) -> Stream<G, D>;
}

trait SumStream<G: Scope, D1: Data> {
    fn sum(&self) -> Stream<G, D1>;
}

trait BranchAfter<G: Scope, D1: Data, D2: Data> {
    fn branch_after<L>(&self, cond: &Stream<G, D2>, logic: L) -> (Stream<G, D1>, Stream<G, D1>)
    where L: Fn(&G::Timestamp, &D2) -> bool + 'static;
}

trait ClosestNeighbour<G: Scope, D1: Data, D2: Data> {
    fn closest_neighbour(&self, sampled: &Stream<G, D1>) -> Stream<G, D2>;
}

trait SelectSamples<G: Scope, D1: Data, D2: Data> {
    fn sample_data(&self, selection_ratios: &Stream<G, D2>) -> (Stream<G, D1>, Stream<G, D1>);
}

trait SelectSamplesLocal<G: Scope, D1: Data, D2: Data> {
    fn sample_data_local(&self, selection_ratios: &Stream<G, D2>) -> (Stream<G, D1>, Stream<G, D1>);
}

trait SelectWeightedInitial<G: Scope, D1: Data, D2: Data> {
    fn select_weighted_initial(&self, sums: &Stream<G, D2>) -> (Stream<G, D1>, Stream<G, D1>);
}

trait SelectWeightedInitialLocal<G: Scope, D1: Data, D2: Data> {
    fn select_weighted_initial_local(&self, sums: &Stream<G, D2>) -> (Stream<G, D1>, Stream<G, D1>);
}

trait SelectRandom<G: Scope, D1: Data, D2: Data> {
    fn select_random(&self, id: usize) -> (Stream<G, D1>, Stream<G, D2>);
}
trait SelectLocalRandom<G: Scope, D1: Data, D2: Data> {
    fn select_local_random(&self) -> (Stream<G, D1>, Stream<G, D2>);
}

trait UpdateCategories<G: Scope, D1: Data, D2: Data, D3: Data> {
    fn update_categories(&self, cats: &Stream<G, D2>) -> (Stream<G, D1>, Stream<G, D3>);
}

trait UpdateCategoriesLocal<G: Scope, D1: Data, D2: Data, D3: Data> {
    fn update_categories_local(&self, cats: &Stream<G, D2>) -> (Stream<G, D1>, Stream<G, D3>);
}

trait CreateCategories<G: Scope, D1: Data> {
    fn create_categories(&self) -> Stream<G, Vec<D1>>;
}

trait DuplicateStream<G: Scope, D1: Data> {
    fn duplicate(&self) -> (Stream<G, D1>, Stream<G, D1>);
}

impl<G: Scope, D1: Data> BranchAfter<G, D1, u64>
for Stream<G, D1> {
    fn branch_after<L>(&self, cond: &Stream<G, u64>, logic: L)
        -> (Stream<G, D1>, Stream<G, D1>)
        where L: Fn(&G::Timestamp, &u64) -> bool + 'static {
        let sorted_stream = self.binary_frontier(
            cond,
            Pipeline,
            Pipeline,
            "Brancher",
            |_, _| {
                let mut stash = HashMap::new();
                let mut to_send = HashMap::new();
                move |data_input, cond_input, output| {

                    // collect the condition on which we split the stream
                    while let Some((cap, v)) = cond_input.next() {
                        let cond = stash.entry(cap.retain())
                            .or_insert(Some(0u64))
                            .as_mut().unwrap();
                        *cond += v.iter().sum::<u64>();
                    }
                    for (time, val_opt) in stash.iter_mut() {
                        if !cond_input.frontier().less_equal(time.time()) {
                            let val = val_opt.as_mut().unwrap();
                            let mut sesh = output.session(time);

                            // first send any stashed data
                            let mut data = to_send.remove(time.time()).unwrap_or_else(Vec::new);
                            for datum in data {
                                if logic(time.time(), &val) {
                                    sesh.give(RightStream(datum));
                                } else {
                                    sesh.give(LeftStream(datum));
                                }
                            }

                            // then send data that's just come in
                            while let Some((cap, data)) = data_input.next() {
                                if cap.time() != time.time() {
                                    let stash = to_send
                                        .entry(cap.time().clone())
                                        .or_insert_with(Vec::new);
                                    stash.append(&mut data.replace(Vec::new()));
                                } else {
                                    for datum in data.replace(Vec::new()) {
                                        if logic(cap.time(), &val) {
                                            sesh.give(RightStream(datum));
                                        } else {
                                            sesh.give(LeftStream(datum));
                                        }
                                    }
                                }
                            }
                        }
                        if !data_input.frontier().less_equal(time.time()) {
                            *val_opt = None;
                        }
                    }
                    stash.retain(|_, v| v.is_some());
                }
            }
        );
        let (out0, out1) =
            sorted_stream.branch(|_, d| d.path());
        (out0.map(|v| v.left()), out1.map(|v| v.right()))
    }
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
                        let now_cats =
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
        let (data, new_cats) =
            self.update_categories_local(cats);
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
            cats,
            Pipeline,
            Pipeline,
            "Compare new categories with old",
            |_, _| {
                let mut old_cats = HashMap::new();
                let mut new_cats = HashMap::new();
                move |in_new, in_old, out| {
                    while let Some((cap, dat)) = in_new.next() {
                        new_cats.insert(cap.time().clone(), dat[0].clone());
                    }
                    while let Some((cap, dat)) = in_old.next() {
                        old_cats.insert(cap.retain(), Some(dat[0].clone()));
                    }
                    for (time, old_cats_opt) in old_cats.iter_mut() {
                        if !in_new.frontier().less_equal(time.time())
                        && !in_old.frontier().less_equal(time.time()) {
                            let old_cats = old_cats_opt.as_ref().unwrap();
                            let mut sesh = out.session(time);
                            let mut results = vec!();
                            if let Some(new_cats) = new_cats.remove(time) {
                                for (old, new) in old_cats.iter().zip(new_cats.iter()) {
                                    let new_cat = if new.1 != 0 {
                                        new.0.scalar_div(new.1 as f64)
                                    } else {
                                        new.0
                                    };
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
                    old_cats.retain(|_, h| h.is_some());
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
        let stream_splitter = self.binary_frontier(
            cats,
            Pipeline,
            Pipeline,
            "Update categories",
            |_, _| {
                let mut old_cats = HashMap::new();
                let mut new_cats = HashMap::new();
                let mut to_sample: HashMap<_, Vec<(f64, Point)>> = HashMap::new();
                move |data_input, cats_input, output| {
                    while let Some((cap, weight)) = cats_input.next() {
                        let cats = old_cats.entry(cap.retain())
                            .or_insert_with(|| Some(Vec::new()))
                            .as_mut().unwrap();
                        *cats = weight.get(0).unwrap_or_else(|| cats).clone();
                    }
                    for (time, cat_opts) in old_cats.iter_mut() {
                        if !cats_input.frontier().less_equal(time.time()) {
                            let cats = cat_opts.as_mut().unwrap();
                            let mut new_cat_list = new_cats.entry(time.time().clone())
                                .or_insert_with(|| vec!((Point::origin(), 0); cats.len()));
                            let mut sesh = output.session(time);

                            if cats.len() > 0 {

                                // drain stashed data first
                                // let mut new_cat_list = vec!((Point::origin(), 0); cats.len());
                                // let data = to_sample.remove(time.time()).unwrap_or_else(Vec::new);
                                // for mut datum in data {
                                //     datum.0 = f64::MAX;
                                //     let mut cat_i = 0;
                                //     for (i, cat) in cats.iter().enumerate() {
                                //         let new_dist = datum.1.distance(cat);
                                //         if new_dist < datum.0 {
                                //             datum.0 = new_dist;
                                //             cat_i = i;
                                //         }
                                //     }
                                //     new_cat_list[cat_i].0 = new_cat_list[cat_i].0.add(&datum.1);
                                //     new_cat_list[cat_i].1 += 1;
                                //     sesh.give(LeftStream(datum));
                                // }

                                // then get to current data
                                while let Some((cap, data)) = data_input.next() {
                                    if cap.time() != time.time() {
                                        let incoming_data = to_sample
                                            .entry(cap.time().clone())
                                            .or_insert_with(Vec::new);
                                        incoming_data.append(&mut data.replace(Vec::new()));
                                    } else {
                                        let mut to_send = to_sample.remove(cap.time()).unwrap_or_else(Vec::new);
                                        to_send.append(&mut data.replace(Vec::new()));
                                        for mut datum in to_send {
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
                                            sesh.give(LeftStream(datum));
                                        }
                                    }
                                }
                            }
                        }
                        if !data_input.frontier().less_equal(time.time()) {
                            let mut sesh = output.session(time);
                            if let Some(send) = new_cats.remove(time.time()) {
                                sesh.give(RightStream(send));
                            }
                            let to_send = to_sample.remove(time.time()).unwrap_or_else(Vec::new);
                            println!("missing {:?}", to_send.len());
                            sesh.give_vec(&mut to_send.into_iter().map(|v| LeftStream(v)).collect());
                            *cat_opts = None;
                        }
                    }
                    old_cats.retain(|_, cats| cats.is_some());
                }
            }
        );
        let (data, cats) = stream_splitter
            .branch(|_, d| d.path());
        (data.map(|v| v.left()), cats.map(|v| v.right()))
    }
}

impl<G: Scope> SelectWeightedInitial<G, (f64, Point), f64> for Stream<G, (f64, Point)> {
    fn select_weighted_initial(&self, sums: &Stream<G, f64>)
                                     -> (Stream<G, (f64, Point)>, Stream<G, (f64, Point)>)
    {
        let (sampled, data) = self
            .select_weighted_initial_local(sums);
        let sub_sums = sampled
            .exchange(|_| 0)
            .sum_local_squared_distances();
        let (glob_sampled, rejected) = sampled
            .exchange(|_| 0)
            .select_weighted_initial_local(&sub_sums);
        (glob_sampled.broadcast(), data.concat(&rejected))
    }
}

// TODO: this operator will need to have data flow control when using lots of data
// left is the sampled value, right is the passed on data
impl<G: Scope> SelectWeightedInitialLocal<G, (f64, Point), f64> for Stream<G, (f64, Point)> {
    fn select_weighted_initial_local(&self, sums: &Stream<G, f64>)
        -> (Stream<G, (f64, Point)>, Stream<G, (f64, Point)>)
    {
        let stream_splitter = self.binary_frontier(
            sums,
            Pipeline,
            Pipeline,
            "Select weighted initial",
            |_, _| {
                let mut probs = HashMap::new();
                let mut to_sample = HashMap::new();
                let mut generator = rand::thread_rng();
                move |data_input, ratio_input, output| {
                    while let Some((cap, weight)) = ratio_input.next() {
                        let rate = probs.entry(cap.time().clone()).or_insert(0f64);
                        *rate = generator.gen_range(0.0, weight[0]);
                    }
                    while let Some((cap, data)) = data_input.next() {
                        let incoming_data = to_sample
                            .entry(cap.retain())
                            .or_insert_with(|| Some(Vec::new()))
                            .as_mut().unwrap();
                        incoming_data.append(&mut data.replace(Vec::new()));
                    }
                    for (time, data_opt) in to_sample.iter_mut() {
                        if !data_input.frontier().less_equal(time)
                            && !ratio_input.frontier().less_equal(time) {
                            let mut rep_opt = None;
                            std::mem::swap(&mut rep_opt, data_opt);
                            let mut sesh = output.session(time);
                            let mut data = rep_opt.unwrap_or_else(Vec::new);
                            if let Some(mut prob) = probs.remove(time.time()) {
                                if prob > 0.0 {
                                    while let Some(datum) = data.pop() {
                                        prob -= datum.0.powi(2);
                                        if prob <= 0.0 {
                                            sesh.give(LeftStream(datum));
                                            break;
                                        } else {
                                            sesh.give(RightStream(datum));
                                        }
                                    }
                                }
                            }
                            while let Some(datum) = data.pop() {
                                sesh.give(RightStream(datum));
                            }
                            *data_opt = None;
                        }
                    }
                    to_sample.retain(|_, v| v.is_some());
                }
            }
        );
        let (samples, remaining) = stream_splitter
            .branch(|_, v| v.path());
        (samples.map(|v| v.left()), remaining.map(|v| v.right()))
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
        let splitter_stream = self.binary_frontier(
            selection_ratios,
            Pipeline,
            Pipeline,
            "Sample data",
            |_, _| {
                let mut rates = HashMap::new();
                let mut to_sample = HashMap::new();
                let mut generator = rand::thread_rng();
                move |data_input, rates_input, output| {
                    while let Some((cap, weight)) = rates_input.next() {
                        let rate = rates.entry(cap.time().clone()).or_insert((1f64, 0));
                        *rate = weight[0];
                    }
                    while let Some((cap, data)) = data_input.next() {
                        let incoming_data = to_sample
                            .entry(cap.retain())
                            .or_insert_with(|| Some(Vec::new()))
                            .as_mut().unwrap();
                        incoming_data.append(&mut data.replace(Vec::new()));
                    }
                    for (time, data_opt) in to_sample.iter_mut() {
                        if !data_input.frontier().less_equal(time.time())
                            && !rates_input.frontier().less_equal(time.time()) {
                            let mut rep_opt = None;
                            std::mem::swap(&mut rep_opt, data_opt);
                            let mut sesh = output.session(time);
                            let mut data = rep_opt.unwrap_or_else(Vec::new);
                            if let Some(weight) = rates.remove(time.time()) {
                                while let Some(datum) = data.pop() {
                                    let prob = generator.gen_range(0.0, 1.0);
                                    let select = (datum.0.powi(2) * weight.1 as f64) / weight.0;
                                    if prob < select {
                                        sesh.give(LeftStream(datum));
                                    } else {
                                        sesh.give(RightStream(datum));
                                    }
                                }
                            }
                            *data_opt = None;
                        }
                    }
                    to_sample.retain(|_, v| v.is_some());
                }
            }
        );
        let (samples, data) = splitter_stream
            .branch(|_, d| d.path());
        (samples.map(|v| v.left()), data.map(|v| v.right()))
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
                        *sum += data.iter().sum::<f64>();
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

impl<G: Scope> SumDistances<G, f64> for Stream<G, (f64, Point)> {
    fn sum_square_distances(&self) -> Stream<G, f64> {
        let sum = self.sum_local_squared_distances();
        let glob_sum = sum.broadcast().sum();
        glob_sum
    }
}

impl<G: Scope> SumLocalDistances<G, f64> for Stream<G, (f64, Point)> {
    fn sum_local_squared_distances(&self) -> Stream<G, f64> {
        self.unary_frontier(
            Pipeline,
            "Local sum",
            |_, _| {
                let mut sums = HashMap::new();
                move |input, output| {
                    while let Some((cap, data)) = input.next() {

                        // sum the weights found with the values, then pipe them
                        let sum = sums.entry(cap.retain()).or_insert(
                            0f64
                        );
                        data.iter().for_each(|v| {
                            *sum += v.0.powi(2);
                        });
                    }
                    for (time, sum) in sums.iter_mut() {
                        if !input.frontier().less_equal(time.time()) {

                            // send the sum along its way
                            let mut sesh = output.session(time);
                            sesh.give(*sum);
                            *sum = f64::NAN;
                        }
                    }
                    sums.retain(|_, sum| !sum.is_nan());
                }
            }
        )
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
                        let sampled = sample_stash
                            .entry(time.time().clone())
                            .or_insert_with(Vec::new);
                        for datum in data.iter() {
                            sampled.push(*datum);
                        }
                    }

                    // compare all the other data with the sampled to re-calcaulte their nearest neighbour
                    while let Some((time, data)) = data.next() {
                        let points = data_stash
                            .entry(time.retain())
                            .or_insert_with(|| Some(Vec::new()))
                            .as_mut()
                            .unwrap();
                        for datum in data.iter() {
                            points.push(*datum);
                        }
                    }
                    for (cap, to_update) in data_stash.iter_mut() {
                        // ensure we have all samples before proceeding
                        if !samples.frontier().less_equal(cap.time())
                            && !data.frontier().less_equal(cap.time()) {
                            let mut session = output.session(cap);
                            let to_update_vec = to_update.as_mut().unwrap();
                            let stashed_vec = sample_stash
                                .remove(cap).unwrap_or_else(Vec::new);
                            for (old_dist, point) in to_update_vec {
                                let mut best_dist = *old_dist;
                                for sampled in stashed_vec.iter() {
                                    let new_dist = point.distance(sampled);
                                    if new_dist < best_dist {
                                        best_dist = new_dist;
                                    }
                                }
                                session.give((best_dist, *point));
                            }
                            *to_update = None;
                        }
                    }
                    data_stash.retain(|_, x| x.is_some());
                }
            }
        )
    }
}

// selects a single value from the stream randomly and evenly among all values
impl<G: Scope, D: Data> SelectLocalRandom<G, D, D> for Stream<G, D> {
    fn select_local_random(&self) -> (Stream<G, D>, Stream<G, D>) {
        let stream_splitter = self.unary_frontier(
            Pipeline,
            "Select local random",
            |_, _| {
                let mut firsts = HashMap::new();
                let mut gen = thread_rng();
                move |input, output| {
                    while let Some((time, data)) = input.next() {
                        let mut vector = Vec::new();

                        // create a "copy" time so we can retain this one and use the session
                        // at the same time as having the first available
                        let time_other = time.delayed(time.time());
                        data.swap(&mut vector);
                        let first = firsts.entry(time.retain()).or_insert(
                            (1f64, None)
                        );

                        // loop through all data individually, maintain a single value
                        // to be selected once all data has passed
                        let mut sesh = output.session(&time_other);
                        for datum in vector.drain(..) {
                            let select = gen.gen_range(0f64, f64::MAX);
                            let prob = f64::MAX / first.0;
                            if prob >= select {
                                let mut to_send = None;
                                std::mem::swap(&mut to_send, &mut first.1);
                                if let Some(p) = to_send {
                                    sesh.give(RightStream(p));
                                }
                                first.1 = Some(datum);
                            } else {
                                sesh.give(RightStream(datum));
                            }
                            first.0 += 1f64;
                        }
                    }
                    for (time, first) in firsts.iter_mut() {
                        if !input.frontier().less_equal(time) {
                            let mut to_send = None;
                            std::mem::swap(&mut to_send, &mut first.1);
                            if let Some(s) = to_send {
                                let mut sesh = output.session(time);
                                sesh.give(LeftStream(s));
                            }
                        }
                    }
                    firsts.retain(|_, first| first.1.is_some());
                }
            }
        );
        let (selected, remaining) = stream_splitter
            .branch(|_, v| v.path());
        (selected.map(|v| v.left()), remaining.map(|v| v.right()))
    }
}

// selects a single random value evenly from among all workers
impl<G: Scope, D: ExchangeData> SelectRandom<G, D, D> for Stream<G, D> {
    fn select_random(&self, id: usize) -> (Stream<G, D>, Stream<G, D>) {

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