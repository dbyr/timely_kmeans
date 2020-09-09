use std::fs::File;

pub trait Attributable {
    // returns a string naming the attributes of this struct
    // returns: attribute name string
    fn attribute_names() -> String;

    // returns this object's values in a corresponding format to attribute_names
    // returns: attribute values string
    fn attribute_values(&self) -> String;
}

pub trait Savable {
    // saves this object to the given file
    // file: the file to which to save the object
    fn save_to_file(&self, file: File);

    // loads an object of this type from a file
    // file: the file from which to load an object
    fn load_from_file(file: File) -> Self;
}

#[derive(Abomonation, Clone)]
pub enum StreamSplitter<D1, D2> {
    LeftStream(D1),
    RightStream(D2)
}

impl<D1, D2> StreamSplitter<D1, D2> {
    pub fn left(self) -> D1 {
        match self {
            StreamSplitter::LeftStream(v) => v,
            StreamSplitter::RightStream(_) => panic!("Attempted to get left value from right value")
        }
    }
    pub fn right(self) -> D2 {
        match self {
            StreamSplitter::LeftStream(_) => panic!("Attempted to get right value from left value"),
            StreamSplitter::RightStream(v) => v
        }
    }
    pub fn path(&self) -> bool {
        match self {
            StreamSplitter::LeftStream(_) => false,
            StreamSplitter::RightStream(_) => true
        }
    }
}

impl<D> StreamSplitter<D, D> {
    pub fn unwrap(self) -> D {
        match self {
            StreamSplitter::LeftStream(v) => v,
            StreamSplitter::RightStream(v) => v
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum TrainingError {
    InvalidData,
    InvalidClassifier,
    InvalidFile,

    FileReadFailed,
}

#[derive(PartialEq, Debug)]
pub enum ClassificationError {
    ClassifierNotTrained,
    ClassifierInvalid
}