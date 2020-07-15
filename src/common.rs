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