use std::error;
use std::fmt;

#[derive(Clone, Debug)]
pub struct NoSpaceLeftError;

impl fmt::Display for NoSpaceLeftError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "no space left on page to add value")
    }
}
impl error::Error for NoSpaceLeftError {}

#[derive(Clone, Debug)]
pub struct KeyAlreadyExistsError;

impl fmt::Display for KeyAlreadyExistsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "key already exists in page")
    }
}
impl error::Error for KeyAlreadyExistsError {}

#[derive(Clone, Debug)]
pub struct KeyDoesNotExistError;

impl fmt::Display for KeyDoesNotExistError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "key does not exist in page")
    }
}
impl error::Error for KeyDoesNotExistError {}

#[derive(Clone, Debug)]
pub struct MemmapCreationFailureError;

impl fmt::Display for MemmapCreationFailureError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "failed to create memmap")
    }
}
impl error::Error for MemmapCreationFailureError {}

#[derive(Clone, Debug)]
pub struct EntryAlreadyDeletedInFileError;

impl fmt::Display for EntryAlreadyDeletedInFileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "entry already deleted in file")
    }
}
impl error::Error for EntryAlreadyDeletedInFileError {}

#[derive(Clone, Debug)]
pub struct InvalidDataTypeError;

impl fmt::Display for InvalidDataTypeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid data type")
    }
}
impl error::Error for InvalidDataTypeError {}
