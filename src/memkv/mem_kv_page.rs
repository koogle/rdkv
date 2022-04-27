use super::errors;
use log::{error, info, warn};
use memmap::MmapMut;
use serde::{Deserialize, Serialize};
use serde_json;
use std::cmp::Ordering;
use std::collections::hash_map::HashMap;
use std::collections::BinaryHeap;
use std::error;
use std::fmt;
use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::mem::size_of;
use std::panic;
use std::path::Path;
use std::path::PathBuf;
use std::str;
use std::sync::RwLock;
use std::usize;

const KV_PAGE_SIZE: u64 = 1024 * 1024 * 4; // 4 MB

#[derive(Copy, Clone)]
pub enum ValueDataType {
    String = 1,
    Integer = 2,
    Blob = 3,
}

impl TryFrom<u8> for ValueDataType {
    type Error = errors::InvalidDataTypeError;

    fn try_from(from_value: u8) -> Result<Self, Self::Error> {
        return match from_value {
            0x1 => Ok(ValueDataType::String),
            0x2 => Ok(ValueDataType::Integer),
            0x3 => Ok(ValueDataType::Blob),
            _ => Err(errors::InvalidDataTypeError),
        };
    }
}

impl fmt::Debug for ValueDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            ValueDataType::String => write!(f, "String"),
            ValueDataType::Integer => write!(f, "Integer"),
            ValueDataType::Blob => write!(f, "Blob"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    String(String),
    Integer(u64),
    Blob(Vec<u8>),
}

impl Value {
    fn get_bytes_length(self: &Self) -> Result<usize, Box<dyn error::Error>> {
        return match self {
            Value::String(text) => Ok(text.as_bytes().len()),
            Value::Integer(number) => Ok(number.to_be_bytes().len()),
            Value::Blob(bytes) => Ok(bytes.len()),
        };
    }

    fn get_data_type(self: &Self) -> ValueDataType {
        return match self {
            Value::String(_) => ValueDataType::String,
            Value::Integer(_) => ValueDataType::Integer,
            Value::Blob(_) => ValueDataType::Blob,
        };
    }
}

pub struct MemKvPage {
    path: PathBuf,
    mmap: MmapMut,
    index: HashMap<String, u64>,
    deleted_entries: BinaryHeap<MemKvPageGap>,
    offset: u64,
}

struct MemKvPageEntry {
    header: MemKvPageEntryHeader,
    key: String,
    value: Value,
    value_data: Vec<u8>,
}

impl MemKvPageEntry {
    fn new(
        offset: u64,
        key: &str,
        value: Value,
        value_data_type: ValueDataType,
    ) -> Result<MemKvPageEntry, Box<dyn error::Error>> {
        let value_data = match value.clone() {
            Value::String(text) => Vec::from(text.as_bytes()),
            Value::Integer(number) => Vec::from(number.to_be_bytes()),
            Value::Blob(bytes) => bytes,
        };

        return Ok(MemKvPageEntry {
            header: MemKvPageEntryHeader::new(offset, key, &value_data, value_data_type),
            key: String::from(key),
            value,
            value_data,
        });
    }
}

#[derive(Clone)]
struct MemKvPageEntryHeader {
    data_type: ValueDataType,
    flags: u8, // Flags are currently only used to marked deleted entries with 0x1
    key_size: u64,
    value_size: u64,
    offset: u64,
}

impl MemKvPageEntryHeader {
    fn get_absolute_data_offset(self: &Self) -> u64 {
        return self.offset + (size_of::<u8>() * 2) as u64 + (size_of::<usize>() * 2) as u64;
    }

    fn get_entry_size(self: &Self) -> u64 {
        return self.key_size
            + self.value_size
            + (size_of::<u8>() * 2) as u64
            + (size_of::<usize>() * 2) as u64;
    }

    fn new(
        offset: u64,
        key: &str,
        value: &[u8],
        value_data_type: ValueDataType,
    ) -> MemKvPageEntryHeader {
        return MemKvPageEntryHeader {
            offset: offset,
            flags: 0x0,
            key_size: key.len() as u64,
            value_size: value.len() as u64,
            data_type: value_data_type,
        };
    }
}

struct MemKvPageGap {
    offset: u64,
    length: u64,
}

impl MemKvPageGap {
    fn new(deleted_header: MemKvPageEntryHeader) -> MemKvPageGap {
        return MemKvPageGap {
            offset: deleted_header.offset,
            length: deleted_header.get_entry_size(),
        };
    }
}

impl Ord for MemKvPageGap {
    fn cmp(&self, other: &Self) -> Ordering {
        return self.offset.cmp(&other.offset);
    }
}

impl PartialOrd for MemKvPageGap {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.offset.cmp(&other.offset))
    }
}

impl PartialEq for MemKvPageGap {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
    }
}

impl Eq for MemKvPageGap {}

impl MemKvPage {
    pub fn new(path: &Path) -> Result<Self, Box<dyn error::Error>> {
        if Path::new(path).exists() {
            return Ok(Self::load_page_from_file(path));
        } else {
            return Self::create_page(path);
        }
    }

    fn load_page_from_file(path: &Path) -> Self {
        panic!("Not implemented")
    }

    fn create_page(path: &Path) -> Result<Self, Box<dyn error::Error>> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .expect("Error loading memory mapped file");
        f.set_len(KV_PAGE_SIZE)?;
        let maybe_mmap = panic::catch_unwind(|| {
            return unsafe { MmapMut::map_mut(&f).expect("Error creating memory map") };
        });

        return match maybe_mmap {
            Ok(mmap) => Ok(MemKvPage {
                path: PathBuf::from(path),
                mmap: mmap,
                index: HashMap::new(),
                offset: 0,
                deleted_entries: BinaryHeap::new(),
            }),
            Err(_) => {
                error!("Failed to create memory map");
                fs::remove_file(path)?;
                return Err(errors::MemmapCreationFailureError.into());
            }
        };
    }

    fn read_header(self: &Self, key: &str) -> Result<MemKvPageEntryHeader, Box<dyn error::Error>> {
        let start_offset = self.index[&String::from(key)];
        return self.read_header_from_offset(start_offset);
    }

    fn read_header_from_offset(
        self: &Self,
        start_offset: u64,
    ) -> Result<MemKvPageEntryHeader, Box<dyn error::Error>> {
        // We assume here that the data was written on the same architecture as it is being read
        let start_offset = start_offset as usize;
        let data_type_size = size_of::<u8>();
        let flags_size = size_of::<u8>();
        let key_len_size = size_of::<usize>();
        let value_len_size = size_of::<usize>();

        let mut data_type_buffer = vec![0; data_type_size];
        data_type_buffer.copy_from_slice(&self.mmap[start_offset..start_offset + data_type_size]);
        let data_type: ValueDataType =
            u8::from_be_bytes(data_type_buffer.try_into().unwrap()).try_into()?; // try into should never panic as we fix size

        let mut flags_buffer = vec![0; flags_size];
        flags_buffer.copy_from_slice(
            &self.mmap[start_offset + data_type_size..start_offset + data_type_size + flags_size],
        );
        let flags: u8 = u8::from_be_bytes(flags_buffer.try_into().unwrap());

        let mut key_len_buffer = vec![0; key_len_size];
        let mut value_len_buffer = vec![0; value_len_size];
        key_len_buffer.copy_from_slice(
            &self.mmap[start_offset + data_type_size + flags_size
                ..start_offset + data_type_size + flags_size + key_len_size],
        );
        value_len_buffer.copy_from_slice(
            &self.mmap[start_offset + data_type_size + flags_size + key_len_size
                ..start_offset + data_type_size + flags_size + key_len_size + value_len_size],
        );
        let key_size = u64::from_be_bytes(key_len_buffer.try_into().unwrap());
        let value_size = u64::from_be_bytes(value_len_buffer.try_into().unwrap());

        return Ok(MemKvPageEntryHeader {
            data_type,
            flags,
            offset: start_offset as u64,
            key_size,
            value_size,
        });
    }

    fn read_key(
        self: &Self,
        header: &MemKvPageEntryHeader,
    ) -> Result<String, Box<dyn error::Error>> {
        let header_offset = header.get_absolute_data_offset() as usize;
        let mut key_buffer = vec![0; header.key_size as usize];
        key_buffer
            .copy_from_slice(&self.mmap[header_offset..header_offset + header.key_size as usize]);
        let entry_key = String::from(str::from_utf8(&key_buffer)?);
        return Ok(entry_key);
    }

    fn read_value(
        self: &Self,
        header: &MemKvPageEntryHeader,
    ) -> Result<(Value, Vec<u8>), Box<dyn error::Error>> {
        let header_offset = header.get_absolute_data_offset() as usize;
        let mut key_buffer = vec![0; header.key_size as usize];
        let mut value_buffer = vec![0; header.value_size as usize];
        key_buffer
            .copy_from_slice(&self.mmap[header_offset..header_offset + header.key_size as usize]);
        value_buffer.copy_from_slice(
            &self.mmap[header_offset + header.key_size as usize
                ..header_offset + header.key_size as usize + header.value_size as usize],
        );
        let value = match header.data_type {
            ValueDataType::String => {
                Value::String(String::from(str::from_utf8(&value_buffer.clone())?))
            }
            ValueDataType::Integer => {
                Value::Integer(u64::from_be_bytes(value_buffer.clone().try_into().unwrap()))
            }
            ValueDataType::Blob => Value::Blob(value_buffer.clone()),
        };
        return Ok((value, value_buffer));
    }

    fn read_entry(self: &Self, key: &str) -> Result<MemKvPageEntry, Box<dyn error::Error>> {
        let header = self.read_header(key)?;

        let entry_key = self.read_key(&header)?;
        assert_eq!(key, entry_key);
        let (value, value_data) = self.read_value(&header)?;

        return Ok(MemKvPageEntry {
            header,
            key: String::from(entry_key),
            value,
            value_data,
        });
    }

    pub fn get(self: &Self, key: &str) -> Result<Value, Box<dyn error::Error>> {
        if !self.index.contains_key(&String::from(key)) {
            return Err(errors::KeyDoesNotExistError.into());
        }
        let entry = self.read_entry(key)?;

        return Ok(entry.value);
    }

    fn write_header(
        self: &mut Self,
        header: MemKvPageEntryHeader,
    ) -> Result<(), Box<dyn error::Error>> {
        // Write type
        let mut index = MemKvPage::write_to_mmap(
            &mut (self.mmap),
            header.offset as usize,
            &(header.data_type as u8).to_be_bytes(),
        )?;
        // Write flags - by default just 0x0
        index =
            MemKvPage::write_to_mmap(&mut self.mmap, index, &(header.flags as u8).to_be_bytes())?;

        // Write size of key
        index = MemKvPage::write_to_mmap(&mut self.mmap, index, &header.key_size.to_be_bytes())?;

        // Write size of value
        index = MemKvPage::write_to_mmap(&mut self.mmap, index, &header.value_size.to_be_bytes())?;

        return Ok(());
    }

    fn append_entry(self: &mut Self, entry: MemKvPageEntry) -> Result<u64, Box<dyn error::Error>> {
        return self.write_entry(entry);
    }

    fn write_entry(self: &mut Self, entry: MemKvPageEntry) -> Result<u64, Box<dyn error::Error>> {
        let data_offset = entry.header.get_absolute_data_offset() as usize;
        self.write_header(entry.header)?;

        // Write key
        let mut index =
            MemKvPage::write_to_mmap(&mut self.mmap, data_offset, &entry.key.as_bytes())?;

        // Write value
        index = MemKvPage::write_to_mmap(&mut self.mmap, index, &entry.value_data)?;

        return Ok(index as u64);
    }

    pub fn insert(self: &mut Self, key: &str, value: Value) -> Result<(), Box<dyn error::Error>> {
        if (self.offset + value.get_bytes_length()? as u64) > KV_PAGE_SIZE {
            return Err(errors::NoSpaceLeftError.into());
        }
        if self.index.contains_key(&String::from(key)) {
            return Err(errors::KeyAlreadyExistsError.into());
        }

        self.index.insert(String::from(key), self.offset);

        let data_type = value.get_data_type();
        self.offset =
            self.append_entry(MemKvPageEntry::new(self.offset, key, value, data_type)?)?;
        self.persist();
        Ok(())
    }

    pub fn delete(self: &mut Self, key: &str) -> Result<(), Box<dyn error::Error>> {
        if !self.index.contains_key(&String::from(key)) {
            return Err(errors::KeyDoesNotExistError.into());
        }

        // Update header to write that it has been deleted
        let mut header = self.read_header(key)?;
        let entry_size = header.get_entry_size();
        if header.flags != 0x0 {
            return Err(errors::EntryAlreadyDeletedInFileError.into());
        }
        header.flags = 0x1;
        self.write_header(header.clone())?;

        self.index.remove(key);
        self.deleted_entries.push(MemKvPageGap::new(header));
        self.persist();
        return Ok(());
    }

    pub fn defrag(self: &mut Self) {
        let next_gap: Option<MemKvPageGap> = self.deleted_entries.pop();
        if next_gap.is_none() {
            println!("Nothing to delete");
            return;
        }

        let next_gap = match next_gap {
            Some(gap) => gap,
            None => panic!("unreachable match"),
        };
        println!(
            "Next gap is from {} len {}",
            next_gap.offset, next_gap.length
        );

        // On the last entry we need to do nothing just reset the offset
        if next_gap.offset + next_gap.length == self.offset {
            println!("not doing anything on last entry");
            self.offset = next_gap.offset;
            return;
        } else {
            println!("moving things aroudnd");
            // Copy values back over deleted gap
            let previous_offset = self.offset as usize;
            let new_offset = self.offset - next_gap.length;

            self.mmap.copy_within(
                (next_gap.offset + next_gap.length) as usize..self.offset as usize,
                next_gap.offset as usize,
            );
            // 0 out moved data
            (&mut self.mmap[new_offset as usize..previous_offset])
                .write_all(&vec![0; next_gap.length as usize])
                .unwrap();

            let mut entry_update_offset = next_gap.offset;
            // Update indices
            // todo(@koogle): Rewrite to perform defrag one entry at a time

            while let Ok(header) = self.read_header_from_offset(entry_update_offset) {
                let key = self.read_key(&header).unwrap();
                *self.index.get_mut(&key).unwrap() = header.offset;
                entry_update_offset += header.get_entry_size()
            }

            self.persist();
            self.offset = new_offset;
        }
    }

    fn delete_page(self: &mut Self, delete_file: bool) -> Result<(), io::Error> {
        self.index.drain();
        self.offset = 0;
        self.persist();
        if delete_file {
            fs::remove_file(self.path.clone())?;
        }
        return Ok(());
    }

    fn persist(self: &Self) {
        // Flush entire map
        self.mmap.flush().unwrap();
    }

    fn write_to_mmap(mmap: &mut MmapMut, offset: usize, data: &[u8]) -> Result<usize, io::Error> {
        let data_size = data.len();
        (&mut mmap[offset..offset + data_size]).write_all(&data)?;

        return Ok(offset + data_size);
    }
}

#[cfg(test)]
mod tests {
    use super::{MemKvPage, Value};
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::fs;
    use std::panic;
    use std::path::Path;

    const TEST_KEYSPACE: &str = "test_keyspace";

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Person {
        name: String,
        age: u8,
        phones: Vec<String>,
    }

    fn run_test<T>(test: T) -> ()
    where
        T: FnOnce() -> () + panic::UnwindSafe,
    {
        setup();

        let result = panic::catch_unwind(|| test());

        teardown();

        assert!(result.is_ok())
    }

    fn setup() {
        if Path::new(TEST_KEYSPACE).exists() {
            fs::remove_file(TEST_KEYSPACE).unwrap();
        }
    }

    fn teardown() {
        if Path::new(TEST_KEYSPACE).exists() {
            fs::remove_file(TEST_KEYSPACE).unwrap();
        }
    }

    #[test]
    fn test_put_and_get() {
        run_test(|| {
            let mut kvmap = MemKvPage::new(Path::new(TEST_KEYSPACE)).unwrap();
            kvmap
                .insert("albert", Value::String(String::from("value")))
                .unwrap();
            kvmap.insert("peter", Value::Integer(123)).unwrap();
            kvmap
                .insert("tom", Value::String(String::from("my third value")))
                .unwrap();
            let person_a = Person {
                name: String::from("peter pan"),
                age: 20,
                phones: vec![],
            };
            kvmap
                .insert("dan", Value::Blob(serde_json::to_vec(&person_a).unwrap()))
                .unwrap();

            if let Value::String(value1) = kvmap.get("albert").unwrap() {
                assert_eq!(value1, "value");
            } else {
                panic!();
            }
            if let Value::Integer(value2) = kvmap.get("peter").unwrap() {
                assert_eq!(value2, 123);
            } else {
                panic!();
            }
            if let Value::String(value3) = kvmap.get("tom").unwrap() {
                assert_eq!(value3, "my third value");
            } else {
                panic!();
            }
            if let Value::Blob(value4) = kvmap.get("dan").unwrap() {
                assert_eq!(serde_json::from_slice::<Person>(&value4).unwrap(), person_a);
            } else {
                panic!("test");
            }

            assert_eq!(kvmap.offset, 157);
            assert_eq!(*kvmap.index.get("peter").unwrap(), 29);
            kvmap.delete("albert").unwrap();
            kvmap.delete("dan").unwrap();
            kvmap.defrag();
            assert_eq!(*kvmap.index.get("peter").unwrap(), 29);
            assert_eq!(kvmap.offset, 95);
            kvmap.defrag();
            assert_eq!(*kvmap.index.get("peter").unwrap(), 0);
            kvmap.defrag();
            assert_eq!(kvmap.offset, 66);
        });
    }
}
