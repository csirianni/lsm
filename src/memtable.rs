use skiplist::SkipMap;

pub struct Memtable {
    entries: SkipMap<String, String>,
    size: usize,
}

#[derive(Debug, PartialEq)]
pub enum MemtableError {
    DuplicateKey,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            entries: SkipMap::new(),
            size: 0,
        }
    }

    pub fn insert(&mut self, key: &str, value: &str) -> Result<(), MemtableError> {
        self.entries
            .try_insert(key.to_owned(), value.to_owned())
            .map_err(|_| MemtableError::DuplicateKey)?;
        self.size += key.len() + value.len();
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.entries.get(key).cloned()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.entries.iter()
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let memtable = Memtable::new();
        assert_eq!(memtable.size(), 0);
        assert_eq!(memtable.get("foo"), Option::None);
    }

    #[test]
    fn test_duplicate_key() {
        let mut memtable = Memtable::new();

        assert!(memtable.insert("foo", "bar").is_ok());
        assert_eq!(memtable.get("foo"), Some(String::from("bar")));
        assert_eq!(
            memtable.insert("foo", "bar"),
            Err(MemtableError::DuplicateKey)
        );
    }

    #[test]
    fn test_multiple_inserts() {
        let mut memtable = Memtable::new();
        assert!(memtable.insert("foo1", "bar1").is_ok());
        assert!(memtable.insert("foo2", "bar2").is_ok());
        assert!(memtable.insert("foo3", "bar3").is_ok());

        assert_eq!(memtable.get("foo1"), Some(String::from("bar1")));
        assert_eq!(memtable.get("foo2"), Some(String::from("bar2")));
        assert_eq!(memtable.get("foo3"), Some(String::from("bar3")));
    }

    #[test]
    fn test_size() {
        let mut memtable = Memtable::new();

        assert!(memtable.insert("foo", "bar").is_ok());
        let initial_size = "foo".len() + "bar".len();
        assert_eq!(memtable.size(), initial_size);

        assert!(memtable.insert("foo", "bar").is_err());
        assert_eq!(memtable.size(), initial_size);

        assert!(memtable.insert("a", &"b".repeat(100)).is_ok());
        assert_eq!(memtable.size(), initial_size + 1 + 100);
    }

    #[test]
    fn test_iter() {
        let mut memtable = Memtable::new();

        for i in 0..5 {
            assert!(
                memtable
                    .insert(&format!("foo{}", i), &format!("bar{}", i))
                    .is_ok()
            );
        }

        for (i, (key, value)) in memtable.iter().enumerate() {
            assert_eq!(*key, format!("foo{}", i));
            assert_eq!(*value, format!("bar{}", i));
        }
    }
}
