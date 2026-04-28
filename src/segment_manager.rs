use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;

use crate::memtable::Memtable;
use crate::sstable::SSTable;

type SegmentId = u64;

// Knows about all the segments (SSTables) on disk.
pub struct SegmentManager {
    segments: BTreeMap<SegmentId, SSTable>,
    next_segment_id: SegmentId,
    dir: PathBuf,
}

impl SegmentManager {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            segments: BTreeMap::new(),
            next_segment_id: 0,
            dir: dir,
        }
    }

    pub fn create_segment(&mut self, memtable: &Memtable) {
        let path = self.dir.join(PathBuf::from(format!(
            "segment-{}.txt",
            self.next_segment_id
        )));

        let mut file = File::create(path).unwrap();

        let mut sstable = SSTable::new();
        sstable.flush(&mut file, memtable).unwrap();

        assert!(
            self.segments
                .insert(self.next_segment_id, sstable)
                .is_none()
        );

        self.next_segment_id += 1;
    }

    pub fn get(&self, key: &str) -> Option<String> {
        // Iterate in reverse order to read more recent segments first.
        for (segment_id, sstable) in self.segments.iter().rev() {
            let path = self
                .dir
                .join(PathBuf::from(format!("segment-{}.txt", segment_id)));

            let mut file = File::open(path).unwrap();
            if let Some(value) = sstable.get(&mut file, key) {
                return Some(value);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_create_segment() {
        let dir = tempdir().unwrap();
        let mut segment_manager = SegmentManager::new(dir.path().to_path_buf());

        segment_manager.create_segment(&Memtable::new());
        assert!(std::fs::exists(dir.path().join("segment-0.txt")).unwrap());

        segment_manager.create_segment(&Memtable::new());
        assert!(std::fs::exists(dir.path().join("segment-1.txt")).unwrap());
    }

    #[test]
    fn test_get() {
        let dir = tempdir().unwrap();
        let mut segment_manager = SegmentManager::new(dir.path().to_path_buf());

        let mut memtable = Memtable::new();
        assert!(memtable.insert("foo", "bar").is_ok());

        segment_manager.create_segment(&memtable);

        assert_eq!(segment_manager.get("foo"), Some("bar".to_owned()));
        assert_eq!(segment_manager.get("bar"), None);
    }
}
