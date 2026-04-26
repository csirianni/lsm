use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};

use crate::memtable::Memtable;

// Knows about all the segments (SSTables) on disk.
pub struct SegmentManager {
    // TODO: Need an ordered list of segment_ids or really just fds
}

/// Sorted Strings Table
pub struct SSTable {
    // Sparse index mapping string keys to a byte offset representing the start of a block.
    index: BTreeMap<String, usize>,
    // TODO: Store fd as a field?
}

impl SSTable {
    pub fn new() -> Self {
        Self {
            index: BTreeMap::new(),
        }
    }

    // Binary file format
    //
    // Data blocks
    // ============
    // key_len: u32
    // key: [u8]
    // value_len: u32
    // value: [u8]
    //
    // Index blocks
    // ============
    // key_len: u32
    // key: [u8]
    // offset: u64
    //
    // Footer
    // ===========
    // index_offset: u64
    pub fn flush<W: Write>(
        &mut self,
        file: &mut W,
        memtable: &Memtable,
    ) -> Result<(), std::io::Error> {
        let mut offset = 0;
        for (key, value) in memtable.iter() {
            file.write_all(&(key.len() as u32).to_le_bytes())?;
            file.write_all(&(key.as_bytes()))?;
            file.write_all(&(value.len() as u32).to_le_bytes())?;
            file.write_all(&(value.as_bytes()))?;

            self.index.insert(key.clone(), offset);
            offset += size_of::<u32>() + key.len() + size_of::<u32>() + value.len();
        }

        for (key, value) in self.index.iter() {
            file.write_all(&(key.len() as u32).to_le_bytes())?;
            file.write_all(&(key.as_bytes()))?;
            file.write_all(&(*value as u64).to_le_bytes())?;
        }

        // TODO: Assert that offset == memtable.size(). The issue is that each data block contains 8
        // extra bytes for the len fields, so we need to avoid for that in our arithmetic.
        file.write_all(&(offset as u64).to_le_bytes())?;
        // TODO: Store size of index here and pre-allocate in load_index().

        Ok(())
    }

    pub fn load_index<R: Read + Seek>(
        &mut self,
        file: &mut R,
        file_len: usize,
    ) -> Result<(), std::io::Error> {
        file.seek(SeekFrom::End(-(size_of::<u64>() as i64)))?;
        let index_offset = {
            let mut buffer = [0u8; size_of::<u64>()];
            file.read_exact(&mut buffer)?;
            u64::from_le_bytes(buffer)
        };

        file.seek(SeekFrom::Start(index_offset))?;

        let end = file_len - size_of::<u64>();

        while (file.stream_position()? as usize) < end {
            let key_len = {
                let mut buffer = [0u8; size_of::<u32>()];
                file.read_exact(&mut buffer)?;
                u32::from_le_bytes(buffer)
            };

            let mut buf = vec![0; key_len as usize];
            file.read_exact(&mut buf)?;
            let key = String::from_utf8(buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            let key_offset = {
                let mut buffer = [0u8; size_of::<u64>()];
                file.read_exact(&mut buffer)?;
                u64::from_le_bytes(buffer)
            };

            // TODO: Should the offset be u32, u64, or usize?
            self.index.insert(key, key_offset as usize);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Setting up a real File is much slower than an in-memory buffer, so we use a cursor instead.
    use std::io::Cursor;

    #[test]
    fn test_flush() {
        let mut memtable = Memtable::new();
        // TODO: Update test to use key/value with different lengths.
        assert!(memtable.insert("foo", "bar").is_ok());
        assert!(memtable.insert("aaa", "bbb").is_ok());

        let mut data: Vec<u8> = Vec::new();
        let mut sstable = SSTable::new();

        assert!(sstable.flush(&mut data, &mut memtable).is_ok());

        let expected_data_block_length = size_of::<u32>() + 3 + size_of::<u32>() + 3;
        let expected_index_block_length = size_of::<u32>() + 3 + size_of::<u64>();
        let expected_footer_length = size_of::<u64>();
        let expected_length = expected_data_block_length * 2
            + expected_index_block_length * 2
            + expected_footer_length;

        assert_eq!(data.len(), expected_length);

        // Data blocks.
        let mut offset = 0;
        assert_eq!(&data[offset..size_of::<u32>()], &(3u32).to_le_bytes());

        offset += size_of::<u32>();
        assert_eq!(&data[offset..offset + 3], "aaa".as_bytes());

        offset += 3;
        assert_eq!(
            &data[offset..offset + size_of::<u32>()],
            &(3u32).to_le_bytes()
        );

        offset += size_of::<u32>();
        assert_eq!(&data[offset..offset + 3], "bbb".as_bytes());

        offset += 3;
        assert_eq!(
            &data[offset..offset + size_of::<u32>()],
            &(3u32).to_le_bytes()
        );

        offset += size_of::<u32>();
        assert_eq!(&data[offset..offset + 3], "foo".as_bytes());

        offset += 3;
        assert_eq!(
            &data[offset..offset + size_of::<u32>()],
            &(3u32).to_le_bytes()
        );

        offset += size_of::<u32>();
        assert_eq!(&data[offset..offset + 3], "bar".as_bytes());

        // Index blocks.
        offset += 3;
        assert_eq!(
            &data[offset..offset + size_of::<u32>()],
            &(3u32).to_le_bytes()
        );

        offset += size_of::<u32>();
        assert_eq!(&data[offset..offset + 3], "aaa".as_bytes());

        offset += 3;
        assert_eq!(
            &data[offset..offset + size_of::<u64>()],
            &(0u64).to_le_bytes()
        );

        offset += size_of::<u64>();
        assert_eq!(
            &data[offset..offset + size_of::<u32>()],
            &(3u32).to_le_bytes()
        );

        offset += size_of::<u32>();
        assert_eq!(&data[offset..offset + 3], "foo".as_bytes());

        offset += 3;
        assert_eq!(
            &data[offset..offset + size_of::<u64>()],
            &(size_of::<u32>() + 3 + size_of::<u32>() + 3).to_le_bytes()
        );

        // Footer.
        offset += size_of::<u64>();
        assert_eq!(
            &data[offset..offset + size_of::<u64>()],
            &(28u64).to_le_bytes()
        );
    }

    #[test]
    fn test_load_index() {
        let mut data: Vec<u8> = Vec::new();

        let key1 = "foo";
        data.extend(&(key1.len() as u32).to_le_bytes());
        data.extend(key1.as_bytes());
        data.extend(123u64.to_le_bytes());

        let key2 = "bar";
        data.extend(&(key2.len() as u32).to_le_bytes());
        data.extend(key2.as_bytes());
        data.extend(456u64.to_le_bytes());

        // Index offset.
        data.extend(0u64.to_le_bytes());

        let mut cursor = Cursor::new(&data);
        let mut sstable = SSTable::new();
        assert!(sstable.load_index(&mut cursor, data.len()).is_ok());
    }
}
