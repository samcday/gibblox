use std::io::Cursor;

use memmap2::Mmap;

pub trait ReadCursorExt {
    fn read_cursor(&self, offset: usize) -> Option<Cursor<&[u8]>>;

    fn get_at(&self, offset: usize, size: usize) -> Option<&[u8]>;
}

impl ReadCursorExt for Mmap {
    fn read_cursor(&self, offset: usize) -> Option<Cursor<&[u8]>> {
        self.get(offset..).map(Cursor::new)
    }

    fn get_at(&self, offset: usize, size: usize) -> Option<&[u8]> {
        self.get(offset..offset + size)
    }
}
