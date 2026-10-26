//! Multi-line line editor with display-width aware cursor layout.

use unicode_width::UnicodeWidthChar;

use super::text::str_width;

#[derive(Debug, Default, Clone)]
pub struct InputBuffer {
    text: String,
    /// Byte offset, always on a char boundary.
    cursor: usize,
}

impl InputBuffer {
    pub fn text(&self) -> &str {
        &self.text
    }
    pub fn is_empty(&self) -> bool {
        self.text.is_empty()
    }
    pub fn is_multiline(&self) -> bool {
        self.text.contains('\n')
    }
    pub fn clear(&mut self) {
        self.text.clear();
        self.cursor = 0;
    }
    pub fn set(&mut self, s: &str) {
        self.text = s.to_string();
        self.cursor = self.text.len();
    }
    pub fn insert_char(&mut self, c: char) {
        self.text.insert(self.cursor, c);
        self.cursor += c.len_utf8();
    }
    pub fn insert_str(&mut self, s: &str) {
        let s = s.replace("\r\n", "\n").replace('\r', "\n");
        self.text.insert_str(self.cursor, &s);
        self.cursor += s.len();
    }
    pub fn backspace(&mut self) {
        if let Some(c) = self.text[..self.cursor].chars().next_back() {
            self.cursor -= c.len_utf8();
            self.text.remove(self.cursor);
        }
    }
    pub fn delete(&mut self) {
        if self.cursor < self.text.len() {
            self.text.remove(self.cursor);
        }
    }
    pub fn left(&mut self) {
        if let Some(c) = self.text[..self.cursor].chars().next_back() {
            self.cursor -= c.len_utf8();
        }
    }
    pub fn right(&mut self) {
        if let Some(c) = self.text[self.cursor..].chars().next() {
            self.cursor += c.len_utf8();
        }
    }
    fn line_start(&self) -> usize {
        self.text[..self.cursor]
            .rfind('\n')
            .map(|i| i + 1)
            .unwrap_or(0)
    }
    fn line_end(&self) -> usize {
        self.text[self.cursor..]
            .find('\n')
            .map(|i| self.cursor + i)
            .unwrap_or(self.text.len())
    }
    pub fn home(&mut self) {
        self.cursor = self.line_start();
    }
    pub fn end(&mut self) {
        self.cursor = self.line_end();
    }

    fn col_in_line(&self) -> usize {
        str_width(&self.text[self.line_start()..self.cursor])
    }
    fn place_in_line(&mut self, start: usize, end: usize, col: usize) {
        let mut w = 0;
        let mut pos = start;
        for c in self.text[start..end].chars() {
            let cw = c.width().unwrap_or(0);
            if w + cw > col {
                break;
            }
            w += cw;
            pos += c.len_utf8();
        }
        self.cursor = pos;
    }
    /// Move to the previous logical line; false if already on the first.
    pub fn up(&mut self) -> bool {
        let start = self.line_start();
        if start == 0 {
            return false;
        }
        let col = self.col_in_line();
        let prev_end = start - 1;
        let prev_start = self.text[..prev_end]
            .rfind('\n')
            .map(|i| i + 1)
            .unwrap_or(0);
        self.place_in_line(prev_start, prev_end, col);
        true
    }
    /// Move to the next logical line; false if already on the last.
    pub fn down(&mut self) -> bool {
        let end = self.line_end();
        if end >= self.text.len() {
            return false;
        }
        let col = self.col_in_line();
        let next_start = end + 1;
        let next_end = self.text[next_start..]
            .find('\n')
            .map(|i| next_start + i)
            .unwrap_or(self.text.len());
        self.place_in_line(next_start, next_end, col);
        true
    }

    /// Hard-wrap to `width` columns. Returns visual rows and the cursor
    /// (row, col) in display cells.
    pub fn layout(&self, width: usize) -> (Vec<String>, (usize, usize)) {
        let width = width.max(2);
        let mut rows = vec![String::new()];
        let mut col = 0usize;
        let mut cursor = (0usize, 0usize);
        let mut placed = false;
        for (idx, ch) in self.text.char_indices() {
            if ch == '\n' {
                if idx == self.cursor {
                    cursor = (rows.len() - 1, col);
                    placed = true;
                }
                rows.push(String::new());
                col = 0;
                continue;
            }
            let w = ch.width().unwrap_or(0);
            if col + w > width && col > 0 {
                rows.push(String::new());
                col = 0;
            }
            if idx == self.cursor {
                cursor = (rows.len() - 1, col);
                placed = true;
            }
            rows.last_mut().unwrap().push(ch);
            col += w;
        }
        if !placed {
            if col + 1 > width {
                rows.push(String::new());
                col = 0;
            }
            cursor = (rows.len() - 1, col);
        }
        (rows, cursor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edit_wide_chars() {
        let mut b = InputBuffer::default();
        for c in "あいう".chars() {
            b.insert_char(c);
        }
        b.left();
        b.backspace();
        assert_eq!(b.text(), "あう");
        b.delete();
        assert_eq!(b.text(), "あ");
        b.home();
        b.insert_char('x');
        assert_eq!(b.text(), "xあ");
        let (rows, (r, c)) = b.layout(10);
        assert_eq!(rows, vec!["xあ"]);
        assert_eq!((r, c), (0, 1));
        b.end();
        assert_eq!(b.layout(10).1, (0, 3));
    }

    #[test]
    fn cursor_wraps_with_wide_chars() {
        let mut b = InputBuffer::default();
        b.insert_str("あいうえ");
        let (rows, cur) = b.layout(4);
        assert_eq!(rows, vec!["あい", "うえ", ""]);
        // cursor after a full row moves to the next row
        assert_eq!(cur, (2, 0));
        b.left();
        assert_eq!(b.layout(4).1, (1, 2));
    }

    #[test]
    fn multiline_up_down() {
        let mut b = InputBuffer::default();
        b.insert_str("あい\nabcd");
        assert!(b.up());
        assert_eq!(b.layout(10).1, (0, 4)); // col 4 -> after "あい"
        assert!(!b.up());
        assert!(b.down());
        assert!(!b.down());
        assert_eq!(b.layout(10).1, (1, 4));
    }
}
