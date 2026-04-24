ALTER TABLE notes ADD COLUMN tags VARCHAR DEFAULT '';
CREATE INDEX idx_notes_created ON notes(created_at);
