import sqlite3


class Database:
    def __init__(self, db_file: str):
        self.conn = sqlite3.connect(db_file)
        self.create_files_table()

    def create_files_table(self):
        self.conn.execute(
            """CREATE TABLE IF NOT EXISTS files
            (filename TEXT PRIMARY KEY NOT NULL,
            status TEXT NOT NULL);"""
        )

    def get_processed_files(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT filename FROM files WHERE status = 'processed'")
        return [row[0] for row in cursor.fetchall()]

    def lock_file(self, filename):
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO files VALUES (?, 'processing')", (filename,)
        )
        self.conn.commit()

    def save_processed_file(self, filename):
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE files SET status = 'processed' WHERE filename = ?", (filename,)
        )
        self.conn.commit()

    def revert_file_status(self, filename):
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE files SET status = 'unprocessed' WHERE filename = ?", (filename,)
        )
        self.conn.commit()

    def get_file_status(self, filename):
        cursor = self.conn.cursor()
        cursor.execute("SELECT status FROM files WHERE filename = ?", (filename,))
        row = cursor.fetchone()
        if row is not None:
            return row[0]
        return None
