use std::path::{Path, PathBuf};

use rocksdb::{
    backup::{BackupEngine, BackupEngineOptions, RestoreOptions},
    Env, DB,
};

pub struct DBPath {
    //dir: tempfile::TempDir, // kept for cleaning up during drop
    path: PathBuf,
}

/// Convert a DBPath ref to a Path ref.
/// We don't implement this for DBPath values because we want them to
/// exist until the end of their scope, not get passed in to functions and
/// dropped early.
impl AsRef<Path> for &DBPath {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl DBPath {
    /// Produces a fresh (non-existent) temporary path which will be DB::destroy'ed automatically.
    pub fn new(prefix: &str) -> DBPath {
        // let dir = tempfile::Builder::new()
        //     .prefix(prefix)
        //     .tempdir()
        //     .expect("Failed to create temporary path for db.");
        // let path = dir.path().join("db");

        // DBPath { dir, path }
        let mut path = PathBuf::new();
        path.push("target");
        path.push("rocksdb_backup");
        path.push(prefix);
        _ = std::fs::create_dir_all(&path).unwrap();
        path.push("db");
        
        DBPath { path }
    }
}

// impl Drop for DBPath {
//     fn drop(&mut self) {
//         let opts = Options::default();
//         DB::destroy(&opts, &self.path).expect("Failed to destroy temporary DB");
//     }
// }

fn main() {
    _ = std::fs::create_dir_all("target/rocksdb_backup").unwrap();
    // create backup
    let db_path = DBPath::new("main");
    let restore_dest = DBPath::new("restore");
    {
        let db = DB::open_default(&db_path.path).unwrap();
        assert!(db.put(b"k1", b"v1111").is_ok());
        let value = db.get(b"k1");
        assert_eq!(value.unwrap().unwrap(), b"v1111");
        {
            let backup_path = DBPath::new("backup");
            println!("Backing up to {}", backup_path.path.display());
            let env = Env::new().unwrap();
            let backup_opts = BackupEngineOptions::new(&backup_path).unwrap();
            let mut backup_engine = BackupEngine::open(&backup_opts, &env).unwrap();
            //assert!(backup_engine.create_new_backup(&db).is_ok());
            assert!(backup_engine.create_new_backup_flush(&db, true).is_ok());

            // check backup info
            let info = backup_engine.get_backup_info();
            assert!(!info.is_empty());
            info.iter().for_each(|i| {
                assert!(backup_engine.verify_backup(i.backup_id).is_ok());
                assert!(i.size > 0);
            });
            
            println!("Restoring to {}", restore_dest.path.display());

            let mut restore_option = RestoreOptions::default();
            restore_option.set_keep_log_files(false); // true to keep log files
            let restore_status = backup_engine.restore_from_latest_backup(
                &restore_dest.path,
                &restore_dest.path,
                &restore_option,
            );
            assert!(restore_status.is_ok());

            let db_restore = DB::open_default(&restore_dest.path).unwrap();
            let value = db_restore.get(b"k1");
            assert_eq!(value.unwrap().unwrap(), b"v1111");
            
            println!("Restored to {}", restore_dest.path.display());
        }
    }
}