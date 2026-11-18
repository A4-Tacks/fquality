use std::{io::Read, iter, path::PathBuf, process::exit, sync::{Arc, mpsc}, thread};

use fs_err::File;
use lz4_flex::block;
use window_sort_iterator::WindowSortIterExt;

fn main() {
    let options = getopts_macro::getopts_options! {
        -T, --threads=N         "set compute threads";
        -r, --recursive         "recursive directory inputs";
        -h, --help              "show help";
    };
    let mut matches = getopts_macro::simple_parse(
        &options,
        "use lz4 quick check file quality (compressibility)",
        0,
        "[FILE..]",
    );
    let jobs_limit = match matches.opt_get::<usize>("threads") {
        Ok(Some(n)) => n,
        Ok(None) => {
            std::thread::available_parallelism()
                .map_or(0, usize::from)
                .max(2) - 1
        },
        Err(e) => {
            eprintln!("invalid threads num: {e}");
            exit(2)
        },
    };
    let (send_files, files) = crossbeam_channel::bounded(64*jobs_limit);

    if matches.opt_present("recursive") {
        let files = std::mem::take(&mut matches.free);
        thread::spawn(move || {
            files.iter().flat_map(|path| {
                walkdir::WalkDir::new(path).into_iter()
                    .filter_map(Result::ok)
                    .filter(|entry| entry.file_type().is_file())
                    .map(|it| it.into_path())
            })
                .enumerate()
                .for_each(|path| send_files.send(path).unwrap());
        });
    } else {
        let files = std::mem::take(&mut matches.free);
        thread::spawn(move || {
            for path in files.into_iter().map(Into::into).enumerate() {
                send_files.send(path).unwrap();
            }
        });
    }

    let (send_processed, processed) = mpsc::channel();
    let send_processed = Arc::new(send_processed);

    thread::scope(|scope| {
        for _ in 0..jobs_limit {
            let sender = send_processed.clone();
            scope.spawn(|| {
                let input = &mut Vec::new();
                let output = &mut Vec::new();
                loop {
                    let Ok((cur, path)) = files.recv() else { break };

                    let mut file = match File::open(&path) {
                        Ok(file) => file,
                        Err(e) => {
                            eprintln!("{e}");
                            continue;
                        },
                    };
                    input.clear();
                    if let Err(e) = file.read_to_end(input) {
                        eprintln!("{e}");
                        continue;
                    }

                    let size_limit = block::get_maximum_output_size(input.len());
                    if output.len() < size_limit {
                        output.resize(size_limit, 0);
                    }
                    let output_size = match block::compress_into(input, output) {
                        Ok(n) => n as f64,
                        Err(e) => unreachable!("`{path:?}`: {e}"),
                    };

                    let quality = output_size / input.len().max(1) as f64 * 100f64;
                    sender.send(Job { cur, quality, path }).unwrap();
                }
                Arc::into_inner(sender);
            });
        }

        drop(send_processed);

        iter::from_fn(|| processed.recv().ok())
            .window_sort(jobs_limit * 4)
            .for_each(|Job { cur: _, quality, path }| {
                println!("{quality:7.3}  {}", path.display())
            });
    });
}

struct Job {
    cur: usize,
    quality: f64,
    path: PathBuf,
}

impl Eq for Job {}

impl PartialEq for Job {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Ord for Job {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.cur.cmp(&self.cur)
    }
}

impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
