// use log::LevelFilter;
// use log4rs::append::console::ConsoleAppender;
// use log4rs::append::file::FileAppender;
// use log4rs::config::{Appender, Config, Root};
// use log4rs::encode::pattern::PatternEncoder;
use std::path::Path;


#[cfg(feature = "debug")]
pub fn init_log<P: AsRef<Path>>(log_path: P) {
    // let file = FileAppender::builder()
    //     .encoder(Box::new(PatternEncoder::new(
    //         "{d(%Y-%m-%d %H:%M:%S)}|{l}|{m}|{n}",
    //     )))
    //     .build(log_path)
    //     .unwrap();

    // let config = Config::builder()
    //     .appender(Appender::builder().build("file", Box::new(file)))
    //     .build(
    //         Root::builder()
    //             .appender("file")
    //             .build(LevelFilter::Debug),
    //     )
    //     .unwrap();

    // let _ = log4rs::init_config(config).unwrap();
}

#[cfg(not(feature = "debug"))]
pub fn init_log<P: AsRef<Path>>(log_path: P) {
    // let file = FileAppender::builder()
    //     .encoder(Box::new(PatternEncoder::new(
    //         "{d(%Y-%m-%d %H:%M:%S)}|{l}|{m}|{n}",
    //     )))
    //     .build(log_path)
    //     .unwrap();

    // let config = Config::builder()
    //     .appender(Appender::builder().build("file", Box::new(file)))
    //     .build(Root::builder().appender("file").build(LevelFilter::Info))
    //     .unwrap();
}