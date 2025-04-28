use crate::genmsg;
use proc_macro::TokenStream;
use quote::quote;
use std::env;
use std::path::Path;

pub fn depend_on_messages(
    messages: &[&str],
    internal: bool,
    ignore_bad_messages: bool,
) -> TokenStream {
    let paths = gen_paths();
    let paths_slices : Vec<&str> = paths.iter().map(String::as_str).collect();
    let output = genmsg::depend_on_messages(ignore_bad_messages, paths_slices.as_slice(), messages)
        .unwrap_or_else(|r| panic!("{}", r))
        .token_stream(&if internal {
            quote! { crate:: }
        } else {
            quote! { rosrust:: }
        });
    (quote! {#output}).into()
}

pub fn gen_paths() -> Vec<String> {
    let cmake_paths = env::var("CMAKE_PREFIX_PATH")
        .unwrap_or_default()
        .split(':')
        .filter_map(append_share_folder)
        .collect::<Vec<String>>();
    let cmake_alt_paths = env::var("CMAKE_PREFIX_PATH")
        .unwrap_or_default()
        .split(':')
        .filter_map(append_src_folder)
        .collect::<Vec<String>>();
    let ros_package_paths = env::var("ROS_PACKAGE_PATH")
        .unwrap_or_default()
        .split(':')
        .map(String::from)
        .collect::<Vec<String>>();
    let extra_paths = env::var("ROSRUST_MSG_PATH")
        .unwrap_or_default()
        .split(':')
        .map(String::from)
        .collect::<Vec<String>>();
    cmake_paths
        .into_iter()
        .chain(cmake_alt_paths.into_iter())
        .chain(ros_package_paths.into_iter())
        .chain(extra_paths.into_iter())
        .collect::<Vec<String>>()
}

fn append_share_folder(path: &str) -> Option<String> {
    Path::new(path).join("share").to_str().map(String::from)
}

fn append_src_folder(path: &str) -> Option<String> {
    Path::new(path)
        .join("..")
        .join("src")
        .to_str()
        .map(String::from)
}
