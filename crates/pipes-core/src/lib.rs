//! Core of rmf-pipes: the data model shared by the bus and the API, and (from
//! phase 1) the topic hub. This crate must not depend on hiroz or zenoh, so that
//! everything above the bus can be tested with an in-memory fake.

pub mod build_info;
