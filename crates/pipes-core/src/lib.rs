//! Core of rmf-pipes: the data model shared by the bus and the API, the [`Bus`]
//! abstraction, and the topic [`Hub`]. This crate must not depend on hiroz or
//! zenoh, so that everything above the bus can be tested with
//! [`fake::FakeBus`].
//!
//! [`Bus`]: bus::Bus
//! [`Hub`]: hub::Hub

pub mod build_info;
pub mod bus;
#[cfg(any(test, feature = "fake"))]
pub mod fake;
pub mod graph;
pub mod hub;
pub mod pattern;
pub mod sample;
pub mod schema;
