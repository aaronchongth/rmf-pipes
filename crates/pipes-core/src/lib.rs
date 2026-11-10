//! Core of rmf-pipes: the data model shared by the bus and the API, the [`Bus`]
//! abstraction, the topic [`Hub`] and the [`Recorder`]. This crate must not depend on hiroz or
//! zenoh, so that everything above the bus can be tested with
//! [`fake::FakeBus`].
//!
//! [`Bus`]: bus::Bus
//! [`Hub`]: hub::Hub
//! [`Recorder`]: recorder::Recorder

pub mod build_info;
pub mod bus;
#[cfg(any(test, feature = "fake"))]
pub mod fake;
pub mod graph;
pub mod hub;
pub mod pattern;
pub mod record;
pub mod recorder;
pub mod sample;
pub mod schema;
