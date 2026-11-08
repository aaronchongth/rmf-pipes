//! The bus: rmf-pipes' only contact point with hiroz and zenoh.
//!
//! Invariant: no other crate in the workspace (except the test kit) imports
//! hiroz or zenoh. Everything above this crate talks to the traits and types in
//! `pipes-core`, which keeps hiroz upgrades contained and lets the API be tested
//! without a zenoh session.

pub mod distro;
