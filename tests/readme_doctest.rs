//! Ensures README code blocks compile (those tagged with rust). Adjust if README changes.

use doc_comment::doctest;

// README is one level up from tests directory
doctest!("../README.md");
