//! Query planning and optimization.
//!
//! This module provides cost-based query planning that transforms SQL AST into
//! optimized physical execution plans.

mod error;
mod logical;
mod optimizer;
mod physical;
mod planner;

pub use error::{PlanError, PlanResult};
pub use logical::{LogicalPlan, JoinType};
pub use optimizer::{Optimizer, OptimizationRule};
pub use physical::{PhysicalPlan, PhysicalOperator};
pub use planner::QueryPlanner;
