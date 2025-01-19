
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use pyo3::types::PyModule;

mod builder;

pub fn register_module(parent_module: &Bound<'_, PyModule>) -> PyResult<()> {
    let m = PyModule::new_bound(parent_module.py(), "nicesigner")?;
    m.add_function(wrap_pyfunction!(builder::build_transaction, &m)?)?;
    parent_module.add_submodule(&m)?;
    Ok(())
}


#[pymodule]
fn nicesigner(m: &Bound<'_, PyModule>) -> PyResult<()> {
    register_module(m)?;
    Ok(())
}