use pyo3::prelude::*;
use decider::init_decider;
use decider::Decider;
use decider::Context;
use decider::Decision;


#[pyclass]
pub struct PyDecider {
    inner: Decider,
}

#[pyclass]
pub struct PyContext {
    inner: Context,
}

#[pyclass]
pub struct PyDecision {
    inner: Option<Decision>,
}

#[pymethods]
impl PyDecider {
    pub fn printer(&self) {
        println!("yooo");
    }

    pub fn choose(&self, feature_name: String, ctx: &PyContext) -> Option<PyDecision> {
        let result = self.inner.choose(feature_name.to_string(), &ctx.inner);

        return match result {
            Ok(res) => Some(PyDecision{inner : res}),
            Err(_e) => None, 
        }
    }
}

#[pyfunction]
pub fn init(decisionmakers: String, filename: String) -> Option<PyDecider> {
    let d = init_decider(
        decisionmakers.to_string(),
        filename.to_string(),
    );
    
    return match d {
        Ok(dec) => Some(PyDecider{inner : dec}),
        Err(_e) => None,
    }
}

#[pymodule]
fn rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyDecider>()?;
    m.add_function(wrap_pyfunction!(init, m)?)?;    

    Ok(())
}
