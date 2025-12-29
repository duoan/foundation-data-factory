use serde_json::Value;

/// Sample is a wrapper around serde_json::Value
/// It represents a JSON object (one row of data)
#[derive(Clone, Debug)]
pub struct Sample(pub Value);

impl Sample {
    /// Create a new empty JSON object
    pub fn new() -> Self {
        Self(Value::Object(serde_json::Map::new()))
    }

    /// Create from a JSON Value
    pub fn from_value(value: Value) -> Option<Self> {
        if value.is_object() {
            Some(Sample(value))
        } else {
            None
        }
    }

    /// Convert to JSON Value
    pub fn into_value(self) -> Value {
        self.0
    }

    /// Get a reference to the inner Value
    pub fn as_value(&self) -> &Value {
        &self.0
    }

    /// Get a mutable reference to the inner Value
    pub fn as_value_mut(&mut self) -> &mut Value {
        &mut self.0
    }

    // --- getters ---
    pub fn get_str(&self, k: &str) -> Option<&str> {
        self.0.get(k)?.as_str()
    }

    pub fn get_i64(&self, k: &str) -> Option<i64> {
        self.0.get(k)?.as_i64()
    }

    pub fn get_f64(&self, k: &str) -> Option<f64> {
        self.0.get(k)?.as_f64()
    }

    pub fn get_bool(&self, k: &str) -> Option<bool> {
        self.0.get(k)?.as_bool()
    }

    pub fn get_array(&self, k: &str) -> Option<&Vec<Value>> {
        self.0.get(k)?.as_array()
    }

    pub fn get_object(&self, k: &str) -> Option<&serde_json::Map<String, Value>> {
        self.0.get(k)?.as_object()
    }

    pub fn get(&self, k: &str) -> Option<&Value> {
        self.0.get(k)
    }

    // --- setters ---
    pub fn set_str(&mut self, k: impl Into<String>, v: impl Into<String>) {
        if let Value::Object(ref mut map) = self.0 {
            map.insert(k.into(), Value::String(v.into()));
        }
    }

    pub fn set_i64(&mut self, k: impl Into<String>, v: i64) {
        if let Value::Object(ref mut map) = self.0 {
            map.insert(k.into(), Value::Number(v.into()));
        }
    }

    pub fn set_f64(&mut self, k: impl Into<String>, v: f64) {
        if let Value::Object(ref mut map) = self.0 {
            map.insert(
                k.into(),
                Value::Number(
                    serde_json::Number::from_f64(v).unwrap_or(serde_json::Number::from(0)),
                ),
            );
        }
    }

    pub fn set_bool(&mut self, k: impl Into<String>, v: bool) {
        if let Value::Object(ref mut map) = self.0 {
            map.insert(k.into(), Value::Bool(v));
        }
    }

    pub fn set_null(&mut self, k: impl Into<String>) {
        if let Value::Object(ref mut map) = self.0 {
            map.insert(k.into(), Value::Null);
        }
    }

    pub fn set_value(&mut self, k: impl Into<String>, v: Value) {
        if let Value::Object(ref mut map) = self.0 {
            map.insert(k.into(), v);
        }
    }

    pub fn remove(&mut self, k: &str) -> Option<Value> {
        if let Value::Object(ref mut map) = self.0 {
            map.remove(k)
        } else {
            None
        }
    }

    /// Deterministic "random" in [0,1)
    pub fn rand01(&self, id_key: &str, seed: u64) -> f64 {
        use xxhash_rust::xxh3::xxh3_64_with_seed;

        let id = self.get_str(id_key).unwrap_or("");
        let h = xxh3_64_with_seed(id.as_bytes(), seed);
        // top 53 bits -> f64
        let v = h >> 11;
        (v as f64) * (1.0 / ((1u64 << 53) as f64))
    }
}

impl Default for Sample {
    fn default() -> Self {
        Self::new()
    }
}
