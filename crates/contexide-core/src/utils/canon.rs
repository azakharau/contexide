use serde_json::{Map, Value};

pub fn canonicalize_value(v: &Value) -> String {
    let sorted = sort_values(v);
    serde_json::to_string(&sorted).unwrap()
}

pub fn canonicalize_str(v: &str) -> Result<String, serde_json::Error> {
    let parsed: Value = serde_json::from_str(v)?;
    Ok(canonicalize_value(&parsed))
}

fn sort_values(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut new_map = Map::with_capacity(keys.len());
            keys.into_iter().for_each(|k| {
                let value_for_sort = map.get(k).unwrap();
                new_map.insert(k.clone(), sort_values(value_for_sort));
            });
            Value::Object(new_map)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(sort_values).collect()),
        _ => v.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ignores_object_key_order() {
        let a = json!({"b":2,"a":1});
        let b = json!({"a":1,"b":2});
        assert_eq!(canonicalize_value(&a), canonicalize_value(&b));
    }

    #[test]
    fn arrays_keep_original_order() {
        let a = json!({"xs":[3,2,1]});
        let b = json!({"xs":[1,2,3]});
        assert_ne!(canonicalize_value(&a), canonicalize_value(&b));
    }

    #[test]
    fn canonicalize_str_compacts_and_sorts() {
        let src = "{\n  \"b\": 2, \n  \"a\": 1\n}";
        let got = canonicalize_str(src).unwrap();
        assert_eq!(got, "{\"a\":1,\"b\":2}");
    }
}
