//! Shared string-to-Arrow-array parsing for inlined data.
//!
//! Both the read path (`table.rs`) and write path (`table_writer.rs`) need to
//! convert `Vec<Option<String>>` into typed Arrow arrays. This module provides
//! a single implementation parameterized by [`ParseMode`].

use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::DataType;

/// Unix epoch date (1970-01-01) for Date32/Date64 conversions.
const UNIX_EPOCH_DATE: chrono::NaiveDate = match chrono::NaiveDate::from_ymd_opt(1970, 1, 1) {
    Some(d) => d,
    None => panic!("1970-01-01 is a valid date"),
};

/// Controls how parse failures are handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseMode {
    /// Parse failures produce `null` values; unknown types fall back to UTF-8 strings.
    /// Used on the read path where lenient handling avoids query failures.
    Lenient,
    /// Parse failures return an error; unknown types return an error.
    /// Used on the write path where data integrity is critical.
    Strict,
}

/// Parse string values into a typed Arrow array.
///
/// Handles booleans, integers, floats, strings, dates, timestamps, and decimals.
/// The `mode` parameter controls whether parse failures produce nulls ([`ParseMode::Lenient`])
/// or errors ([`ParseMode::Strict`]).
pub fn parse_string_values_to_array(
    values: &[Option<String>],
    data_type: &DataType,
    mode: ParseMode,
) -> crate::Result<Arc<dyn Array>> {
    use arrow::array::*;

    macro_rules! handle_parse_error {
        ($val:expr, $type_name:expr, $builder:expr) => {
            match mode {
                ParseMode::Lenient => {
                    $builder.append_null();
                },
                ParseMode::Strict => {
                    return Err(crate::error::DuckLakeError::Internal(format!(
                        "Failed to parse inlined value '{}' as {}",
                        $val, $type_name
                    )));
                },
            }
        };
    }

    macro_rules! parse_primitive {
        ($builder_ty:ty, $values:expr) => {{
            let mut builder = <$builder_ty>::with_capacity($values.len());
            for val in $values {
                match val {
                    Some(s) => match s.parse() {
                        Ok(v) => builder.append_value(v),
                        Err(_) => {
                            handle_parse_error!(s, std::any::type_name::<$builder_ty>(), builder);
                        },
                    },
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as Arc<dyn Array>
        }};
    }

    let array: Arc<dyn Array> = match data_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(values.len());
            for val in values {
                match val {
                    Some(s) => match s.to_lowercase().as_str() {
                        "true" | "1" | "t" => builder.append_value(true),
                        "false" | "0" | "f" => builder.append_value(false),
                        _ => {
                            handle_parse_error!(s, "Boolean", builder);
                        },
                    },
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        },
        DataType::Int8 => parse_primitive!(Int8Builder, values),
        DataType::Int16 => parse_primitive!(Int16Builder, values),
        DataType::Int32 => parse_primitive!(Int32Builder, values),
        DataType::Int64 => parse_primitive!(Int64Builder, values),
        DataType::UInt8 => parse_primitive!(UInt8Builder, values),
        DataType::UInt16 => parse_primitive!(UInt16Builder, values),
        DataType::UInt32 => parse_primitive!(UInt32Builder, values),
        DataType::UInt64 => parse_primitive!(UInt64Builder, values),
        DataType::Float32 => parse_primitive!(Float32Builder, values),
        DataType::Float64 => parse_primitive!(Float64Builder, values),
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for val in values {
                match val {
                    Some(s) => builder.append_value(s),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        },
        DataType::LargeUtf8 => {
            let mut builder = LargeStringBuilder::new();
            for val in values {
                match val {
                    Some(s) => builder.append_value(s),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        },
        DataType::Date32 => {
            let mut builder = Date32Builder::with_capacity(values.len());
            for val in values {
                match val {
                    Some(s) => {
                        let epoch_days = if let Ok(date) =
                            chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                        {
                            date.signed_duration_since(UNIX_EPOCH_DATE).num_days() as i32
                        } else if let Ok(v) = s.parse::<i32>() {
                            v
                        } else {
                            match mode {
                                ParseMode::Lenient => {
                                    builder.append_null();
                                    continue;
                                },
                                ParseMode::Strict => {
                                    return Err(crate::error::DuckLakeError::Internal(format!(
                                        "Failed to parse inlined value '{}' as Date32",
                                        s
                                    )));
                                },
                            }
                        };
                        builder.append_value(epoch_days);
                    },
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as Arc<dyn Array>
        },
        DataType::Date64 => {
            let mut builder = Date64Builder::with_capacity(values.len());
            for val in values {
                match val {
                    Some(s) => {
                        let epoch_ms = if let Ok(date) =
                            chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                        {
                            date.signed_duration_since(UNIX_EPOCH_DATE).num_days() as i64
                                * 86_400_000
                        } else if let Ok(v) = s.parse::<i64>() {
                            v
                        } else {
                            match mode {
                                ParseMode::Lenient => {
                                    builder.append_null();
                                    continue;
                                },
                                ParseMode::Strict => {
                                    return Err(crate::error::DuckLakeError::Internal(format!(
                                        "Failed to parse inlined value '{}' as Date64",
                                        s
                                    )));
                                },
                            }
                        };
                        builder.append_value(epoch_ms);
                    },
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as Arc<dyn Array>
        },
        DataType::Timestamp(unit, tz) => {
            use arrow::datatypes::TimeUnit;

            /// Parse a timestamp string (ISO or epoch integer) to epoch microseconds.
            fn parse_ts_to_us(s: &str) -> Option<i64> {
                if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                    return Some(dt.and_utc().timestamp_micros());
                }
                if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
                    return Some(dt.and_utc().timestamp_micros());
                }
                s.parse::<i64>().ok()
            }

            macro_rules! build_timestamp {
                ($builder_ty:ty, $convert:expr) => {{
                    let mut builder = <$builder_ty>::with_capacity(values.len());
                    let convert_fn: fn(i64) -> crate::Result<i64> = $convert;
                    for val in values {
                        match val {
                            Some(s) => match parse_ts_to_us(s) {
                                Some(us) => builder.append_value(convert_fn(us)?),
                                None => match mode {
                                    ParseMode::Lenient => builder.append_null(),
                                    ParseMode::Strict => {
                                        return Err(crate::error::DuckLakeError::Internal(
                                            format!("Failed to parse '{}' as Timestamp", s),
                                        ));
                                    },
                                },
                            },
                            None => builder.append_null(),
                        }
                    }
                    let arr = builder.finish();
                    match tz {
                        Some(tz) => Arc::new(arr.with_timezone(tz.as_ref())) as Arc<dyn Array>,
                        None => Arc::new(arr) as Arc<dyn Array>,
                    }
                }};
            }

            match unit {
                TimeUnit::Second => {
                    build_timestamp!(TimestampSecondBuilder, |us: i64| Ok(us / 1_000_000))
                },
                TimeUnit::Millisecond => {
                    build_timestamp!(TimestampMillisecondBuilder, |us: i64| Ok(us / 1_000))
                },
                TimeUnit::Microsecond => {
                    build_timestamp!(TimestampMicrosecondBuilder, |us: i64| Ok(us))
                },
                TimeUnit::Nanosecond => {
                    build_timestamp!(TimestampNanosecondBuilder, |us: i64| {
                        us.checked_mul(1_000).ok_or_else(|| {
                            crate::error::DuckLakeError::Internal(
                                "Timestamp nanosecond overflow".into(),
                            )
                        })
                    })
                },
            }
        },
        DataType::Decimal128(precision, scale) => {
            let mut builder = Decimal128Builder::with_capacity(values.len());
            for val in values {
                match val {
                    Some(s) => match parse_decimal_string(s, *scale) {
                        Ok(i128_val) => builder.append_value(i128_val),
                        Err(_) if mode == ParseMode::Lenient => builder.append_null(),
                        Err(e) => return Err(e),
                    },
                    None => builder.append_null(),
                }
            }
            Arc::new(
                builder
                    .finish()
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| {
                        crate::error::DuckLakeError::Internal(format!(
                            "Invalid Decimal128 precision/scale: {}",
                            e
                        ))
                    })?,
            ) as Arc<dyn Array>
        },
        DataType::Decimal256(precision, scale) => {
            let mut builder = Decimal256Builder::with_capacity(values.len());
            for val in values {
                match val {
                    Some(s) => match parse_decimal_string(s, *scale) {
                        Ok(i128_val) => {
                            builder.append_value(arrow::datatypes::i256::from_i128(i128_val));
                        },
                        Err(_) if mode == ParseMode::Lenient => builder.append_null(),
                        Err(e) => return Err(e),
                    },
                    None => builder.append_null(),
                }
            }
            Arc::new(
                builder
                    .finish()
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| {
                        crate::error::DuckLakeError::Internal(format!(
                            "Invalid Decimal256 precision/scale: {}",
                            e
                        ))
                    })?,
            ) as Arc<dyn Array>
        },
        other => match mode {
            ParseMode::Lenient => {
                // Fallback: store as strings
                let mut builder = StringBuilder::new();
                for val in values {
                    match val {
                        Some(s) => builder.append_value(s),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            },
            ParseMode::Strict => {
                return Err(crate::error::DuckLakeError::UnsupportedType(format!(
                    "Unsupported data type {:?} in parse_string_values_to_array",
                    other
                )));
            },
        },
    };

    Ok(array)
}

fn parse_decimal_string(s: &str, scale: i8) -> crate::Result<i128> {
    let negative = s.starts_with('-');
    let s = if negative {
        &s[1..]
    } else {
        s
    };

    let (integer_part, frac_part) = if let Some(dot_pos) = s.find('.') {
        (&s[..dot_pos], &s[dot_pos + 1..])
    } else {
        (s, "")
    };

    let integer: i128 = if integer_part.is_empty() {
        0
    } else {
        integer_part.parse::<i128>().map_err(|_| {
            crate::error::DuckLakeError::Internal(format!(
                "Failed to parse decimal integer part '{}'",
                integer_part
            ))
        })?
    };

    let scale_u = scale.max(0) as u32;
    let frac_len = frac_part.len() as u32;
    let frac: i128 = if frac_part.is_empty() {
        0
    } else if frac_len <= scale_u {
        let frac_val: i128 = frac_part.parse::<i128>().map_err(|_| {
            crate::error::DuckLakeError::Internal(format!(
                "Failed to parse decimal fraction part '{}'",
                frac_part
            ))
        })?;
        frac_val * 10i128.pow(scale_u - frac_len)
    } else {
        // Truncate extra digits
        let truncated = &frac_part[..scale_u as usize];
        truncated.parse::<i128>().map_err(|_| {
            crate::error::DuckLakeError::Internal(format!(
                "Failed to parse decimal fraction part '{}'",
                truncated
            ))
        })?
    };

    let unscaled = integer * 10i128.pow(scale_u) + frac;
    Ok(if negative {
        -unscaled
    } else {
        unscaled
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::TimeUnit;

    #[test]
    fn test_lenient_mode_nulls_on_invalid() {
        let values = vec![Some("not_a_number".to_string())];
        let result =
            parse_string_values_to_array(&values, &DataType::Int32, ParseMode::Lenient).unwrap();
        assert!(result.is_null(0));
    }

    #[test]
    fn test_strict_mode_errors_on_invalid() {
        let values = vec![Some("not_a_number".to_string())];
        let result = parse_string_values_to_array(&values, &DataType::Int32, ParseMode::Strict);
        assert!(result.is_err());
    }

    #[test]
    fn test_strict_mode_bool_error_on_invalid() {
        let values = vec![Some("maybe".to_string())];
        let result = parse_string_values_to_array(&values, &DataType::Boolean, ParseMode::Strict);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Failed to parse"));
    }

    #[test]
    fn test_lenient_mode_bool_nulls_on_invalid() {
        let values = vec![Some("maybe".to_string())];
        let result =
            parse_string_values_to_array(&values, &DataType::Boolean, ParseMode::Lenient).unwrap();
        assert!(result.is_null(0));
    }

    #[test]
    fn test_date32_roundtrip() {
        let values = vec![Some("2024-06-15".to_string())];
        let result =
            parse_string_values_to_array(&values, &DataType::Date32, ParseMode::Strict).unwrap();
        let date_array = result.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(date_array.value(0), 19889);
    }

    #[test]
    fn test_timestamp_roundtrip() {
        let epoch_us: i64 = 1_718_451_000_000_000;
        let values = vec![Some("2024-06-15 11:30:00".to_string())];
        let result = parse_string_values_to_array(
            &values,
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            ParseMode::Strict,
        )
        .unwrap();
        let ts_array = result
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts_array.value(0), epoch_us);
    }

    #[test]
    fn test_lenient_unknown_type_falls_back_to_string() {
        let values = vec![Some("hello".to_string())];
        let result = parse_string_values_to_array(
            &values,
            &DataType::Duration(TimeUnit::Second),
            ParseMode::Lenient,
        )
        .unwrap();
        let str_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_array.value(0), "hello");
    }

    #[test]
    fn test_strict_unknown_type_errors() {
        let values = vec![Some("hello".to_string())];
        let result = parse_string_values_to_array(
            &values,
            &DataType::Duration(TimeUnit::Second),
            ParseMode::Strict,
        );
        assert!(result.is_err());
    }
}
