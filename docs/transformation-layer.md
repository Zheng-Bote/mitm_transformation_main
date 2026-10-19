# MitM Transformation Layer Guide

The Transformation Layer in the MitM Data Aggregator acts as the data manipulation and quality assurance engine. When mapping a source field to a target field, the data flows through two distinct phases: **Transformations** and **Validations**.

---

## 1. Transformations

Transformations modify, reformat, or compute data. They are executed sequentially as a "chain". The output of one transformation step becomes the input to the next step.

**Available Rules:**

- `to_upper`: Converts a string to uppercase.
- `to_lower`: Converts a string to lowercase.
- `trim_whitespace`: Removes leading and trailing whitespaces.
- `default_value`: Provides a fallback value if the field is empty or null.
- `regex_replace`: Replaces text matching a regex pattern.
- `parse_date`: Parses a date from an input format and outputs it in a new format. If `input_format` is omitted, it automatically detects common formats (e.g. `dd.mm.yyyy`, `mm/dd/yyyy`) and defaults to a `yyyy-mm-dd` output format.
- `string_split`: Splits a string and takes a specific index.
- `cast_type`: Casts data types (e.g. string to int). **Please note:** priority is given to the defined schema (mapping_target_field).

**JSON Configuration Example:**
The configuration is stored in the database as a JSON array in the `transformation_chain` column.

```json
[
  {
    "name": "parse_date",
    "parameters": {
      "input_format": "2006-01-02",
      "output_format": "02.01.2006"
    }
  },
  {
    "name": "default_value",
    "parameters": {
      "value": "01.01.1900"
    }
  },
  {
    "name": "regex_replace",
    "parameters": {
      "pattern": "[üÜ]",
      "replace": "ue"
    }
  },
  {
    "name": "string_split",
    "parameters": {
      "separator": ",",
      "index": 0
    }
  },
  {
    "name": "cast_type",
    "parameters": {
      "target_type": "int"
    }
  }
]
```

_(Explanation: The engine first tries to parse an incoming date formatted as `YYYY-MM-DD` and formats it as `DD.MM.YYYY`. If the field was originally empty, it falls back to `01.01.1900`. Then, it replaces all occurrences of "ü" or "Ü" with "ue". After that, it splits the string by comma and takes the first element. Finally, it casts the resulting value to an integer)._

_valid date formats:_

```code
	time.RFC3339,                 // e.g. "2006-01-02T15:04:05Z07:00"
	time.RFC3339Nano,             // e.g. "2006-01-02T15:04:05.999999999Z07:00"
	"2006-01-02T15:04:05Z07:00",  // ISO 8601 / RFC 3339
	"2006-01-02T15:04:05",        // ISO 8601 without timezone
	"2006-01-02 15:04:05",        // yyyy-mm-dd HH:MM:SS
	"2006-01-02 15:04:05.999999999 -0700 MST",
	"01/02/2006", // mm/dd/yyyy
	"02/01/2006", // dd/mm/yyyy
	"02-01-2006", // dd-mm-yyyy
	"01-02-2006", // mm-dd-yyyy
	"2006/01/02", // yyyy/mm/dd
	"2006-01-02", // yyyy-mm-dd
	"02.01.2006", // dd.mm.yyyy
	"02.01.06",   // dd.mm.yy
```

_escape JSON special chars_

```json
[
      {
        "name": "regex_replace",
        "parameters": {
          "pattern": "\\\\",
          "replace": "\\\\"
        }
      },
      {
        "name": "regex_replace",
        "parameters": {
          "pattern": "\"",
          "replace": "\\\""
        }
      },
      {
        "name": "regex_replace",
        "parameters": {
          "pattern": "\r",
          "replace": "\\r"
        }
      },
      {
        "name": "regex_replace",
        "parameters": {
          "pattern": "\n",
          "replace": "\\n"
        }
      },
      {
        "name": "regex_replace",
        "parameters": {
          "pattern": "\t",
          "replace": "\\t"
        }
      }
    ]
```

_(Explanation: Regarding the order: The backslash (\) must be escaped first; otherwise, the backslashes belonging to subsequent escape sequences (\", \n, \r, \t) would be escaped again in the next step.)_

---

## 2. Validations

Validations do not modify the data. Instead, they run after all transformations have finished to verify that the final value meets the required business constraints. If any validation fails, the entire record is rejected and sent to the **Dead Letter Queue (DLQ)**.

**Available Rules:**

- `not_null`: Ensures the value is neither null nor an empty string.
- `email`: Validates that the string is a properly formatted email address.
- `regex_match`: Checks if the string matches a given regular expression.
- `range_check`: Checks if a numeric value falls within a `min` and `max` boundary.
- `in_list`: Ensures the value is part of an allowed list of values.

**JSON Configuration Example:**
The configuration is stored in the database as a JSON array in the `validation_chain` column.

```json
[
  {
    "name": "not_null",
    "parameters": {}
  },
  {
    "name": "regex_match",
    "parameters": {
      "pattern": "^[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}$"
    }
  }
]
```

_(Explanation: The engine guarantees that the data is provided and ensures it strictly matches an email regex pattern. Note that the `email` rule already does this natively, this is just to demonstrate parameters)._


_check for valid German date format (dd.mm.yyyy):_
```json
[
      {
        "name": "regex_match",
        "parameters": {
          "pattern": "^(0[1-9]|[12][0-9]|3[01])\\.(0[1-9]|1[012])\\.(19|20)\\d{2}$"
        }
      }
    ]
```



## Summary of Execution Flow

1. **Source Data Extraction** -> 2. **Apply Transformation Chain** -> 3. **Apply Validation Chain** -> 4. **Map to Target Field**
