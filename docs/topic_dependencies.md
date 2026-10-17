# Topic Dependencies

## 1. The Problem: Asynchronous and Distributed Data

In the architecture of the MitM Aggregator, different source systems deliver data fragments for the same business context (a topic) independently of each other and at different times.
For example, for the Employee topic, the HR system might deliver the master data, while the Active Directory provides the IT permissions. However, the aggregator is supposed to forward this data to the target system as a common, complete dataset (Golden Record).

## 2. The Table topic_dependencies (from 008_topic_dependencies.sql)

This table serves as a configuration and rule set for when a dataset is considered "complete":

    CREATE TABLE IF NOT EXISTS topic_dependencies (
        topic VARCHAR(255) PRIMARY KEY,
        required_sources TEXT[] NOT NULL
    );

- topic: The name of the business topic (e.g., EmployeeData).
- required_sources: An array (a list) of strings that exactly defines which source systems (e.g., ['SAP_HR', 'ActiveDirectory']) must deliver data for this topic.

The transformation job reads this table (topic compared case-insensitively). `required_sources` in the job arguments overrides it; if neither is set, the job aborts.

## 3. How it Works at Runtime (Stateful Aggregation)

The Transformation Layer operates as a so-called Stateful Aggregator. The process runs in the following steps:

1. Data Receipt: Various collectors asynchronously gather data and store it as encrypted fragments in the raw_ingestion table of the Landing Zone. Each fragment has a topic, a source_system specification, and a deterministically generated correlation_id (a unique ID that connects related data fragments – e.g., a hash of the personnel number).
2. The Waiting Mode (Stateful): The Orchestrator checks the incoming fragments based on the correlation_id. Instead of immediately processing the fragment from the first system, the Orchestrator looks up in the topic_dependencies table which required_sources are strictly required for this topic.
3. The Matching: The Orchestrator basically waits until fragments for a specific correlation_id from all required_sources requested in the list have arrived in the database.
4. Merging & Transformation: As soon as the condition is met (all necessary systems have delivered):
   - The Orchestrator fetches all associated raw_ingestion fragments.
   - The encrypted payloads (payload) are decrypted using the Data Encryption Key (DEK).
   - The fragments are merged into a large JSON dataset (the Golden Record).
   - Only then are the mappings, transformations, and validation rules applied.
   - After successful processing, the final dataset is (partially) encrypted again and written to the target_fragments table.
5. Re-aggregation: if a source later delivers a new fragment for an already processed correlation_id, the required sources count as complete via their latest processed fragments. The new fragment is merged with the latest processed fragment of every other source, and the Golden Record is written again. Only fragments still present in raw_ingestion (see the maintenance retention) can be used.

### Correlation ID

The mapping of different keys (like HR.Personalnummer and AD.Mitarbeiternummer) to each other is done via a so-called Business Key, from which a UUID (the correlation_id) is deterministically calculated.

For this to work, the different source systems must logically share the same identifier value (e.g., the number "12345"). The technical implementation in code happens at the collector layer (as can be seen in the source code of the PostgreSQL collector).

Here is the exact process:

#### 1. Configuration of the Collector (business_key_column)

For each source system, the column that serves as the primary identification feature is configured in the collector.

- For the HR collector, you set e.g.: business_key_column = "Personalnummer"
- For the AD collector, you set e.g.: business_key_column = "Mitarbeiternummer"

#### 2. Reading the Key at Runtime

When the collector now reads a dataset from the source system, it dynamically extracts the value from this configured column.

- The HR system delivers a dataset with Personalnummer = "12345". The collector stores in the variable businessKey = "12345".
- The AD system delivers a dataset with Mitarbeiternummer = "12345". The collector also stores businessKey = "12345" here.

#### 3. Deterministic UUID Generation

The collector does not store "12345" directly in the database. Instead, a deterministic hash (UUID v5 / SHA-1) is generated from this businessKey. In the Go code, it looks exactly like this:

    // Generate deterministic Correlation ID
    correlationID := uuid.NewSHA1(namespaceMitM, []byte(businessKey))

The clever part: Since the base value ("12345") and the namespace (namespaceMitM) are identical in both systems, the hash algorithm generates exactly the same UUID (e.g., e3b0c442-989b-464c-86e5-123456789abc) completely independently in both different collectors.

#### 4. Decoupled Aggregation in the Database

Both collectors now write their data (the encrypted payload) as fragments into the raw_ingestion table.

The Orchestrator in the Transformation Layer now has a very easy job: It doesn't need to know at all what the IDs were called in the source systems (whether Personalnummer or Mitarbeiternummer).
It simply blindly groups the fragments by the correlation_id.
As soon as it sees that fragments from all systems required in the topic_dependencies are present for the UUID e3b0c442-..., it merges them into the Golden Record.
