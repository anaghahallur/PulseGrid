# Agent Rules & Architectural Constraints

All AI agents and contributors working in this repository MUST adhere to the following rules:

---

## 1. Clinical Reality & Provenance Rules
- **NEVER claim clinical validity:** All models, metrics, and dashboards are evaluated strictly on synthetic ADT event simulations. They establish engineering and ML pipeline validity, NOT real-world medical safety.
- **Always label synthetic data:** All public endpoints, dashboard screens, and reports must clearly designate results as synthetic demonstrations.
- **Zero Secrets / Protected Health Information (PHI):** Never commit real patient records, PhysioNet credentials, `.env` secrets, or raw clinical datasets.

---

## 2. Machine Learning & Feature Integrity
- **Zero Lookahead / Future Leakage:** All feature transformations must strictly leverage past observations ($L_0 \dots L_{168}$) and left-closed rolling windows. Never shift negative numbers into features.
- **Chronological Boundary Isolation:** Always use chronological splits with a **24-hour target buffer gap** between train, validation, and test sets.
- **Never Tune Against Held-Out Test Set:** Model selection must be conducted strictly on the validation partition. The test partition must remain untouched.
- **Never Silently Zero-Fill Missing Observations:** If a historical gap is detected in inference inputs, the service must reject the request or issue an explicit warning/fallback. Do not impute zeros.
- **Preserve Baselines:** Always evaluate models against `PersistenceBaseline` ($y_{t+h} = y_t$) and `SeasonalNaiveBaseline`. If a candidate loses to persistence, report it honestly.

---

## 3. Streaming & Big Data Integrity
- **Idempotent Storage:** Raw events persisted to HDFS must use atomic rename patterns. Offsets in Kafka must only be committed AFTER successful persistence.
- **Double-Counting Prevention:** An encounter can only occupy one bed at any given hourly snapshot.
- **Unit Boundary Isolation:** Feature generation must group by `unit_id` so that shifts and rolling windows never bleed between distinct hospital wards.

---

## 4. Software Quality & Testing Standards
- **Never Delete Existing Tests or Weaken Assertions:** When adding new features, add corresponding tests; never silence existing test failures.
- **Verify Live Containers Before Integration Tests:** Ensure Docker containers (`icu-zookeeper`, `icu-kafka`, `icu-namenode`, `icu-datanode`) are healthy before running integration suites.
- **Run `graphify update .` after changes:** Keep the AST knowledge graph updated after modifying source files.
