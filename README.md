🏥 Enterprise Healthcare Data Warehouse & Medallion Lakehouse Platform[!IMPORTANT]
End-to-End Hybrid Healthcare Data Engineering PipelineBuilt with Azure Data Factory (ADF), Azure Data Lake Storage (ADLS) Gen2, Azure Databricks (PySpark & Delta Live Tables), Unity Catalog, and Power BI—implementing HL7 FHIR R4 clinical interoperability and HIPAA Safe Harbor PII/PHI data protection across distributed on-premises and cloud SQL Server systems.📌 1. Project Overview & Business ProblemHealthcare organizations frequently struggle with fragmented clinical and financial data spread across legacy hospital servers and modern cloud applications. In this project, patient records, outpatient clinic visits, and insurance claims are distributed across three separate SQL Server databases (1 On-Premises and 2 Cloud-Hosted), each using incompatible table schemas, local medical codes, and unmasked Protected Health Information (PHI).This platform solves that fragmentation by engineering a single metadata-driven hybrid ingestion pipeline in Azure Data Factory and a 3-tier Medallion Lakehouse (Bronze → Silver → Gold) in Azure Databricks. The pipeline unifies clinical and billing data into canonical HL7 FHIR R4 Resources, enforces HIPAA Privacy Rule cryptographic masking, and serves a dimensional Star Schema for executive analytics in Power BI.🖼️ 2. System Architecture Diagram[!TIP]
How to Add Your Custom Architecture Image in GitHub:Save your architecture diagram image as architecture_diagram.png inside a docs/images/ folder in your GitHub repository.Keep the Markdown image line below—GitHub will automatically display your image right here!🎨 Interactive Color-Coded Architecture Flow (Rendered Natively on GitHub)flowchart LR
    subgraph SRC["🗄️ 1. Distributed SQL Server Sources"]
        S1[("🏥 On-Premises SQL Server\nInpatient EHR & Admissions")]
        S2[("☁️ Cloud SQL Server #1\nOutpatient Clinic EMR")]
        S3[("☁️ Cloud SQL Server #2\nInsurance Claims & Billing")]
    end

    subgraph ADF["⚙️ 2. Hybrid Ingestion (Azure Data Factory)"]
        SHIR["🔌 Self-Hosted IR\n(On-Prem Gateway)"]
        AIR["🌐 AutoResolve IR\n(Cloud Compute)"]
        META[("📋 SQL Metadata\nControl Table")]
        PIPE["🔄 Single Parameterized Pipeline\nLookup ➔ ForEach ➔ If Condition\n(Full Load & Incremental CDC)"]
    end

    subgraph ADLS["📦 3. Bronze Layer (ADLS Gen2)"]
        BRZ[("🟫 Raw Landing Zone\nSnappy Parquet Files\n/source/table/YYYY/MM/DD/")]
    end

    subgraph DBR["⚡ 4. Lakehouse Processing (Azure Databricks & Unity Catalog)"]
        SLV[("🥈 Silver Layer (DLT)\n• HL7 FHIR R4 Mapping\n• HIPAA SHA-256 PHI Masking\n• Quarantine Bad Records")]
        GLD[("🥇 Gold Layer (Delta Lake)\n• Star Schema Fact & Dim\n• SCD Type 2 History\n• Z-Order Optimized")]
    end

    subgraph BI["📊 5. Serving & Analytics"]
        PBI["📈 Power BI Semantic Model\n• 30-Day Readmission Rate\n• Avg Length of Stay (ALOS)\n• Claim Denial Analytics"]
    end

    S1 --> SHIR
    S2 & S3 --> AIR
    SHIR & AIR --> PIPE
    META -.->|"Dynamic JSON Params"| PIPE
    PIPE -->|"Parquet Write"| BRZ
    BRZ -->|"Auto Loader / PySpark"| SLV
    SLV -->|"MERGE INTO (SCD2)"| GLD
    GLD -->|"Databricks SQL"| PBI

    style SRC fill:#f8fafc,stroke:#475569,stroke-width:2px,color:#0f172a
    style ADF fill:#eff6ff,stroke:#2563eb,stroke-width:2px,color:#1e3a8a
    style ADLS fill:#fef3c7,stroke:#d97706,stroke-width:2px,color:#78350f
    style DBR fill:#ecfdf5,stroke:#059669,stroke-width:2px,color:#064e3b
    style BI fill:#faf5ff,stroke:#7c3aed,stroke-width:2px,color:#4c1d95
🛠️ 3. Technology Stack & Standards MatrixLayerTechnology / StandardRole in This ProjectSource DatabasesMicrosoft SQL Server (On-Premises & Cloud)Stores transactional inpatient EHR, outpatient clinic EMR, and healthcare claims data across 3 distributed locations.Hybrid ConnectivitySelf-Hosted Integration Runtime (SHIR) & AutoResolve IRBridges private on-premises hospital network traffic and public/VNet cloud endpoints securely into Azure.Orchestration & IngestionAzure Data Factory (ADF)Executes a single metadata-driven, parameterized pipeline supporting both Full Loads and Watermark/CDC Incremental Loads.Data Lake StorageADLS Gen2 (Parquet Format)Stores immutable raw extracts in compressed Snappy Parquet format with hierarchical date partitioning.Distributed ComputeAzure Databricks & Delta Live Tables (Lakeflow)Runs PySpark transformations, declarative data quality expectations, quarantine routing, and SCD Type 2 merges.Healthcare Data StandardHL7 FHIR Release 4 (R4)Harmonizes disparate source schemas into canonical Patient, Encounter, Observation, Condition, and Claim resources.Security & GovernanceUnity Catalog & HIPAA Safe HarborApplies salted SHA-256 hashing on SSN/MRN identifiers, dynamic column masking on PII/PHI, and Azure Key Vault secret management.Business IntelligenceMicrosoft Power BIConnects to Gold Delta tables via Databricks SQL to track clinical outcomes and revenue cycle KPIs.🗄️ 4. Distributed Data Sources (3 SQL Server Locations)Source SystemHosting EnvironmentIntegration RuntimeCore Tables ExtractedLoad StrategySource 1: Hospital Inpatient EHROn-Premises Data Center (SQL Server)Self-Hosted IR (SHIR)dbo.Patients, dbo.Inpatient_Encounters, dbo.ICU_Observations, dbo.DepartmentsIncremental (Encounters, Observations) & Full (Departments)Source 2: Outpatient Clinic EMRCloud SQL Server (Location A)AutoResolve IRclinic.Patient_Registry, clinic.Appointments, clinic.Prescriptions, clinic.ProvidersIncremental (Appointments, Prescriptions) & Full (Providers)Source 3: Revenue & Insurance ClaimsCloud SQL Server (Location B)AutoResolve IRbilling.Claims_Header, billing.Claims_Line_Items, billing.Payer_Master, billing.ICD10_CPT_RefIncremental (Claims_Header, Claims_Line_Items) & Full (Payer_Master, ICD10_CPT_Ref)🔄 5. Single Metadata-Driven ADF Pipeline (Full + Incremental Load)Instead of maintaining dozens of hardcoded pipelines, a single parameterized pipeline (pl_ingest_healthcare_sources) dynamically ingests all tables across all three SQL Server instances.flowchart TD
    A(["🚀 Pipeline Trigger"]) --> B["🔍 Lookup Activity\nQuery control.pipeline_metadata\nWHERE IsActive = 1"]
    B --> C["🔁 ForEach Activity\nIterate over @activity('Lookup').output.value"]
    C --> D{"⚖️ If Condition\n@equals(item().LoadType, 'Incremental')"}
    
    D -->|"True (Incremental)"| E["⏱️ Capture Upper-Bound Timestamp\n& Run Parameterized Copy Activity\nWHERE ModifiedDate > LastWatermark\nAND ModifiedDate <= CurrentMaxWatermark"]
    E --> F["💾 Stored Procedure Activity\nsp_UpdateWatermark\nUpdate LastWatermarkValue in Control Table"]
    
    D -->|"False (Full Load)"| G["📥 Full Copy Activity\nSELECT * FROM SourceTable\nOverwrite Bronze Parquet Partition"]
    
    F & G --> H(["✅ Landed in ADLS Gen2 Bronze (Parquet)"])

    style A fill:#dbeafe,stroke:#1d4ed8,color:#1e3a8a
    style D fill:#fef9c3,stroke:#ca8a04,color:#713f12
    style E fill:#dcfce7,stroke:#15803d,color:#14532d
    style G fill:#f3e8ff,stroke:#7e22ce,color:#4c1d95
    style H fill:#ffedd5,stroke:#c2410c,color:#7c2d12
📋 SQL Metadata Control Table SchemaCREATE TABLE control.pipeline_metadata (
    MetadataID          INT IDENTITY(1,1) PRIMARY KEY,
    SourceSystemName    VARCHAR(50)   NOT NULL, -- 'OnPrem_EHR', 'Cloud_Clinic', 'Cloud_Claims'
    SecretScopeKeyName  VARCHAR(100)  NOT NULL, -- Key Vault secret name for connection string
    IntegrationRuntime  VARCHAR(50)   NOT NULL, -- 'SHIR-OnPrem-Prod' or 'AutoResolveIntegrationRuntime'
    SourceSchema        VARCHAR(50)   NOT NULL,
    SourceTable         VARCHAR(100)  NOT NULL,
    LoadType            VARCHAR(20)   NOT NULL, -- 'Full' or 'Incremental'
    WatermarkColumn     VARCHAR(50)   NULL,     -- 'LastModifiedDate' or 'UpdatedAt'
    LastWatermarkValue  DATETIME2     DEFAULT '1900-01-01T00:00:00',
    BronzeSinkPath      VARCHAR(255)  NOT NULL,
    IsActive            BIT           DEFAULT 1
);
⚡ Key Production Ingestion EnhancementsUpper-Bound Watermark Windowing: Captures CurrentMaxWatermark prior to extraction (WHERE ModifiedDate > '@{item().LastWatermarkValue}' AND ModifiedDate <= '@{variables('CurrentMaxWatermark')}') to prevent partial-run data loss when new clinical visits are logged mid-pipeline.Hard-Delete Capture via Change Tracking: Joins incremental queries against SQL Server Change Tracking (CHANGETABLE(CHANGES ...)) so cancelled appointments or voided claims are flagged with SYS_CHANGE_OPERATION = 'D' and propagated downstream.Dynamic Dataset Parameterization: Uses a single SQL Server dataset and a single ADLS Gen2 Parquet dataset parameterized by @dataset().ServerName, @dataset().DatabaseName, @dataset().SchemaName, and @dataset().TableName.📦 6. ADLS Gen2 Storage Layout (Medallion Architecture)abfss://healthcare-lakehouse@sthealthcaredwhprod.dfs.core.windows.net/
│
├── 🟫 bronze/
│   ├── onprem_ehr/
│   │   ├── patients/year=2026/month=11/day=17/part-00001.snappy.parquet
│   │   └── inpatient_encounters/year=2026/month=11/day=17/part-00001.snappy.parquet
│   ├── cloud_clinic/
│   │   └── appointments/year=2026/month=11/day=17/part-00001.snappy.parquet
│   └── cloud_claims/
│       └── claims_header/year=2026/month=11/day=17/part-00001.snappy.parquet
│
├── 🥈 silver/ (Unity Catalog Managed Delta Tables - FHIR R4 & HIPAA Masked)
│   ├── fhir_patient
│   ├── fhir_encounter
│   ├── fhir_observation
│   ├── fhir_claim
│   └── quarantine_invalid_records
│
└── 🥇 gold/ (Unity Catalog Managed Delta Tables - Star Schema for Power BI)
    ├── dim_patient (SCD Type 2)
    ├── dim_provider (SCD Type 2)
    ├── dim_diagnosis_icd10
    ├── dim_payer
    ├── fact_clinical_encounters
    └── fact_claims_billing
🧬 7. Azure Databricks Transformations: FHIR R4, HIPAA & Medallion Flow🔗 Why HL7 FHIR R4 Standardization in the Silver Layer?Each of the 3 SQL Server sources uses different table structures, column names, and local codes for the exact same clinical entities. In the Silver Layer, PySpark normalizes all 3 sources into canonical FHIR R4 Resources (Patient, Encounter, Observation, Claim) before building Gold analytics tables:Clinical ConceptSource 1 (On-Premises EHR)Source 2 (Cloud Clinic EMR)Source 3 (Cloud Claims DB)Silver Layer (FHIR R4 Standard)Unique Patient IDpat_id (INT)patient_guid (VARCHAR)member_no (VARCHAR)patient_resource_id (Salted SHA-256 Hash)Administrative Gendersex ('M', 'F', 'U')gender_cd (1, 2, 9)gen ('Male', 'Female')gender ('male', 'female', 'other', 'unknown')Birth Datedob (MM/DD/YYYY)birth_dt (YYYY-MM-DD)bdate (DATETIME)birthDate (YYYY-MM-DD ISO-8601)Visit / Encounter Windowadmit_ts / disch_tscheck_in / check_outservice_start / service_endperiod_start_utc / period_end_utcEncounter Class'I' (Inpatient), 'E' (ER)'O' (Outpatient)'CLM_AMB' / 'CLM_INP'class_code ('IMP', 'EMER', 'AMB')🔐 HIPAA Safe Harbor PII/PHI Masking & GovernanceTo protect sensitive patient records in compliance with the HIPAA Privacy Rule, the Silver layer enforces strict de-identification and governance via Unity Catalog:Deterministic Cryptographic Hashing (Salted SHA-256): Hashes direct identifiers (SSN, MRN, Insurance_Member_ID) using a secret salt fetched from Azure Key Vault (dbutils.secrets.get("kv-healthcare-scope", "hipaa-hash-salt")). This preserves cross-system join accuracy across EHR, EMR, and Claims without exposing raw identifiers.Dynamic Column Masking in Unity Catalog: Redacts FullName, Phone, Email, and StreetAddress (***-MASKED-PHI-***) for standard BI analysts while allowing authorized compliance auditors to inspect records via role-based access control (IS_ACCOUNT_GROUP_MEMBER('hipaa_compliance_officers')).Age & Geographic Generalization: Converts exact dates of birth for patients aged over 89 into a capped 90+ age bucket and truncates 5-digit ZIP codes to 3-digit prefixes where required by HIPAA Safe Harbor guidelines.Dead-Letter Quarantine Routing: Uses Delta Live Tables expectations (@dlt.expect_or_drop) to route rows with missing MRN, negative billing amounts, or invalid ICD-10 codes into silver.quarantine_invalid_records for root-cause inspection.💻 Sample PySpark / Delta Live Tables Transformation (silver_fhir_patient.py)import dlt
from pyspark.sql import functions as F

HIPAA_SALT = dbutils.secrets.get(scope="kv-healthcare-scope", key="hipaa-hash-salt")

@dlt.table(
    name="silver_fhir_patient",
    comment="FHIR R4 standardized and HIPAA-masked Patient resource unified from 3 SQL Server sources",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"}
)
@dlt.expect_or_drop("valid_mrn", "raw_mrn IS NOT NULL AND length(trim(raw_mrn)) > 0")
@dlt.expect_or_drop("valid_birth_date", "birthDate IS NOT NULL AND birthDate >= '1900-01-01'")
def silver_fhir_patient():
    df_raw = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "parquet") \
        .load("abfss://healthcare-lakehouse@sthealthcaredwhprod.dfs.core.windows.net/bronze/*/patients/*")

    return (
        df_raw
        # 1. Deterministic Salted SHA-256 Hashing for Cross-System Joins
        .withColumn("patient_resource_id", F.sha2(F.concat(F.trim(F.col("raw_mrn")), F.lit(HIPAA_SALT)), 256))
        .withColumn("ssn_hash", F.sha2(F.concat(F.regexp_replace("ssn", "[^0-9]", ""), F.lit(HIPAA_SALT)), 256))
        
        # 2. HL7 FHIR R4 Administrative Gender Standardization
        .withColumn(
            "gender",
            F.when(F.upper(F.col("raw_gender")).isin("M", "1", "MALE"), "male")
             .when(F.upper(F.col("raw_gender")).isin("F", "2", "FEMALE"), "female")
             .when(F.upper(F.col("raw_gender")).isin("O", "3", "OTHER"), "other")
             .otherwise("unknown")
        )
        
        # 3. HIPAA Safe Harbor Age Capping (Age > 89 -> 90+) & PII Redaction
        .withColumn("birthDate", F.to_date("raw_dob"))
        .withColumn("age_years", F.floor(F.months_between(F.current_date(), F.col("birthDate")) / 12))
        .withColumn("hipaa_safe_age", F.when(F.col("age_years") > 89, F.lit("90+")).otherwise(F.col("age_years").cast("string")))
        .withColumn("masked_email", F.regexp_replace("email", "(^[^@]{2})[^@]+(@.*$)", "$1***$2"))
        .withColumn("zip_3digit", F.substring(F.trim(F.col("zip_code")), 1, 3))
        .drop("raw_mrn", "ssn", "raw_name", "phone_number", "email")
    )
🌟 8. Gold Layer Dimensional Star Schema (SCD Type 2)In the Gold Layer, standardized Silver tables are modeled into a dimensional Star Schema optimized for Power BI DirectQuery / Import semantic models, using Slowly Changing Dimension (SCD) Type 2 to track historical changes in patient insurance plans and provider hospital departments.erDiagram
    DIM_PATIENT ||--o{ FACT_CLINICAL_ENCOUNTERS : "has_visits"
    DIM_PROVIDER ||--o{ FACT_CLINICAL_ENCOUNTERS : "attends"
    DIM_DIAGNOSIS_ICD10 ||--o{ FACT_CLINICAL_ENCOUNTERS : "diagnosed_with"
    DIM_PATIENT ||--o{ FACT_CLAIMS_BILLING : "billed_for"
    DIM_PAYER ||--o{ FACT_CLAIMS_BILLING : "covered_by"
    FACT_CLINICAL_ENCOUNTERS ||--o{ FACT_CLAIMS_BILLING : "generates_claim"

    DIM_PATIENT {
        bigint patient_sk PK
        string patient_resource_id "Salted SHA-256 NK"
        string gender "FHIR R4 code"
        string hipaa_safe_age "Capped at 90+"
        string zip_3digit "HIPAA Safe Harbor"
        string primary_payer_code
        timestamp effective_start_dt "SCD2 Start"
        timestamp effective_end_dt "SCD2 End"
        boolean is_current "SCD2 Active Flag"
    }

    DIM_PROVIDER {
        bigint provider_sk PK
        string npi_number "National Provider ID"
        string specialty_code
        string department_name
        string facility_location
        boolean is_current "SCD2 Active Flag"
    }

    DIM_DIAGNOSIS_ICD10 {
        bigint diagnosis_sk PK
        string icd10_code
        string clinical_description
        string chronic_category
    }

    DIM_PAYER {
        bigint payer_sk PK
        string payer_id
        string payer_name
        string plan_type "Medicare, Medicaid, Commercial"
    }

    FACT_CLINICAL_ENCOUNTERS {
        string encounter_id PK
        bigint patient_sk FK
        bigint provider_sk FK
        bigint primary_diagnosis_sk FK
        string fhir_class_code "IMP, EMER, AMB"
        timestamp admit_timestamp
        timestamp discharge_timestamp
        decimal length_of_stay_days
        boolean is_30day_readmission
    }

    FACT_CLAIMS_BILLING {
        string claim_id PK
        string encounter_id FK
        bigint patient_sk FK
        bigint payer_sk FK
        string cpt_procedure_code
        decimal billed_amount_usd
        decimal allowed_amount_usd
        decimal paid_amount_usd
        string claim_status "Paid, Denied, Pending"
    }
📊 9. Power BI Analytics & Executive Healthcare KPIsThe Gold Star Schema feeds an interactive Power BI Executive Healthcare Dashboard centered on four core operational and financial metrics:30-Day Hospital Readmission Rate: Tracks unplanned inpatient readmissions within 30 days of discharge across ICD-10 diagnosis cohorts to identify high-risk clinical pathways.Average Length of Stay (ALOS): Measures inpatient bed utilization (discharge_timestamp - admit_timestamp) segmented by department, severity, and attending provider.First-Pass Claim Denial Rate: Analyzes rejected insurance claims by payer, CPT procedure code, and denial reason code to accelerate revenue cycle recovery.Net Collection Rate & Patient Volume Trends: Compares billed vs. reimbursed claim amounts across inpatient, emergency, and outpatient clinic locations.📂 10. Repository Folder Structurehealthcare-data-warehouse/
├── README.md
├── docs/
│   ├── images/
│   │   └── architecture_diagram.png      # <-- Drop your architecture diagram PNG here
│   └── fhir_r4_mapping_specification.md
├── adf/
│   ├── pipelines/
│   │   └── pl_ingest_healthcare_sources.json
│   ├── datasets/
│   │   ├── ds_sqlserver_parameterized.json
│   │   └── ds_adls_parquet_bronze.json
│   └── linkedServices/
│       ├── ls_sqlserver_shir_onprem.json
│       └── ls_sqlserver_autoresolve_cloud.json
├── sql/
│   ├── 01_create_metadata_control_table.sql
│   ├── 02_sp_update_watermark.sql
│   └── 03_enable_sql_change_tracking.sql
├── databricks/
│   ├── 01_bronze_to_silver_fhir_dlt.py
│   ├── 02_hipaa_unity_catalog_masking.sql
│   ├── 03_silver_to_gold_scd2_merge.py
│   └── 04_delta_optimize_zorder.sql
└── powerbi/
    └── dax_clinical_and_claims_measures.md
👤 AuthorVenkatesh C MData Engineer | Azure Data Factory • Azure Databricks • Microsoft Fabric • PySpark • SQL • Power BI📍 Bengaluru, Karnataka, India📧 Email: venkateshcm108@gmail.com
