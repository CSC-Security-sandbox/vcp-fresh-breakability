# 2. Database choice for VCP

Date: 2025-05-03

## Status

Accepted

## Context

Our control plane requires a robust and reliable database solution to manage our data, which is relational in nature. The resources we are managing have dependencies on each other, forming a hierarchical structure. 
For example:
Account → Pool → Volume(multiple volumes under the same pool) → Snapshot (multiple snapshots within the same volume)
Account → Active Directory → Pool (multiple pools using the same Active Directory)
Account → Backup Policy → Volume (multiple volumes using the same Backup Policy)
Account → Backup Vault → Volume (A single Backup Vault attached to multiple volumes)

Given the relational nature of our data and the need for data consistency, we require a database system that supports ACID (Atomicity, Consistency, Isolation, Durability) properties. This ensures that our data remains consistent and reliable even in the face of concurrent transactions and potential system failures.
For example:
When a volume is deleted, we need to ensure that all associated snapshots are also deleted to maintain data integrity. This requires strong transactional support to ensure that either all operations succeed or none do, preventing orphaned snapshots.

Additionally, our data model includes a hybrid approach that combines structured relational data with semi-structured data. This hybrid model leverages JSONB columns to store semi-structured data that does not require strict relational integrity, providing flexibility and scalability.

A significant challenge we face is managing concurrency and consistency across our data. Concurrency is important because multiple users may attempt to modify the same data simultaneously. For example, if two users try to update the size of a volume at the same time, we need to ensure that the system can handle this without leading to data corruption or inconsistencies.
NoSQL databases often lack the strong consistency and sophisticated concurrency control mechanisms provided by relational databases. In our use case, ensuring data integrity and consistency during concurrent transactions is critical.

Furthermore, our control plane utilizes Temporal, a powerful workflow orchestration engine that requires SQL databases for its persistence layer. Temporal relies on SQL databases to store workflow state, history, and metadata, making the choice of a robust SQL database essential for our architecture.

We also need to consider the cloud provider we choose for our deployment. We want to avoid vendor lock-in and ensure that our database solution is hyper-scaler agnostic, allowing us to deploy on any major cloud provider without significant changes to our architecture.

Finally, we need to consider the cost of the database solution. While we are willing to invest in a reliable and scalable database, we also want to ensure that it is cost-effective and does not lead to unnecessary expenses.

## Decision

We have decided to use PostgreSQL as our relational database management system (RDBMS) for the following reasons:

1. **Hyper-scaler Agnostic**:
    - PostgreSQL is supported by all major cloud providers, offering managed services such as Amazon RDS, Google Cloud SQL, and Azure Database for PostgreSQL. This provides flexibility and avoids vendor lock-in.

2. **Scalability**:
    - PostgreSQL meets our scaling requirements, both vertically and horizontally. It supports large volumes of data and high transaction rates, making it suitable for our growing data needs.

3. **Support for JSONB Columns**:
    - PostgreSQL supports JSONB columns, which allows us to implement a hybrid data model. This enables the storage of semi-structured data in a NoSQL-like format while maintaining relational integrity for structured data. This hybrid approach provides the flexibility to handle diverse data types within a single database system.

4. **Concurrency and Consistency Management**:
    - PostgreSQL provides robust concurrency control mechanisms, including Multi-Version Concurrency Control (MVCC), which allows multiple transactions to occur simultaneously without interfering with each other. This is essential for maintaining data consistency and integrity in our highly concurrent environment.
    - The strong consistency guarantees of PostgreSQL ensure that our data remains accurate and reliable, even during complex transactions and concurrent access, which is a significant advantage over many NoSQL databases that offer eventual consistency.

5. **Temporal Workflow Orchestration**:
    - Temporal requires a SQL database for its persistence layer, where it stores workflow state, history, and metadata. PostgreSQL is fully compatible with Temporal, providing the necessary reliability and performance for managing workflow orchestration in our control plane.

6. **Cost-Effectiveness**:
    - Using PostgreSQL is cost-effective. Managed PostgreSQL services (e.g., Google Cloud SQL) are generally cheaper than other options like Google Cloud Spanner, making it a budget-friendly choice.

7. **ORM Tool Support**:
    - PostgreSQL has excellent out-of-the-box support for Object-Relational Mapping (ORM) tools such as GORM. This simplifies the integration with our application and improves developer productivity.


## Consequences

- **Benefits**:
    - Ensures data consistency and integrity through ACID compliance.
    - Provides flexibility in cloud provider selection, reducing the risk of vendor lock-in.
    - Supports both relational and semi-structured data with JSONB columns, catering to diverse data storage needs in a hybrid model.
    - Offers a cost-effective solution compared to other managed database services.
    - Enhances developer productivity with strong ORM tool support.
    - Robust concurrency control and strong consistency management ensure data integrity and reliability in a highly concurrent environment.
    - Fully compatible with Temporal for workflow orchestration, ensuring reliable and efficient management of workflow state, history, and metadata.

- **Drawbacks**:
    - Requires expertise in PostgreSQL for effective management and optimization.
    - May need additional configuration and tuning to handle very large-scale deployments.


## Alternatives Considered

- **Google Cloud Spanner**:
    - Pros: Highly scalable, globally distributed, and offers strong consistency.
    - Cons: Higher cost compared to PostgreSQL, limited support across different cloud providers.

- **NoSQL Databases (e.g., MongoDB, Cassandra)**:
    - Pros: Excellent for handling large volumes of semi-structured data, high scalability.
    - Cons: Lack of ACID properties and strong consistency guarantees, which are crucial for our relational data requirements. Managing concurrency and consistency in NoSQL databases often requires additional complexity and can lead to data integrity issues. Additionally, NoSQL databases are not compatible with Temporal's requirements for a SQL persistence layer.
