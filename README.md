# Day 14 — DataFrame and Dataset

A Spark Scala project demonstrating DataFrame and Dataset APIs through a typed employee payroll pipeline.

## Scenario

The application processes employee salary information and calculates:

- Gross salary
- Tax
- Net salary
- Department-level payroll statistics
- Highest-paid employees

## Concepts Covered

### DataFrame

An untyped distributed table containing rows and columns.

### Dataset

A strongly typed distributed collection.

```scala
Dataset[Employee]
Case Class
case class Employee(
  employee_id: String,
  name: String,
  department: String,
  role: String,
  basic_salary: Double,
  bonus: Double,
  experience_years: Int
)
DataFrame to Dataset
val employeeDS = employeeDF.as[Employee]
Dataset to DataFrame
val payrollDF = payrollDS.toDF()
Payroll Formula
gross_salary = basic_salary + bonus

tax = gross_salary × 10%

net_salary = gross_salary - tax
RDD vs DataFrame vs Dataset
Feature	RDD	DataFrame	Dataset
Type safety	Yes	No	Yes
Schema	No	Yes	Yes
Catalyst optimization	No	Yes	Yes
Tungsten execution	Limited	Yes	Yes
SQL support	No	Yes	Yes
Object-oriented API	Yes	Limited	Yes
Performance	Lower	High	High
Catalyst Optimizer

Spark SQL uses Catalyst to optimize DataFrame and Dataset queries.

Examples include:

Predicate pushdown
Column pruning
Constant folding
Logical plan optimization
Physical plan selection
Run
sbt run
Test
sbt test
Technologies
Scala 2.12
Apache Spark 3.5.6
Spark SQL
sbt
