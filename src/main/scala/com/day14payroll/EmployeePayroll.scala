package com.day14payroll

import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class Employee(
  employee_id: String,
  name: String,
  department: String,
  role: String,
  basic_salary: Double,
  bonus: Double,
  experience_years: Int
)

case class Payroll(
  employee_id: String,
  name: String,
  department: String,
  role: String,
  basic_salary: Double,
  bonus: Double,
  experience_years: Int,
  gross_salary: Double,
  tax: Double,
  net_salary: Double
)

object EmployeePayroll {

  def createSparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("Day14-DataFrame-Dataset")
      .master("local[*]")
      .config("spark.hadoop.fs.defaultFS", "file:///")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  def loadEmployees(spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/employees.csv")
  }

  def dataframeToDataset(
      df: DataFrame
  ): Dataset[Employee] = {
    import df.sparkSession.implicits._
    df.as[Employee]
  }

  def calculatePayroll(
      employees: Dataset[Employee]
  ): Dataset[Payroll] = {

    import employees.sparkSession.implicits._

    employees.map { employee =>

      val grossSalary =
        employee.basic_salary + employee.bonus

      val tax =
        grossSalary * 0.10

      val netSalary =
        grossSalary - tax

      Payroll(
        employee.employee_id,
        employee.name,
        employee.department,
        employee.role,
        employee.basic_salary,
        employee.bonus,
        employee.experience_years,
        grossSalary,
        tax,
        netSalary
      )
    }
  }

  def main(args: Array[String]): Unit = {

    val spark = createSparkSession()

    import spark.implicits._

    println("\n======================================")
    println("DAY 14 - DATAFRAME AND DATASET")
    println("EMPLOYEE PAYROLL PIPELINE")
    println("======================================")

    // ------------------------------------------------
    // 1. Create DataFrame
    // ------------------------------------------------

    val employeeDF = loadEmployees(spark)

    println("\n--- EMPLOYEE DATAFRAME ---")
    employeeDF.show(false)

    println("\n--- DATAFRAME SCHEMA ---")
    employeeDF.printSchema()

    // ------------------------------------------------
    // 2. DataFrame -> Dataset
    // ------------------------------------------------

    val employeeDS: Dataset[Employee] =
      dataframeToDataset(employeeDF)

    println("\n--- EMPLOYEE DATASET ---")
    employeeDS.show(false)

    // ------------------------------------------------
    // 3. Typed Dataset transformation
    // ------------------------------------------------

    val engineeringEmployees =
      employeeDS
        .filter(_.department == "Engineering")

    println("\n--- ENGINEERING EMPLOYEES ---")
    engineeringEmployees.show(false)

    // ------------------------------------------------
    // 4. Calculate Payroll
    // ------------------------------------------------

    val payrollDS =
      calculatePayroll(employeeDS)

    println("\n--- TYPED PAYROLL DATASET ---")
    payrollDS.show(false)

    // ------------------------------------------------
    // 5. Dataset -> DataFrame
    // ------------------------------------------------

    val payrollDF =
      payrollDS.toDF()

    println("\n--- PAYROLL DATAFRAME ---")
    payrollDF.show(false)

    // ------------------------------------------------
    // 6. SQL
    // ------------------------------------------------

    payrollDF.createOrReplaceTempView("payroll")

    val departmentReport =
      spark.sql(
        """
          |SELECT
          |  department,
          |  COUNT(*) AS employee_count,
          |  ROUND(SUM(gross_salary), 2) AS total_gross_salary,
          |  ROUND(AVG(net_salary), 2) AS average_net_salary
          |FROM payroll
          |GROUP BY department
          |ORDER BY total_gross_salary DESC
          |""".stripMargin
      )

    println("\n--- DEPARTMENT PAYROLL REPORT ---")
    departmentReport.show(false)

    // ------------------------------------------------
    // 7. Highest paid employees
    // ------------------------------------------------

    val highestPaid =
      payrollDF
        .orderBy(desc("net_salary"))
        .select(
          "employee_id",
          "name",
          "department",
          "net_salary"
        )

    println("\n--- HIGHEST PAID EMPLOYEES ---")
    highestPaid.show(false)

    // ------------------------------------------------
    // 8. Overall payroll statistics
    // ------------------------------------------------

    val overall =
      payrollDF.select(
        count("*").alias("employee_count"),
        round(sum("gross_salary"), 2).alias("total_gross_salary"),
        round(sum("tax"), 2).alias("total_tax"),
        round(sum("net_salary"), 2).alias("total_net_salary")
      )

    println("\n--- OVERALL PAYROLL ---")
    overall.show(false)

    println("\n======================================")
    println("PIPELINE COMPLETED SUCCESSFULLY")
    println("======================================")

    spark.stop()
  }
}
