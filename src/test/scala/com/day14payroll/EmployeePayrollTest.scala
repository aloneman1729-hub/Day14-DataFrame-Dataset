package com.day14payroll

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class EmployeePayrollTest extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession.builder()
      .appName("EmployeePayrollTest")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.hadoop.fs.defaultFS", "file:///")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
  }

  import org.apache.spark.sql.Encoders

  test("DataFrame should load 8 employees") {
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/employees.csv")

    assert(df.count() == 8)
  }

  test("DataFrame should convert to Dataset[Employee]") {
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/employees.csv")

    val session = spark
    import session.implicits._

    val employeeDS = df.as[Employee]

    assert(employeeDS.count() == 8)
    assert(employeeDS.head().isInstanceOf[Employee])
  }

  test("Engineering department should contain 4 employees") {
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/employees.csv")

    val session = spark
    import session.implicits._

    val employeeDS = df.as[Employee]

    val engineeringCount =
      employeeDS.filter(_.department == "Engineering").count()

    assert(engineeringCount == 4)
  }

  test("E101 payroll calculation should be correct") {
    val employee = Employee(
      "E101",
      "Prudhvi",
      "Engineering",
      "Data Engineer",
      70000,
      10000,
      2
    )

    val gross = employee.basic_salary + employee.bonus
    val tax = gross * 0.10
    val net = gross - tax

    assert(gross == 80000)
    assert(tax == 8000)
    assert(net == 72000)
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }
}
