// Databricks notebook source
// MAGIC %sql
// MAGIC 
// MAGIC create database if not exists dashboarddb

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC create table if not exists dashboarddb.customers
// MAGIC using csv
// MAGIC options
// MAGIC (
// MAGIC path "/mnt/data/customers/*.csv",
// MAGIC inferSchema True,
// MAGIC header True,
// MAGIC sep ","
// MAGIC )

// COMMAND ----------

val getCustomerType = (credit: Int) => {
  if(credit >= 1 && credit < 1000) "Silver"
  else if(credit >= 10000 && credit < 25000) "Gold"
  else "Platinum"
}

spark.udf.register("getCustomerType", getCustomerType)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE WIDGET DROPDOWN customertype DEFAULT "Gold" 
// MAGIC CHOICES 
// MAGIC SELECT DISTINCT getCustomerType(credit) customertype 
// MAGIC FROM dashboarddb.customers

// COMMAND ----------

val selectedCustomerType = dbutils.widgets.get("customertype")
val resultsv2 = spark.sql("SELECT customerid, fullname, address, credit, getCustomerType(credit) as customertype, status FROM dashboarddb.customers WHERE getCustomerType(credit) = '" + selectedCustomerType + "'")

display(resultsv2)

// COMMAND ----------

val selectedCustomerType = dbutils.widgets.get("customertype")
val resultsv2 = spark.sql("SELECT customerid, fullname, address, credit, getCustomerType(credit) as customertype, status FROM dashboarddb.customers WHERE getCustomerType(credit) = '" + selectedCustomerType + "'")

display(resultsv2)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Learning ADB Professionally

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ![Image Title](https://encrypted-tbn0.gstatic.com/images?q=tbn%3AANd9GcTTIwqnM496Lo4k5PKJ_UdJYRo8bZkMP7GHgFJu9QE-tMs81E4A)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC \\(c = \\pm\\sqrt{a^2 + b^2} \\)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC \\( f(\beta)= -Y_t^T X_t \beta + \sum log( 1+{e}^{X_t\bullet\beta}) + \frac{1}{2}\delta^t S_t^{-1}\delta\\)
// MAGIC 
// MAGIC where \\(\delta=(\beta - \mu_{t-1})\\)
// MAGIC 
// MAGIC $$\sum_{i=0}^n i^2 = \frac{(n^2+n)(2n+1)}{6}$$

// COMMAND ----------

displayHTML("""<svg width="100" height="100">
   <circle cx="50" cy="50" r="40" stroke="green" stroke-width="4" fill="yellow" />
   Sorry, your browser does not support inline SVG.
</svg>""")

// COMMAND ----------

val colorsRDD = sc.parallelize(
	Array(
		(197,27,125), (222,119,174), (241,182,218), (253,244,239), (247,247,247), 
		(230,245,208), (184,225,134), (127,188,65), (77,146,33)))

val colors = colorsRDD.collect()

displayHTML(s"""
<!DOCTYPE html>
<meta charset="utf-8">
<style>

path {
  fill: yellow;
  stroke: #000;
}

circle {
  fill: #fff;
  stroke: #000;
  pointer-events: none;
}

.PiYG .q0-9{fill:rgb${colors(0)}}
.PiYG .q1-9{fill:rgb${colors(1)}}
.PiYG .q2-9{fill:rgb${colors(2)}}
.PiYG .q3-9{fill:rgb${colors(3)}}
.PiYG .q4-9{fill:rgb${colors(4)}}
.PiYG .q5-9{fill:rgb${colors(5)}}
.PiYG .q6-9{fill:rgb${colors(6)}}
.PiYG .q7-9{fill:rgb${colors(7)}}
.PiYG .q8-9{fill:rgb${colors(8)}}

</style>
<body>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.6/d3.min.js"></script>
<script>

var width = 960,
    height = 500;

var vertices = d3.range(100).map(function(d) {
  return [Math.random() * width, Math.random() * height];
});

var svg = d3.select("body").append("svg")
    .attr("width", width)
    .attr("height", height)
    .attr("class", "PiYG")
    .on("mousemove", function() { vertices[0] = d3.mouse(this); redraw(); });

var path = svg.append("g").selectAll("path");

svg.selectAll("circle")
    .data(vertices.slice(1))
  .enter().append("circle")
    .attr("transform", function(d) { return "translate(" + d + ")"; })
    .attr("r", 2);

redraw();

function redraw() {
  path = path.data(d3.geom.delaunay(vertices).map(function(d) { return "M" + d.join("L") + "Z"; }), String);
  path.exit().remove();
  path.enter().append("path").attr("class", function(d, i) { return "q" + (i % 9) + "-9"; }).attr("d", String);
}

</script>
  """)