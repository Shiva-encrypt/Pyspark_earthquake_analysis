# Pyspark_earthquake_analysis
PySpark project analyzing earthquake data. Utilizes distributed processing for insights into weekly, monthly patterns. Stores results in MySQL. Explore earthquake occurrences effortlessly

## Prerequisites

- Apache Spark installed
- Python 3.x
- MySQL server installed

## Setup

1. Clone the repository:

   ```bash
   git clone https://github.com/Shiva-encrypt/pyspark-earthquake-analysis.git
   ```

2. Install the required Python packages:

   ```bash
   pip install pyspark pandas sqlalchemy
   ```

3. Download the MySQL Connector JAR file and place it in the `C:/Program Files/JDBC/` directory.

4. Update the MySQL connection details in the `app` script:

   ```python
   mysql_username = "root"
   mysql_password = "YourMySQLPassword"
   mysql_host = "localhost"
   mysql_port = "3306"
   mysql_database = "mysql"
   ```

5. Place your earthquake data CSV file in the `data` directory.

## Running the Application

1. Open a terminal and navigate to the project directory:

   ```bash
   cd path/to/pyspark-earthquake-analysis
   ```

2. Run the PySpark application:

   ```bash
   spark-submit app.py
   ```

## Notes

- The application answers questions related to the day of the week, day of the month, average frequency, yearly counts, magnitude variation, standard deviation by year, geographic locations, high-frequency locations, and magnitude-related statistics.

- Ensure your MySQL server is running before executing the application.

- Feel free to modify the application to suit your specific needs or add more analysis tasks.

```
