<br />
<br />

<p align="center">
  <img src=".images/video-call.png" alt="ifco data engineering test challenge" width="80" height="80">
</p>


<h1 align="center">
  <b>
    IFCO Data Engineering Challenge
  </b>
</h1>

<h3 align="center">
  <b>
    Miguel Ángel Sotomayor Fernández
  </b>
</h3>

<br />

---

## **Analysis and Assumptions**

Firs assumption: Exercises are name as tests (Test 1, Test 2,...) in the original `README.md`. I'm going to rename these exercises as `Challenges` to avoid a misunderstanding between exercises and unit tests.

The main challenge for this input is `orders.csv` file. This file has a data quality issue for both `company_id` and `company_name` columns.
My first thought was to identify uniquely a company based on `company_id`. Fixing this is quite easy. Just materializing all differents `company_name` in just one per `company_id`. Something like this:

```python
# Materialize unique company names by company_id
# This is because there are multiple names for the same company_id
# So I'm grouping based on company_id because my assumption is that id is correct
# And company_name contains typo errors
materialized_companies = (
    orders_df.groupBy("company_id")
    .agg(first("company_name", ignorenulls=True).alias("materialized_company_name"))

# Join materialized names back to the original DataFrame
normalized_df = orders_df.join(materialized_companies, on="company_id", how="inner")
```

For instance, having this input:
| Company ID | Company Name  |
|---|---|
| 1  | Veggie Shop  |
| 2  | Veggie Shop co  |
| 2  | Veggi S Co  |

The output will be:

| Company ID | Company Name  |
|---|---|
| 1  | Veggie Shop  |
| 2  | Veggie Shop co  |


But if we pay attention to Challenge 5, there is hint indicating the following:
*Hint: Consider the possibility of duplicate companies stored under multiple IDs in the database. Take this into account while devising a solution to this exercise.*

That means that we can have the following scenario:
| Company ID | Company Name  |
|---|---|
| 1  | Veggie Shop  |
| 2  | Veggie Shop  |
| 2  | Veggi S Co  |

Therefore, we can't trust on either `company_id` and `company_name`. The description of the challenge is not providing any information to resolve this discrepancy.

##### Decicion
So, I need to take a decision. For me, `company_name` is the one to decide what a company is. In parallel, I'm using the 
[Levenshtein Distance](https://en.wikipedia.org/wiki/Levenshtein_distance) to normalize `company_name`. I'll use [fuzzywuzzy library](https://pypi.org/project/fuzzywuzzy/) to apply *Levenshtein Distance*.
It's risky, because sometimes we can make mistakes using Levenshtein Distance, joining two companies which are differents. That because I'm going to write a file called `company_names.csv` specifying the matching between original names and materialization names generated based on Levenshtein Distance, so we can do further investigations about this.


## **Challenges**

### 1. **Distribution of Crate Type per Company**
   - Analyze the distribution of crate types used by companies for their orders.
   - **Output**: A DataFrame showing the count of each crate type per company.

### 2. **DataFrame of Orders with Full Name of the Contact**
   - Extract the full name of the contact from the `contact_data` column, applying placeholders (`John Doe`) for missing names.
   - **Output**: A DataFrame containing `order_id` and `contact_full_name`.

### 3. **DataFrame of Orders with Contact Address**
   - Extract the address from the `contact_data` field in the format `city name, postal code`. If any information is missing, use placeholders (`Unknown`, `UNK00`).
   - **Output**: A DataFrame containing `order_id` and `contact_address`.

### 4. **Calculation of Sales Team Commissions**
   - Compute commissions for sales owners based on their participation in orders, using a tiered system:
     - **Main Owner**: 6% of net invoiced value.
     - **Co-owner 1**: 2.5%.
     - **Co-owner 2**: 0.95%.
   - Aggregate total commissions per salesperson.
   - **Output**: A sorted DataFrame of sales owners and their total commissions.

### 5. **DataFrame of Companies with Sales Owners**
   - Generate a DataFrame containing a unique, comma-separated, and alphabetically sorted list of sales owners associated with each company.
   - Handle potential duplicate company IDs.
   - **Output**: A DataFrame containing `company_id`, `company_name`, and `list_salesowners`.

---

## **Requirements**

- Python 3.11
- [Poetry](https://python-poetry.org/) for dependency management. Look at [this section](#1-install-dependencies-locally) to install poetry locally.
- Docker (optional, for containerized execution)

---

## **Setup**

### **1. Install Dependencies Locally**
```bash
pip install poetry
poetry install
```

### **2. Execute Challenges Locally**
```bash
poetry shell
poetry run python main.py
```

### **3. Execute Challenges using Docker**
```bash
docker build -t my-python-app 
docker run -it my-python-app
```

### **4. Run Unit Test and code coverage**
```bash
poetry run pytest --disable-pytest-warnings
```

```bash
coverage run -m pytest && coverage report -m
```

# Menu Workflow for Challenges

## Overview
The menu provides a simple interface to execute various challenges, each focused on a specific data processing task.

## Menu Structure
The menu presents the following options:
1. **Challenge 1**: Distribution of Crate Type per Company
2. **Challenge 2**: Data Deduplication and Normalization
3. **Challenge 3**: Advanced Metrics Calculation
4. **Challenge 4**: Aggregation Over Time
5. **Challenge 5**: Handling Duplicate Companies by ID
6. **Exit**

## How It Works
1. **Display Menu**: The user is shown a list of challenges.
2. **User Input**: The user selects a challenge by entering its corresponding number.
3. **Route to Function**: The menu calls the appropriate function for the selected challenge.
4. **Execute Challenge**: The function processes the required data, performs calculations, and displays the results.
5. **Repeat or Exit**: The user can choose another challenge or exit the program.
